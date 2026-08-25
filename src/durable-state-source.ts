import { durableStreamSource, type OffsetGranularity } from "./durable-stream-source.js";
import type { ShapeSource } from "./shape-source.js";
import type { Operation, ShapeChange } from "./types.js";

// --- Durable Streams State Protocol -----------------------------------------
//
// The State Protocol (`@durable-streams/state`) layers typed CRUD records over
// a durable stream, which is what Electric's agents runtime writes. Its record
// is very nearly ours already:
//
//   ChangeEvent  { type, key, value, old_value, headers{ operation, txid, timestamp, from, offset } }
//   ShapeChange  { shape, key,        value,             operation,                          offset }
//
// so this source is a rename plus three decisions: what to do with `upsert`,
// what to do with control frames, and what `txid` buys us.

/** One State Protocol record, as it appears on the wire. */
export type StateChangeEvent<T = Record<string, unknown>> = Readonly<{
  type: string;
  key: string;
  value?: T;
  old_value?: T;
  headers: Readonly<{
    operation: "insert" | "update" | "delete" | "upsert";
    txid?: string;
    timestamp?: string;
    from?: string;
    offset?: string;
  }>;
}>;

/** A stream-management frame: not a change, and never projected. */
export type StateControlEvent = Readonly<{
  headers: Readonly<{ control: "snapshot-start" | "snapshot-end" | "reset"; offset?: string }>;
}>;

export type StateEvent<T = Record<string, unknown>> = StateChangeEvent<T> | StateControlEvent;

/**
 * The stream declared its prior contents void. Drop the checkpoint and
 * re-materialize from the start; an idempotent projector converges on replay.
 *
 * This mirrors `ElectricMustRefetchError` deliberately: both mean "your
 * cursor names a history this stream no longer has".
 */
export class StateResetError extends Error {
  readonly stream: string;

  constructor(stream: string) {
    super(
      `Durable stream "${stream}" sent a \`reset\` control frame: everything read before it is void. ` +
        `Discard the checkpoint for this stream and call read(undefined) to re-materialize from the start.`,
    );
    this.name = "StateResetError";
    this.stream = stream;
  }
}

/**
 * How a State Protocol stream is addressed. Extends the stream granularities
 * with the one the protocol's own metadata makes possible.
 *
 * - `"transaction"` — every change sharing a `headers.txid` takes one offset,
 *   so `consume` applies a whole SOURCE transaction in one TypeGraph
 *   transaction under one recorded anchor. This is the granularity that makes
 *   recorded time line up with the writer's own commits rather than with
 *   transport chunking. Records without a `txid` fall back to per-message.
 */
export type StateOffsetGranularity = OffsetGranularity | "transaction";

export type DurableStateConfig = Readonly<{
  /** Full URL of the durable stream, e.g. `"http://localhost:8791/agents/crm-agent"`. */
  url: string;
  /** Logical name for this stream (the checkpoint key). */
  name: string;
  /** HTTP headers for every request — typically `Authorization`. */
  headers?: Readonly<Record<string, string>>;
  /**
   * Restrict to records whose `type` is in this set. Omit to take every type;
   * the projector then narrows on `change.shape`.
   */
  types?: readonly string[];
  /** Offset granularity. Defaults to `"message"`. */
  granularity?: StateOffsetGranularity;
  /** Maximum time to wait for the catch-up read to reach the stream's head. */
  timeoutMs?: number;
}>;

/** Type guard: a change record rather than a control frame. */
export function isStateChangeEvent<T>(event: StateEvent<T>): event is StateChangeEvent<T> {
  return "key" in event && "type" in event;
}

/**
 * The `shape` a control frame is parked under while it travels through the
 * stream source, which stamps offsets before anything can be filtered. Chosen
 * to be unrepresentable as a real collection type.
 */
const CONTROL_SHAPE = "\u0000control";

/**
 * State Protocol's `upsert` collapses to `update` on the way in.
 *
 * `Operation` describes what the PROJECTOR must do, and there are only two
 * such behaviours: make this key hold this value, or remove it. A projector is
 * required to be idempotent — `upsertById`, never `create` — so it cannot
 * distinguish insert from update anyway, and this source genuinely does not
 * know which one an `upsert` turned out to be.
 */
function toOperation(operation: StateChangeEvent["headers"]["operation"]): Operation {
  return operation === "upsert" ? "update" : operation;
}

/**
 * A {@link ShapeSource} over a Durable Streams State Protocol stream.
 *
 * CONTROL FRAMES. `reset` rejects with {@link StateResetError}. `snapshot-start`
 * / `snapshot-end` are DROPPED, which is correct for a snapshot that only adds
 * or supersedes and WRONG for one that implies deletions: a snapshot means "this
 * is the complete state", and reconciling that needs a diff against what the
 * belief already holds, not a run of upserts. Until that reconciliation exists,
 * a snapshot is applied as ordinary changes and rows the snapshot omitted are
 * left standing. Do not point this at a stream whose snapshots prune.
 *
 * Because control frames are filtered only after the underlying stream source
 * has stamped offsets, a batch that ENDS in a control frame contributes no
 * change carrying that batch's end offset. The next read then re-fetches from
 * the last real change — one redundant batch, absorbed idempotently.
 */
export function durableStateSource<V extends Record<string, unknown> = Record<string, unknown>>(
  config: DurableStateConfig,
): ShapeSource<V> {
  const wanted = config.types === undefined ? undefined : new Set(config.types);
  const granularity = config.granularity ?? "message";
  // The whole record travels as the value so transport metadata — `txid` above
  // all — survives as far as the grouping pass. Unwrapping to the row happens
  // last, on the way out, so decoding lives in exactly one place.
  const inner = durableStreamSource<StateEvent<V>, StateEvent<V>>({
    url: config.url,
    name: config.name,
    ...(config.headers === undefined ? {} : { headers: config.headers }),
    ...(config.timeoutMs === undefined ? {} : { timeoutMs: config.timeoutMs }),
    // Transaction granularity is applied here, after the whole catch-up is
    // collected, so a transaction split across transport chunks still lands in
    // one group. The stream source itself only ever sees "message".
    granularity: granularity === "transaction" ? "message" : granularity,
    toChange: (event) =>
      isStateChangeEvent(event)
        ? { shape: event.type, key: event.key, operation: toOperation(event.headers.operation), value: event }
        : { shape: CONTROL_SHAPE, key: "", operation: "update", value: event },
  });

  return {
    name: config.name,
    async read(after) {
      const kept: ShapeChange<StateEvent<V>>[] = [];
      for (const record of await inner.read(after)) {
        const event = record.value;
        if (!isStateChangeEvent(event)) {
          if (event.headers.control === "reset") throw new StateResetError(config.name);
          continue;
        }
        if (wanted !== undefined && !wanted.has(event.type)) continue;
        kept.push(record);
      }
      const grouped = granularity === "transaction" ? groupByTransaction(kept) : kept;
      return grouped.map((record) => ({
        offset: record.offset,
        shape: record.shape,
        key: record.key,
        operation: record.operation,
        value: ((record.value as StateChangeEvent<V>).value ?? {}) as V,
      }));
    },
  };
}

/**
 * Give every change in a source transaction the offset of that transaction's
 * last change, so `consume` applies the transaction atomically under one
 * recorded anchor.
 *
 * The txid is read off the RAW record rather than the projected change, because
 * `ShapeChange` deliberately carries no transport metadata. A run whose txid is
 * absent keeps its own per-message offset.
 *
 * A transaction straddling the stream's head at read time is the one case this
 * cannot group: its tail has not been written yet, so the leading part commits
 * on its own and the rest follows on the next read. That is a smaller
 * transaction than the source's, never a torn one — every change still applies
 * exactly once, and the cursor never advances past unapplied work.
 */
function groupByTransaction<V>(
  changes: readonly ShapeChange<StateEvent<V>>[],
): readonly ShapeChange<StateEvent<V>>[] {
  const txidOf = (record: ShapeChange<StateEvent<V>>): string | undefined =>
    isStateChangeEvent(record.value) ? record.value.headers.txid : undefined;

  const grouped: ShapeChange<StateEvent<V>>[] = [];
  let index = 0;
  while (index < changes.length) {
    const txid = txidOf(changes[index]!);
    if (txid === undefined) {
      grouped.push(changes[index]!);
      index += 1;
      continue;
    }
    // Scan to the end of this transaction's run, then give every member its
    // final offset so `consume` sees one offset and opens one transaction.
    let end = index;
    while (end + 1 < changes.length && txidOf(changes[end + 1]!) === txid) end += 1;
    const runOffset = changes[end]!.offset;
    for (; index <= end; index += 1) grouped.push({ ...changes[index]!, offset: runOffset });
  }
  return grouped;
}
