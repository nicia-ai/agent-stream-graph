import { STATUS_GONE } from "./http.js";
import { composeOffset, parseCompositeOffset, STREAM_START } from "./offset.js";
import type { ShapeSource } from "./shape-source.js";
import type { ShapeChange, ShapeChangeInput } from "./types.js";

// --- Durable Streams adapter ------------------------------------------------
//
// Wraps `@durable-streams/client`. The client is an OPTIONAL dependency,
// imported lazily — this module loads fine without it; the adapter only
// requires it when `read()` is first called.
//
// OFFSET SEMANTICS: unlike the Electric shape adapter, this source can assign a
// DISTINCT offset to every message, so `anchorFor(stream, offset)` reconstructs
// the belief as of that exact change rather than as of the end of a batch. That
// is the default; `granularity: "append"` trades it back for one transaction per
// append when bulk catch-up matters more.
//
// The protocol addresses whole appends, not messages: a read returns a batch
// and one `Stream-Next-Offset` describing the position AFTER it. Message-level
// positions therefore need the protocol's own two-part form — a base offset
// plus a count of messages past it, exactly what `Stream-Fork-Offset` /
// `Stream-Fork-Sub-Offset` use to anchor a fork inside an append. `composeOffset`
// renders that pair as one cursor string, so a checkpoint converts straight into
// a fork point (see `forkStream`).
//
// The count is measured from the offset the read was ISSUED at, never from a
// batch boundary, which makes it independent of how the server chunks its
// responses: resuming from `<base>,<n>` re-reads from `base` and drops `n`
// messages, and any chunking of those messages yields the same survivors. The
// last message of each batch is relabelled onto that batch's own end offset, so
// the count resets at every boundary instead of growing without bound.

/** A durable stream reported that the cursor is below its earliest retained offset. */
export class DurableStreamRetentionError extends Error {
  readonly stream: string;
  readonly offset: string;

  constructor(stream: string, offset: string, cause: unknown) {
    super(
      `Durable stream "${stream}" returned 410 Gone for offset "${offset}": the cursor is before the ` +
        `earliest retained position, so the messages between it and the stream's head are gone. ` +
        `Retention dropped them (see Stream-TTL / server retention policy). Recover by discarding the ` +
        `checkpoint and calling read(undefined) to re-materialize from the stream's start; an idempotent ` +
        `projector converges on replay.`,
      { cause },
    );
    this.name = "DurableStreamRetentionError";
    this.stream = stream;
    this.offset = offset;
  }
}

/**
 * How finely the stream is addressed, which decides how finely `consume` can
 * anchor it: one transaction and one recorded instant per distinct offset.
 *
 * - `"message"` — every message gets its own offset. `anchorFor` reconstructs
 *   the belief as of any single change, at the cost of a transaction each.
 * - `"append"` — every message in an append shares that append's end offset, so
 *   an append is applied in one transaction and carries one anchor. Cheaper for
 *   bulk catch-up, and the same trade an Electric shape batch makes.
 */
export type OffsetGranularity = "message" | "append";

export type DurableStreamConfig<V = Record<string, unknown>, TItem = unknown> = Readonly<{
  /** Full URL of the durable stream, e.g. `"http://localhost:8791/agents/crm-agent"`. */
  url: string;
  /** Logical name for this stream (the checkpoint key). */
  name: string;
  /** HTTP headers for every request — typically `Authorization`. */
  headers?: Readonly<Record<string, string>>;
  /**
   * Maps one JSON message to a `ShapeChange` (minus `offset`) for your
   * projector; the adapter stamps the resumable position.
   */
  toChange: (item: TItem) => ShapeChangeInput<V>;
  /** Offset granularity. Defaults to `"message"`. See {@link OffsetGranularity}. */
  granularity?: OffsetGranularity;
  /**
   * Maximum time to wait for the catch-up read to reach the stream's head, in
   * milliseconds. Default 30s; pass `Infinity` to disable. Without a bound a
   * stalled server would hang `read()` forever.
   */
  timeoutMs?: number;
}>;

const DEFAULT_READ_TIMEOUT_MS = 30_000;
const DEFAULT_GRANULARITY: OffsetGranularity = "message";

/** One batch of parsed JSON messages, as surfaced by `@durable-streams/client`. */
type JsonBatch<T> = Readonly<{
  items: readonly T[];
  /** The stream position AFTER this batch — `Stream-Next-Offset`. */
  offset: string;
  upToDate: boolean;
}>;

type StreamResponse<T> = Readonly<{
  subscribeJson: (subscriber: (batch: JsonBatch<T>) => void | Promise<void>) => () => void;
  cancel: (reason?: unknown) => void;
  closed: Promise<void>;
}>;

type StreamHandle = Readonly<{
  stream: <T>(options: Readonly<{ offset?: string; live: false }>) => Promise<StreamResponse<T>>;
}>;

type DurableStreamsClient = Readonly<{
  DurableStream: new (options: Readonly<{ url: string; headers?: Record<string, string> }>) => StreamHandle;
}>;

/** Narrow an unknown throw to its HTTP status, when the client attached one. */
function statusOf(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null || !("status" in error)) return undefined;
  const status = (error as { status: unknown }).status;
  return typeof status === "number" ? status : undefined;
}

async function loadClient(): Promise<DurableStreamsClient> {
  return (await import("@durable-streams/client")) as unknown as DurableStreamsClient;
}

/**
 * Re-throw as {@link DurableStreamRetentionError} when the server answered
 * `410 Gone`, otherwise unchanged. Both the session open and the wait for its
 * first batch can surface the retention drop, so neither owns the translation.
 */
function rethrowRead(error: unknown, name: string, cursor: string): never {
  if (statusOf(error) === STATUS_GONE) {
    throw new DurableStreamRetentionError(name, cursor, error);
  }
  throw error;
}

/**
 * A {@link ShapeSource} backed by a Durable Streams JSON stream, with a distinct
 * resumable offset per message.
 *
 * `read(after)` performs one catch-up pass: it reads from `after`'s base offset,
 * drops the messages that offset already accounts for, and resolves at the
 * stream's head. Call it in a loop to keep tailing a live stream.
 *
 * Throws {@link DurableStreamRetentionError} when the server has already dropped
 * the cursor's position.
 */
export function durableStreamSource<V = Record<string, unknown>, TItem = unknown>(
  config: DurableStreamConfig<V, TItem>,
): ShapeSource<V> {
  return {
    name: config.name,
    async read(after) {
      const client = await loadClient();
      const position = after === undefined ? undefined : parseCompositeOffset(after);
      if (after !== undefined && position === undefined) {
        throw new Error(
          `durableStreamSource: cursor "${after}" is not a composite offset. This source only ever ` +
            `emits "<base>,<messages past base>" offsets; a plain offset means the checkpoint was ` +
            `written by a different source (an Electric shape, say) against the same stream name "${config.name}".`,
        );
      }
      const base = position?.base ?? STREAM_START;
      // A cold handle: the constructor performs no network I/O, whereas
      // `DurableStream.connect` spends a HEAD round trip populating a
      // `contentType` this adapter never reads. Every GET reports the head via
      // `upToDate` anyway, so the HEAD would buy one extra round trip per read —
      // and `read()` is documented to be called in a polling loop.
      const handle = new client.DurableStream(
        config.headers === undefined ? { url: config.url } : { url: config.url, headers: { ...config.headers } },
      );
      return await collectCatchUp<V, TItem>(handle, config, base, position?.consumed ?? 0);
    },
  };
}

async function collectCatchUp<V, TItem>(
  handle: StreamHandle,
  config: DurableStreamConfig<V, TItem>,
  base: string,
  consumed: number,
): Promise<readonly ShapeChange<V>[]> {
  const granularity = config.granularity ?? DEFAULT_GRANULARITY;
  // The position this read resumes from. Held for error messages: it names where
  // the caller asked to start, which stays true however far the loop gets.
  const cursor = composeOffset(base, consumed);
  const deadline = readDeadline(config);
  const changes: ShapeChange<V>[] = [];
  // The offset the current batch was requested at. It advances at every batch
  // boundary so the sub-offset stays small and every checkpoint names a position
  // the server can seek to.
  let currentBase = base;
  let toSkip = consumed;

  // One request per iteration: `live: false` fetches a single response and
  // stops, whether or not it reached the head, so reaching the head — the
  // `read(after)` contract — is this loop's job. Looping also makes the result
  // independent of how the server chunks, which is what keeps the sub-offset
  // meaningful across a re-read.
  try {
    for (;;) {
      const batch = await readOneBatch<TItem>(handle, config.name, currentBase, cursor, deadline);
      if (batch === undefined) break;
      batch.items.forEach((item, index) => {
        const isBatchEnd = index === batch.items.length - 1;
        // At append granularity the whole batch shares its end offset, so `consume`
        // applies it in one transaction under one anchor. At message granularity
        // only the batch's last message takes that offset; the rest are addressed
        // relative to the offset this batch was requested at.
        const offset =
          isBatchEnd || granularity === "append"
            ? composeOffset(batch.offset, 0)
            : composeOffset(currentBase, index + 1);
        if (toSkip > 0) {
          toSkip -= 1;
          return;
        }
        changes.push(mapChange(item, config, offset));
      });
      currentBase = batch.offset;
      if (batch.upToDate) break;
    }
  } finally {
    deadline.cancel();
  }
  return changes;
}

/**
 * Fetch one batch from `from`, or `undefined` when the session ended without
 * delivering one.
 *
 * Returning `undefined` ends the catch-up early rather than failing: the cursor
 * only ever advances over messages actually applied, so a short read costs one
 * extra poll and never a lost change.
 */
async function readOneBatch<TItem>(
  handle: StreamHandle,
  name: string,
  from: string,
  cursor: string,
  deadline: ReadDeadline,
): Promise<JsonBatch<TItem> | undefined> {
  const response = await openRead<TItem>(handle, name, from, cursor);
  let delivered: JsonBatch<TItem> | undefined;
  let resolveDelivery: (() => void) | undefined;
  const delivery = new Promise<void>((resolve) => {
    resolveDelivery = resolve;
  });
  const unsubscribe = response.subscribeJson((batch) => {
    delivered ??= batch;
    resolveDelivery?.();
  });
  try {
    // `closed` can resolve BEFORE the subscriber runs, so it cannot be the
    // completion signal on its own — it only bounds the wait for a batch that is
    // never coming (an empty or errored session).
    await Promise.race([
      delivery,
      response.closed.then(() => new Promise<void>((resolve) => setImmediate(resolve))),
      deadline.expired,
    ]);
  } catch (error) {
    rethrowRead(error, name, cursor);
  } finally {
    unsubscribe();
    response.cancel();
  }
  return delivered;
}

async function openRead<TItem>(
  handle: StreamHandle,
  name: string,
  from: string,
  cursor: string,
): Promise<StreamResponse<TItem>> {
  try {
    return await handle.stream<TItem>({ offset: from, live: false });
  } catch (error) {
    rethrowRead(error, name, cursor);
  }
}

/** The one deadline covering a whole catch-up read, however many batches it takes. */
type ReadDeadline = Readonly<{
  /** Rejects when the deadline passes; never resolves. Safe to race repeatedly. */
  expired: Promise<never>;
  cancel: () => void;
}>;

/**
 * Arm ONE timer for the whole read. A timer per batch would leave every earlier
 * batch's timer armed — each retaining its rejection closure — for the rest of
 * the timeout, long after the read returned.
 */
function readDeadline<V, TItem>(config: DurableStreamConfig<V, TItem>): ReadDeadline {
  const timeoutMs = config.timeoutMs ?? DEFAULT_READ_TIMEOUT_MS;
  if (!Number.isFinite(timeoutMs)) {
    return { expired: new Promise<never>(() => undefined), cancel: () => undefined };
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(
        new Error(
          `durableStreamSource: stream "${config.name}" did not reach its head within ${timeoutMs}ms. ` +
            "Check the durable streams endpoint, network, and proxy buffering. Pass `timeoutMs: Infinity` " +
            "only if you accept an unbounded wait.",
        ),
      );
    }, timeoutMs);
    timer.unref?.();
  });
  // Between batches nothing is racing this promise, so a rejection landing in
  // that window would surface as an unhandled rejection. Attaching a sink keeps
  // it handled without changing what the race observes.
  void expired.catch(() => undefined);
  return { expired, cancel: () => clearTimeout(timer) };
}

/** Map one message through the user's `toChange`, stamping the offset and wrapping any throw with context. */
function mapChange<V, TItem>(
  item: TItem,
  config: DurableStreamConfig<V, TItem>,
  offset: string,
): ShapeChange<V> {
  try {
    return { ...config.toChange(item), offset };
  } catch (error) {
    throw new Error(
      `durableStreamSource: \`toChange\` threw while mapping the message at offset ${offset} on stream ` +
        `"${config.name}": ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  }
}
