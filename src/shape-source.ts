import type { Operation, ShapeChange, ShapeChangeInput } from "./types.js";
import { compareOffsets } from "./offset.js";

/**
 * A durable, resumable source of shape changes. `read(after)` returns every
 * change strictly after the `after` offset, in stream order, up to the source's
 * current head. Passing `undefined` reads from the beginning.
 */
export interface ShapeSource<V = Record<string, unknown>> {
  readonly name: string;
  read(after: string | undefined): Promise<readonly ShapeChange<V>[]>;
}

/** An in-memory source over a fixed, ordered change list (tests and demos). */
export function mockShapeSource<V = Record<string, unknown>>(
  name: string,
  changes: readonly ShapeChange<V>[],
): ShapeSource<V> {
  const ordered = [...changes].sort((left, right) => compareOffsets(left.offset, right.offset));
  return {
    name,
    async read(after) {
      return after === undefined ? ordered : ordered.filter((change) => compareOffsets(change.offset, after) > 0);
    },
  };
}

// --- Electric adapter -------------------------------------------------------
//
// Wraps `@electric-sql/client`'s `ShapeStream`. The client is an OPTIONAL
// dependency, imported lazily — this module loads fine without it; the adapter
// only requires it when `read()` is first called. `read(after)` collects one
// catch-up batch: it subscribes from `after`, drains change messages until the
// `up-to-date` control message, then resolves. Call it in a loop to keep
// tailing a live shape.
//
// OFFSET SEMANTICS: Electric does not give per-message resume offsets. Every
// change in a catch-up batch shares the batch's `ShapeStream.lastOffset`. The
// adapter tags all changes in the batch with that one offset, so
// `anchorFor(stream, offset)` reconstructs the belief as of the END of the
// batch, not after each individual change. Intermediate within-batch states are
// still in the belief store's history (each projector write recorded its own
// anchor) but are reachable only by recorded time, not by stream offset.

/** A change message as surfaced by `@electric-sql/client`. */
export type ElectricChangeMessage = Readonly<{
  key: string;
  value: Record<string, unknown>;
  headers: Readonly<{ operation: Operation } & Record<string, unknown>>;
}>;

type ElectricMessage = Readonly<{
  key?: string;
  value?: Record<string, unknown>;
  headers: Readonly<{ operation?: Operation; control?: string } & Record<string, unknown>>;
}>;

type ElectricStream = Readonly<{
  subscribe: (onMessages: (messages: readonly ElectricMessage[]) => void, onError?: (error: unknown) => void) => () => void;
  lastOffset: string;
  /** The server-assigned shape handle; required (with the offset) to resume. */
  shapeHandle?: string;
}>;

type ElectricClient = Readonly<{
  ShapeStream: new (
    options: Readonly<{ url: string; params: Record<string, unknown>; offset?: string; handle?: string; signal?: AbortSignal }>,
  ) => ElectricStream;
  isChangeMessage: (message: ElectricMessage) => boolean;
}>;

export type ElectricShapeConfig<V = Record<string, unknown>> = Readonly<{
  /** Electric shape endpoint, e.g. `"http://localhost:3000/v1/shape"`. */
  url: string;
  /** Logical name for this stream (the checkpoint key). */
  name: string;
  /** Shape params — at minimum the table, plus any `where`/`columns` filters. */
  params: Readonly<{ table: string } & Record<string, unknown>>;
  /**
   * Shape handle to resume an existing shape. Electric rejects a resume offset
   * without its handle, so to resume mid-shape across a process restart, persist
   * the handle from {@link onHandle} and pass it back here. Within one process
   * the adapter captures and reuses the handle automatically.
   */
  handle?: string;
  /**
   * Called with the server-assigned shape handle after each batch. Persist it
   * (next to your checkpoint) and pass it back as {@link handle} on restart to
   * resume mid-shape. Without a persisted handle a cold start re-fetches the
   * shape from the beginning — safe and idempotent, but it re-streams the shape
   * once.
   */
  onHandle?: (handle: string) => void;
  /**
   * Maps one Electric change message to a `ShapeChange` (minus `offset`) for
   * your projector; the adapter stamps the authoritative Electric resume offset.
   */
  toChange: (message: ElectricChangeMessage) => ShapeChangeInput<V>;
  /**
   * Maximum time to wait for the batch's `up-to-date` control message, in
   * milliseconds. Default 30s; pass `Infinity` to disable. Without a bound a
   * stalled Electric server would hang `read()` forever.
   */
  timeoutMs?: number;
}>;

/** The shape is no longer valid; drop the handle and re-fetch from the start. */
export class ElectricMustRefetchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ElectricMustRefetchError";
  }
}

/** Electric reported an `error` control message on the shape stream. */
export class ElectricControlError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ElectricControlError";
  }
}

const DEFAULT_BATCH_TIMEOUT_MS = 30_000;
const CONTROL_UP_TO_DATE = "up-to-date";
const CONTROL_MUST_REFETCH = "must-refetch";
const CONTROL_ERROR = "error";
/**
 * Electric sentinel offsets that carry no resumable position.
 *
 * `"-1"` is deliberately NOT `offset.ts`'s `STREAM_START`, despite the shared
 * literal: there it is a durable stream's real, seekable origin, while here it
 * means Electric has not yet assigned this shape a position — checkpointing it
 * would record a cursor no resume can use.
 */
const SENTINEL_OFFSETS = new Set(["-1", "now"]);

/**
 * The projectable operations, sourced as a `satisfies Record<Operation, true>`
 * so `tsc` fails if the `Operation` union gains or loses a member until this is
 * updated to match.
 */
const OPERATIONS: ReadonlySet<string> = new Set(
  Object.keys({ insert: true, update: true, delete: true } satisfies Record<Operation, true>),
);

/** A narrowed change, or the precise reason the message is not a projectable change. */
type NarrowedChange = { readonly change: ElectricChangeMessage } | { readonly reason: string };

/** Narrow an Electric message to a typed change message, or report why it cannot be. */
function asChangeMessage(message: ElectricMessage): NarrowedChange {
  const op = message.headers.operation;
  if (op === undefined || !OPERATIONS.has(op)) {
    return { reason: `unsupported \`headers.operation\` ${JSON.stringify(op)} (expected one of insert, update, delete)` };
  }
  if (message.key === undefined) return { reason: `missing \`key\` (operation ${op})` };
  if (message.value === undefined) return { reason: `missing \`value\` (operation ${op})` };
  return { change: { key: message.key, value: message.value, headers: message.headers } as ElectricChangeMessage };
}

/** A {@link ShapeSource} backed by an Electric `ShapeStream`. */
export function electricShapeSource<V = Record<string, unknown>>(config: ElectricShapeConfig<V>): ShapeSource<V> {
  // The handle Electric requires to resume from a real offset. Seeded from a
  // persisted handle, refreshed after every batch so the next read() can resume.
  let handle = config.handle;
  return {
    name: config.name,
    async read(after) {
      const client = (await import("@electric-sql/client")) as unknown as ElectricClient;
      const aborter = new AbortController();
      // Resuming from a real offset requires the handle too. Without it (a cold
      // start that never persisted one) fall back to a full re-fetch from the
      // start rather than letting Electric throw MissingShapeHandleError; the
      // idempotent projector dedups the replay.
      const resuming = after !== undefined && handle !== undefined;
      const stream = new client.ShapeStream({
        url: config.url,
        params: config.params,
        signal: aborter.signal,
        ...(resuming ? { offset: after, handle } : {}),
      });
      const timeoutMs = config.timeoutMs ?? DEFAULT_BATCH_TIMEOUT_MS;
      const changes = await collectBatch(stream, aborter, client, config.toChange, timeoutMs);
      // Capture the handle Electric assigned so the next read() can resume.
      if (stream.shapeHandle !== undefined && stream.shapeHandle !== handle) {
        handle = stream.shapeHandle;
        config.onHandle?.(handle);
      }
      return changes;
    },
  };
}

function collectBatch<V>(
  stream: ElectricStream,
  aborter: AbortController,
  client: ElectricClient,
  toChange: (message: ElectricChangeMessage) => ShapeChangeInput<V>,
  timeoutMs: number,
): Promise<readonly ShapeChange<V>[]> {
  return new Promise((resolve, reject) => {
    const buffered: ElectricChangeMessage[] = [];
    let settled = false;
    let unsubscribe: (() => void) | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const finish = (result: { ok: true; value: readonly ShapeChange<V>[] } | { ok: false; error: Error }): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      unsubscribe?.();
      aborter.abort();
      if (result.ok) {
        resolve(result.value);
      } else {
        reject(result.error);
      }
    };

    if (Number.isFinite(timeoutMs)) {
      timer = setTimeout(
        () =>
          finish({
            ok: false,
            error: new Error(
              `electricShapeSource: batch did not reach "up-to-date" within ${timeoutMs}ms. ` +
                "Check the Electric endpoint, network, and proxy buffering. Pass `timeoutMs: Infinity` only if you accept an unbounded wait.",
            ),
          }),
        timeoutMs,
      );
    }

    const onMessages = (messages: readonly ElectricMessage[]): void => {
      for (const message of messages) {
        if (message.headers.control !== undefined) {
          if (message.headers.control === CONTROL_UP_TO_DATE) {
            // Electric does not provide per-message offsets; the whole batch
            // shares `stream.lastOffset`. Tag every buffered change with it.
            const offset = stream.lastOffset;
            if (buffered.length > 0 && SENTINEL_OFFSETS.has(offset)) {
              finish({
                ok: false,
                error: new Error(
                  `electricShapeSource: batch carried ${buffered.length} change(s) but ShapeStream.lastOffset is the ` +
                    `non-resumable sentinel "${offset}". Cannot assign a durable resume offset.`,
                ),
              });
              return;
            }
            const changes: ShapeChange<V>[] = buffered.map((cm) => mapChange(cm, toChange, offset));
            finish({ ok: true, value: changes });
            return;
          }
          if (message.headers.control === CONTROL_MUST_REFETCH) {
            finish({
              ok: false,
              error: new ElectricMustRefetchError(
                "Electric sent `must-refetch`: the shape is no longer valid. Drop any persisted handle and call read(undefined) to re-fetch from the start.",
              ),
            });
            return;
          }
          if (message.headers.control === CONTROL_ERROR) {
            finish({
              ok: false,
              error: new ElectricControlError(
                `Electric sent an \`error\` control message: ${JSON.stringify(message.headers)}`,
              ),
            });
            return;
          }
          // Unknown control — ignore (forward-compatible with new control types).
          continue;
        }
        if (client.isChangeMessage(message)) {
          const narrowed = asChangeMessage(message);
          if ("reason" in narrowed) {
            finish({
              ok: false,
              error: new Error(
                `electricShapeSource: cannot project change message — ${narrowed.reason}. Full headers: ${JSON.stringify(message.headers)}.`,
              ),
            });
            return;
          }
          buffered.push(narrowed.change);
        }
      }
    };

    // Guard the callback: a throw here (most likely from the user's `toChange`)
    // would otherwise escape into Electric's publish loop, surface as an uncaught
    // rejection, and leave read() to hang until timeout. Routing it through
    // finish() rejects read() promptly with the real cause.
    unsubscribe = stream.subscribe(
      (messages) => {
        try {
          onMessages(messages);
        } catch (error) {
          finish({ ok: false, error: error instanceof Error ? error : new Error(String(error)) });
        }
      },
      (error) => {
        finish({ ok: false, error: error instanceof Error ? error : new Error(String(error)) });
      },
    );
  });
}

/** Map one buffered change through the user's `toChange`, stamping the batch offset and wrapping any throw with context. */
function mapChange<V>(
  cm: ElectricChangeMessage,
  toChange: (message: ElectricChangeMessage) => ShapeChangeInput<V>,
  offset: string,
): ShapeChange<V> {
  try {
    return { ...toChange(cm), offset };
  } catch (error) {
    throw new Error(
      `electricShapeSource: \`toChange\` threw while mapping change "${cm.key}": ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  }
}
