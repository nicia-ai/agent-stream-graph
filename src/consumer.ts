import type { GraphDef, RecordedInstant, Store, TransactionContext } from "@nicia-ai/typegraph";

import type { CheckpointBook } from "./checkpoint.js";
import { compareOffsets } from "./offset.js";
import type { ShapeSource } from "./shape-source.js";
import type { Operation, ShapeChange } from "./types.js";

/**
 * Applies one shape change to a belief store. MUST be idempotent: at-least-once
 * streams re-deliver, so re-applying a change has to converge (use `upsertById`
 * / `getOrCreateByEndpoints`, not `create`).
 *
 * The value type `V` defaults to `Record<string, unknown>`; supply it to get
 * typed `change.value` without `as string` casts at call sites that handle a
 * single shape. Projectors that handle many shapes keep the default and narrow
 * per `change.shape`.
 *
 * Write through the transaction-scoped `tx` you are handed (its `nodes`/`edges`
 * collections), not a captured store reference. Every insert/update change is
 * expected to complete at least one write intent; a `delete` may be a legitimate
 * no-op (deleting an absent key). The consumer treats an insert/update that
 * completes no write intent as a dropped change and throws
 * {@link ProjectorRecordedNothingError}, so do not swallow write errors or write
 * to a different store.
 */
export type Projector<G extends GraphDef, V = Record<string, unknown>> = (
  tx: TransactionContext<G>,
  change: ShapeChange<V>,
) => Promise<void>;

export type ConsumeResult = Readonly<{
  processed: number;
  fromOffset: string | undefined;
  lastOffset: string | undefined;
}>;

/**
 * Largest number of changes applied in one transaction. Bounds the write-lock
 * hold and the history capture buffer: an Electric initial sync tags its whole
 * shape with one offset, so an unbounded run would open a single transaction
 * over the entire shape.
 *
 * Purely a transaction-size bound, never a checkpoint boundary — a run split
 * across several transactions still checkpoints only at its true offset
 * boundary, so a crash mid-run re-delivers the whole run.
 */
export const DEFAULT_MAX_BATCH_SIZE = 1000;

export type ConsumeArgs<G extends GraphDef, V = Record<string, unknown>> = Readonly<{
  source: ShapeSource<V>;
  /** Belief store to project into. MUST be created with `{ history: true }`. */
  store: Store<G>;
  checkpoints: CheckpointBook;
  project: Projector<G, V>;
  /** Stop after this many changes — simulates a crash. Omit to drain the batch. */
  stopAfter?: number;
  /** Transaction-size bound. Defaults to {@link DEFAULT_MAX_BATCH_SIZE}. */
  maxBatchSize?: number;
}>;

/**
 * Thrown when a projector completes no write intent for a non-`delete` change.
 * An insert/update is expected to write to the belief store; if the transaction
 * receipt counts nothing the change would be silently dropped and the cursor
 * advanced past it, losing it forever. Failing loud surfaces the common causes:
 * the projector wrote to a different store than the one it was handed, or it
 * swallowed a write error.
 */
export class ProjectorRecordedNothingError extends Error {
  readonly stream: string;
  readonly offset: string;
  readonly operation: Operation;
  readonly shape: string;
  readonly key: string;

  constructor(stream: string, change: ShapeChange<unknown>) {
    super(
      `Projector completed no write for ${change.operation} change "${change.shape}"/"${change.key}" ` +
        `at offset ${change.offset} on stream "${stream}" (transaction receipt counted 0 writes). An ` +
        `insert/update change must write to the belief store; nothing did, so the change would be ` +
        `silently dropped. Verify the projector writes through the handle it is given (not a different ` +
        `store) and does not swallow write errors.`,
    );
    this.name = "ProjectorRecordedNothingError";
    this.stream = stream;
    this.offset = change.offset;
    this.operation = change.operation;
    this.shape = change.shape;
    this.key = change.key;
  }
}

/**
 * Thrown when `maxBatchSize` is not a positive integer.
 *
 * Rejected before any change is read, because every invalid value fails silently
 * rather than loudly: a zero, negative, or `NaN` bound makes each offset run
 * yield an EMPTY batch, so the cursor never advances and `consume` spins
 * forever; a fractional bound admits `ceil(maxBatchSize)` changes per
 * transaction, quietly exceeding the cap it was given.
 */
export class InvalidMaxBatchSizeError extends RangeError {
  readonly maxBatchSize: number;

  constructor(maxBatchSize: number) {
    super(
      `maxBatchSize must be a positive integer, received ${maxBatchSize}. It bounds ` +
        `how many changes share one transaction; a non-positive or non-finite bound ` +
        `produces empty batches and never advances the cursor, and a fractional bound ` +
        `admits more changes per transaction than it names.`,
    );
    this.name = "InvalidMaxBatchSizeError";
    this.maxBatchSize = maxBatchSize;
  }
}

/**
 * Consume a durable shape source into a history-enabled belief store. Resumes
 * from the last durable checkpoint, applies each change with `project` inside a
 * transaction, and records the offset paired with the recorded-time instant that
 * transaction committed at.
 *
 * Crash-safe (a restart resumes from the cursor), at-least-once idempotent
 * (re-delivery re-applies the same upserts), and replayable by offset (each
 * offset's anchor reconstructs the belief via `store.asOfRecorded`).
 *
 * Changes sharing one resumable offset are applied in BOUNDED TRANSACTIONS and
 * CHECKPOINTED ONCE: a run of N changes consumes `ceil(N / maxBatchSize)`
 * transactions, and therefore that many recorded instants, but advances the
 * cursor a single time at its offset boundary.
 *
 * Each commit consumes one recorded revision from a strictly monotonic per-graph
 * counter, so committing per change would burn N revisions while only the run's
 * boundary offset is ever addressable by {@link CheckpointBook.anchorFor} — the
 * intermediate instants name beliefs no checkpoint can reach.
 *
 * How finely a stream is addressed is therefore the SOURCE's decision, not this
 * function's: one transaction and one anchor per distinct offset is the rule
 * either way. `durableStreamSource`'s `granularity` chooses between an offset
 * per message (per-message anchors, a transaction each) and an offset per append
 * (one transaction per append, like an Electric batch).
 *
 * Both signals come from transaction receipts, and they are distinct — note they
 * are read at different scopes:
 *
 * - `receipt.writes.total` — write intents the projector completed, read from a
 *   PER-CHANGE `tx.measure` scope. Zero means the projector dropped the change
 *   (wrong store, swallowed error, unhandled shape, or an empty bulk call, which
 *   counts 0 by input length). For a non-`delete` change that is
 *   {@link ProjectorRecordedNothingError}. The scope is what keeps this exact
 *   under batching: the batch receipt aggregates, so one dropped change among
 *   many would still show a non-zero total and pass unnoticed.
 * - `receipt.recorded` — the recorded instant THIS transaction allocated, read
 *   from the BATCH receipt (a scoped receipt's `recorded` is always undefined,
 *   the commit instant being a per-transaction flush concern). Undefined when
 *   the batch captured nothing. Never `store.recordedNow()`, which is the
 *   graph-global high-water: under a concurrent writer it would hand back
 *   someone else's commit instant and the offset would anchor to a belief this
 *   stream never produced.
 *
 * The two disagree on a legitimate no-op: deleting an absent key completes a
 * write intent (`writes.total === 1`) but captures nothing (`recorded ===
 * undefined`), so the offset carries the prior anchor forward and
 * `anchorFor(offset)` still reconstructs the unchanged belief.
 *
 * Create the belief store with `{ history: true, coalesceUnchangedUpserts: true }`.
 * Coalescing makes a re-delivered byte-identical row a true no-op — the same
 * `writes.total === 1`, `recorded === undefined` shape as a no-op delete, which
 * this function already handles — so at-least-once re-delivery and full replays
 * stop rewriting rows and churning recorded history. Without it, replaying an
 * N-event log rewrites every row and grows history by N.
 *
 * The durable checkpoint advances only at an OFFSET BOUNDARY — a change after
 * which no later change in the batch shares its offset. A source whose batch
 * shares one resume offset (Electric tags a whole catch-up batch with
 * `ShapeStream.lastOffset`) is resumable only at that boundary: `read(after)`
 * returns changes STRICTLY after `after`, so recording the shared offset
 * mid-batch would make a crash skip the batch's remaining changes on resume.
 * Deferring to the boundary keeps a partial batch fully re-deliverable. Under
 * `durableStreamSource` at message granularity every change carries a distinct
 * offset, so every change is a boundary and the deferral never triggers.
 */
export async function consume<G extends GraphDef, V = Record<string, unknown>>(
  args: ConsumeArgs<G, V>,
): Promise<ConsumeResult> {
  const maxBatchSize = args.maxBatchSize ?? DEFAULT_MAX_BATCH_SIZE;
  if (!Number.isInteger(maxBatchSize) || maxBatchSize < 1) {
    throw new InvalidMaxBatchSizeError(maxBatchSize);
  }

  const fromOffset = await args.checkpoints.lastOffset(args.source.name);
  const changes = await args.source.read(fromOffset);
  let processed = 0;
  let lastOffset = fromOffset;
  // Seeds the anchor a leading no-op change carries forward, and surfaces a
  // store created without `{ history: true }` before any change is applied.
  // `undefined` on a graph that has never been written to.
  //
  // This is the ONE place the anchor comes from `recordedNow()` rather than a
  // receipt, and it is only ever checkpointed if the first change(s) at an
  // offset boundary capture nothing (a no-op delete, or — now that belief stores
  // coalesce — a re-delivered identical row). After a crash the change's own
  // commit instant is unrecoverable, so `recordedNow()` at resume is the best
  // available anchor: it reflects wherever the belief actually is, which for a
  // crash-window replay is correctly AHEAD of the durable cursor. That is exact
  // only under the library's invariant of ONE consumer per belief store — a
  // concurrent writer could advance this clock past the change's real instant.
  // Do not point two consumers at one belief store.
  let anchor: RecordedInstant | undefined = await args.store.recordedNow();

  let index = 0;

  while (index < changes.length) {
    if (args.stopAfter !== undefined && processed >= args.stopAfter) break;

    // The next transaction's changes: consecutive changes sharing one offset,
    // bounded by maxBatchSize and by stopAfter (which simulates a crash, so it
    // must land on a transaction boundary rather than tear one).
    const offset = changes[index]!.offset;
    let end = index;
    while (
      end < changes.length &&
      compareOffsets(changes[end]!.offset, offset) === 0 &&
      end - index < maxBatchSize &&
      (args.stopAfter === undefined || processed + (end - index) < args.stopAfter)
    ) {
      end += 1;
    }
    const batch = changes.slice(index, end);

    // One transaction for the whole run, but a `measure` scope per change: the
    // batch receipt aggregates writes, so only the scoped receipts can tell a
    // dropped change from its neighbours' writes.
    const { receipt } = await args.store.transactionWithReceipt(async (tx) => {
      for (const change of batch) {
        const { receipt: scoped } = await tx.measure(async (change_tx) => {
          await args.project(change_tx, change);
        });
        // Thrown INSIDE the transaction so the drop rolls back the whole batch
        // rather than committing its neighbours: the cursor never advances, so
        // every change in the run re-delivers intact.
        if (scoped.writes.total === 0 && change.operation !== "delete") {
          throw new ProjectorRecordedNothingError(args.source.name, change);
        }
      }
    });

    if (receipt.recorded !== undefined) anchor = receipt.recorded;
    processed += batch.length;
    index = end;

    // Only durably advance at an offset boundary (see the function-level note):
    // when the next change carries a strictly greater offset, or the batch ends.
    // A run split by maxBatchSize or cut short by `stopAfter` stops short of the
    // boundary, leaving the cursor at the prior boundary so the whole run is
    // re-delivered and idempotently re-applied. Skip when there is no anchor yet
    // (empty graph).
    const next = changes[index];
    const atOffsetBoundary = next === undefined || compareOffsets(next.offset, offset) > 0;
    if (atOffsetBoundary && anchor !== undefined) {
      await args.checkpoints.record(args.source.name, offset, anchor);
      lastOffset = offset;
    }
  }

  return { processed, fromOffset, lastOffset };
}
