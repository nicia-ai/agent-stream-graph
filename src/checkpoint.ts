import {
  type AdapterStore,
  asNodeId,
  asRecordedInstant,
  defineGraph,
  defineNode,
  type RecordedInstant,
  type Store,
} from "@nicia-ai/typegraph";
import { z } from "zod";

import { compareOffsets } from "./offset.js";

/**
 * Durable offset bookkeeping: the highest offset processed per stream, and the
 * recorded-time anchor captured the moment each offset was applied. The anchor
 * is what makes a stream replayable — `store.asOfRecorded(anchorFor(offset))`
 * reconstructs the belief as it stood when that offset was consumed.
 *
 * Anchors are {@link RecordedInstant}s, not bare strings: `record` accepts only
 * an instant a store actually allocated (a transaction receipt's `recorded`, or
 * `store.recordedNow()`), and `anchorFor` re-brands on the way out of storage.
 * A wall-clock string can therefore never be checkpointed as an anchor — it
 * carries no logical revision, so it cannot name which commit to replay.
 */
export interface CheckpointBook {
  lastOffset(stream: string): Promise<string | undefined>;
  record(
    stream: string,
    offset: string,
    anchor: RecordedInstant,
  ): Promise<void>;
  anchorFor(
    stream: string,
    offset: string,
  ): Promise<RecordedInstant | undefined>;
}

/**
 * A checkpoint book that can join a caller-owned adapter transaction.
 * `TNativeTransaction` is the precise native handle being adopted — for example,
 * `AnySqliteDatabase` for the local SQLite adapter.
 */
export interface AdoptingCheckpointBook<
  TNativeTransaction,
> extends CheckpointBook {
  /**
   * Like {@link record}, but enlists in a caller-owned transaction instead of
   * opening its own. This is the exactly-once building block: when the belief
   * projection and this checkpoint write share one transaction (the belief store
   * and this checkpoint store on one backend, both adopting `externalTx`), a
   * crash can never leave the cursor and the belief disagreeing — there is
   * nothing to re-deliver. The at-least-once {@link record} instead accepts
   * re-delivery and leans on an idempotent projector. The caller frames the
   * transaction boundary (see the exactly-once example).
   *
   * The checkpoint store MUST be non-history (the default): this adopts the
   * transaction via `store.withTransaction`, which a history-enabled store
   * rejects (`RECORDED_CAPTURE_REQUIRES_CALLBACK_TRANSACTION`) because raw
   * adoption has no recorded-capture flush point. A checkpoint is a cursor, not
   * a belief — it wants no bitemporal history — so this is the natural shape.
   */
  recordIn(
    externalTx: TNativeTransaction,
    stream: string,
    offset: string,
    anchor: RecordedInstant,
  ): Promise<void>;
}

// A per-(stream, offset) row backing `anchorFor` — O(1) lookup by id. Retained
// for the lifetime of the stream so any past offset can be replayed; a future
// compaction pass could prune anchors older than the high-water minus N.
const Checkpoint = defineNode("Checkpoint", {
  schema: z.object({
    stream: z.string(),
    offset: z.string(),
    anchor: z.string(),
  }),
});

// One row per stream holding the high-water offset, backing `lastOffset` as an
// O(1) point read instead of scanning every Checkpoint row.
const Stream = defineNode("Stream", {
  schema: z.object({ name: z.string(), lastOffset: z.string() }),
});

/** Graph schema backing {@link typeGraphCheckpoints}; build a store from it. */
export const checkpointGraph = defineGraph({
  id: "agent_stream_checkpoints",
  nodes: { Checkpoint: { type: Checkpoint }, Stream: { type: Stream } },
  edges: {},
});

/**
 * Portable checkpoint store used by the at-least-once API. Build it with
 * `createStoreWithSchema`; an AdapterStore is also assignable when an application
 * already owns one.
 */
export type CheckpointStore = Store<typeof checkpointGraph>;

/** Adapter checkpoint store used only for caller-owned transaction adoption. */
export type AdoptingCheckpointStore<TNativeTransaction> = AdapterStore<
  typeof checkpointGraph,
  TNativeTransaction
>;

const checkpointRowId = (stream: string, offset: string): string =>
  JSON.stringify([stream, offset]);

/**
 * The Checkpoint/Stream write surface — the shape shared by the store's own
 * `nodes` and the `nodes` of an adopted transaction context, so
 * {@link CheckpointBook.record} and {@link AdoptingCheckpointBook.recordIn}
 * share one advance body over either.
 */
type CheckpointWriteNodes = Store<typeof checkpointGraph>["nodes"];

async function advanceCheckpoint(
  nodes: CheckpointWriteNodes,
  stream: string,
  offset: string,
  anchor: RecordedInstant,
): Promise<void> {
  await nodes.Checkpoint.upsertById(checkpointRowId(stream, offset), {
    stream,
    offset,
    anchor,
  });
  const current = await nodes.Stream.getById(asNodeId(stream));
  if (
    current === undefined ||
    compareOffsets(offset, current.lastOffset) >= 0
  ) {
    await nodes.Stream.upsertById(stream, { name: stream, lastOffset: offset });
  }
}

/** A {@link CheckpointBook} persisted in a TypeGraph store, durable across restarts. */
export function typeGraphCheckpoints(store: CheckpointStore): CheckpointBook {
  return {
    async lastOffset(stream) {
      const row = await store.nodes.Stream.getById(asNodeId(stream));
      return row?.lastOffset;
    },
    async record(stream, offset, anchor) {
      await advanceCheckpoint(store.nodes, stream, offset, anchor);
    },
    async anchorFor(stream, offset) {
      const row = await store.nodes.Checkpoint.getById(
        asNodeId(checkpointRowId(stream, offset)),
      );
      // Re-brand at the storage boundary: the anchor round-tripped through an
      // untyped `z.string()` column. `asRecordedInstant` validates the canonical
      // `r1:<revision>:<timestamp>` form, so a hand-edited or corrupted row fails
      // here rather than deeper in an `asOfRecorded` read.
      return row === undefined ? undefined : asRecordedInstant(row.anchor);
    },
  };
}

/**
 * Adds exactly-once checkpoint adoption to the portable checkpoint book.
 * Build the store with `createAdapterStoreWithSchema` so the native handle stays
 * precisely typed.
 */
export function typeGraphAdoptingCheckpoints<TNativeTransaction>(
  store: AdoptingCheckpointStore<TNativeTransaction>,
): AdoptingCheckpointBook<TNativeTransaction> {
  return {
    ...typeGraphCheckpoints(store),
    async recordIn(externalTx, stream, offset, anchor) {
      await advanceCheckpoint(
        store.withTransaction(externalTx).nodes,
        stream,
        offset,
        anchor,
      );
    },
  };
}
