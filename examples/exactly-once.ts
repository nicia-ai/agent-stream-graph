/**
 * Exactly-once materialization.
 *
 * The library's `consume()` is AT-LEAST-ONCE: the belief projection and the
 * durable cursor advance commit in two separate transactions, so a crash between
 * them re-delivers the change on resume, and an idempotent projector converges.
 * Safe, but only because the projector is idempotent.
 *
 * This demo shows the stronger EXACTLY-ONCE variant: the projection and the
 * cursor advance commit in ONE transaction, so they can never disagree — there
 * is nothing to re-deliver. Achievable only when the source gives a resumable
 * offset PER change (a Postgres LSN changefeed, a Kafka offset). Electric tags a
 * whole catch-up batch with one offset, so an Electric source stays exactly-once
 * only at batch boundaries and at-least-once within a batch — hence the
 * per-change offsets in this demo.
 *
 * It proves the claim from both sides of the cursor write: a projector that
 * fails after writing leaves the belief untouched, and a crash after the cursor
 * write but before COMMIT leaves the cursor untouched too.
 *
 * The primitives it leans on:
 *   - `store.withRecordedTransaction(externalTx, fn)` adopts a caller-owned
 *     transaction and returns a receipt: `receipt.writes.total` for
 *     dropped-change detection, `receipt.recorded` as the per-offset anchor.
 *   - `book.recordIn(externalTx, …)` advances the cursor inside that same
 *     transaction (a checkpoint store co-located on the one backend).
 *   - `tx.sqlAvailability`: raw SQL is disabled under history capture, which is
 *     WHY the cursor write goes through the external `db` handle, not `tx.sql`.
 *     The non-available arms omit `sql` entirely, so that mistake does not
 *     compile.
 *
 * Run with:  pnpm demo:exactly-once
 */
import { asNodeId, createAdapterStoreWithSchema, defineGraph, defineNode, type RecordedInstant } from "@nicia-ai/typegraph";
import { createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";
import { sql } from "drizzle-orm";
import type { BetterSQLite3Database } from "drizzle-orm/better-sqlite3";
import { z } from "zod";

import {
  checkpointGraph,
  compareOffsets,
  mockShapeSource,
  ProjectorRecordedNothingError,
  type AdoptingCheckpointBook,
  typeGraphAdoptingCheckpoints,
  type Projector,
  type ShapeChange,
  type ShapeSource,
} from "../src";
import { assertEqual, type DemoHistoryStore, type DemoTransaction, expectRejection, RULE, runAsMain, section } from "./_support";

// ============================================================
// A small belief graph — the focus here is the transaction, not the schema.
// ============================================================

const Doc = defineNode("Doc", { schema: z.object({ title: z.string() }) });
const docGraph = defineGraph({ id: "xo_docs", nodes: { Doc: { type: Doc } }, edges: {} });
type DocStore = DemoHistoryStore<typeof docGraph>;
type DocRow = Readonly<{ title: string }>;

const project: Projector<typeof docGraph, DocRow> = async (tx, change) => {
  if (change.operation === "delete") {
    await tx.nodes.Doc.delete(asNodeId(change.key));
    return;
  }
  await tx.nodes.Doc.upsertById(change.key, { title: change.value.title });
};

// ============================================================
// Exactly-once consumer: projection + cursor advance in ONE transaction.
// ============================================================

type ExactlyOnceArgs = Readonly<{
  // The concrete better-sqlite3 handle: it frames the transaction (BEGIN/COMMIT)
  // and is the adapter-native transaction both stores enlist in.
  db: BetterSQLite3Database;
  belief: DocStore;
  book: AdoptingCheckpointBook<DemoTransaction>;
  source: ShapeSource<DocRow>;
  project: Projector<typeof docGraph, DocRow>;
  /** Stop after this many changes — simulates a crash mid-stream. */
  stopAfter?: number | undefined;
}>;

async function consumeExactlyOnce(args: ExactlyOnceArgs): Promise<{ processed: number; lastOffset: string | undefined }> {
  const stream = args.source.name;
  const changes = await args.source.read(await args.book.lastOffset(stream));
  let processed = 0;
  // Carry-forward anchor for a no-op change (delete of an absent key, or a
  // coalesced re-delivery) whose transaction captured nothing. The one anchor
  // sourced from `recordedNow()` rather than a receipt: exact only under one
  // consumer per belief store, and `undefined` until the graph has any history.
  let anchor: RecordedInstant | undefined = await args.belief.recordedNow();

  for (const [index, change] of changes.entries()) {
    if (args.stopAfter !== undefined && processed >= args.stopAfter) break;

    // better-sqlite3 is synchronous, so the caller frames the transaction with
    // explicit BEGIN/COMMIT (its driver rejects an async db.transaction()
    // callback). On Postgres/libsql: `await db.transaction(async (pgTx) => { … })`
    // and pass `pgTx` where `db` is passed here.
    await args.db.run(sql`BEGIN IMMEDIATE`);
    try {
      const { receipt } = await args.belief.withRecordedTransaction(args.db, async (tx) => {
        // Same rule as `consume()`: with no anchor yet, make this commit mint a
        // revision even if the change captures nothing. Asking only then keeps a
        // replay from churning history.
        if (anchor === undefined) tx.requestRecordedRevision();
        await args.project(tx, change);
      });
      if (receipt.writes.total === 0 && change.operation !== "delete") {
        throw new ProjectorRecordedNothingError(stream, change);
      }
      if (receipt.recorded !== undefined) anchor = receipt.recorded;

      // Every offset here is distinct, so each change is its own boundary; a
      // shared-offset (Electric) batch would defer this to the boundary, exactly
      // as consume() does.
      const next = changes[index + 1];
      const atBoundary = next === undefined || compareOffsets(next.offset, change.offset) > 0;
      if (atBoundary) {
        if (anchor === undefined) {
          throw new Error(`exactly-once: no recorded anchor at offset ${change.offset} despite requesting a revision`);
        }
        await args.book.recordIn(args.db, stream, change.offset, anchor);
      }
      await args.db.run(sql`COMMIT`);
    } catch (error) {
      // One transaction wraps projection AND cursor — rollback undoes both.
      await args.db.run(sql`ROLLBACK`);
      throw error;
    }
    processed += 1;
  }

  return { processed, lastOffset: await args.book.lastOffset(stream) };
}

// ============================================================
// The demo
// ============================================================

const STREAM_NAME = "docs";
const CHANGES: readonly ShapeChange<DocRow>[] = [
  { offset: "1_0", shape: "doc", key: "d1", operation: "insert", value: { title: "Alpha" } },
  { offset: "2_0", shape: "doc", key: "d2", operation: "insert", value: { title: "Beta" } },
  { offset: "3_0", shape: "doc", key: "d3", operation: "insert", value: { title: "Gamma" } },
];
const ALL_TITLES = CHANGES.map((change) => change.value.title);

class SimulatedProjectorFailure extends Error {
  constructor(key: string) {
    super(`simulated projector failure after writing "${key}"`);
    this.name = "SimulatedProjectorFailure";
  }
}

const failAfterWriting: Projector<typeof docGraph, DocRow> = async (tx, change) => {
  await project(tx, change);
  throw new SimulatedProjectorFailure(change.key);
};

class SimulatedCrashBeforeCommit extends Error {
  /** The cursor as read inside the doomed transaction, after its write landed. */
  readonly uncommittedCursor: string | undefined;

  constructor(uncommittedCursor: string | undefined) {
    super(`simulated crash after advancing the cursor to ${uncommittedCursor}, before COMMIT`);
    this.name = "SimulatedCrashBeforeCommit";
    this.uncommittedCursor = uncommittedCursor;
  }
}

/**
 * A checkpoint book whose cursor write succeeds and is then followed by a crash:
 * the last point a failure can strike before COMMIT. Reading the cursor back on
 * the same connection sees the write, which proves it really landed before the
 * rollback undid it.
 */
function crashAfterCursorWrite(book: AdoptingCheckpointBook<DemoTransaction>): AdoptingCheckpointBook<DemoTransaction> {
  return {
    ...book,
    async recordIn(externalTx, stream, offset, anchor) {
      await book.recordIn(externalTx, stream, offset, anchor);
      throw new SimulatedCrashBeforeCommit(await book.lastOffset(stream));
    },
  };
}

async function titles(view: Pick<DocStore, "query">): Promise<string[]> {
  const rows = await view.query().from("Doc", "d").select((c) => ({ title: c.d.title })).execute();
  return rows.map((row) => row.title).sort();
}

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" Exactly-once materialization — projection + cursor in one transaction");
  console.log(RULE);

  // Belief store and checkpoint store share ONE backend, so they can share ONE
  // transaction.
  const { backend, db } = createLocalSqliteBackend();
  const [belief] = await createAdapterStoreWithSchema(docGraph, backend, {
    history: true,
    coalesceUnchangedUpserts: true,
  });
  const [cursorStore] = await createAdapterStoreWithSchema(checkpointGraph, backend);
  const book = typeGraphAdoptingCheckpoints(cursorStore);
  const consumeWith = (overrides: Partial<Pick<ExactlyOnceArgs, "project" | "book" | "stopAfter">> = {}) =>
    consumeExactlyOnce({ db, belief, book, source: mockShapeSource(STREAM_NAME, CHANGES), project, ...overrides });

  try {
    await belief.transaction(async (tx) => {
      console.log(`\n  inside a history transaction: tx.sqlAvailability = "${tx.sqlAvailability}"`);
      console.log("  → raw SQL is disabled under history capture, so the cursor store adopts `db` directly.");
    });

    section("(1) Consume 2 of 3 changes, then stop — a simulated crash");
    const partial = await consumeWith({ stopAfter: 2 });
    const beliefAfterPartial = await titles(belief);
    assertEqual(partial, { processed: 2, lastOffset: "2_0" }, "partial run");
    assertEqual(beliefAfterPartial, ALL_TITLES.slice(0, 2), "belief after the partial run");
    console.log(`\n  processed ${partial.processed} — durable cursor at ${partial.lastOffset}`);
    console.log(`  belief: ${beliefAfterPartial.join(", ")}`);

    section("(2) A failure anywhere before COMMIT moves neither belief nor cursor");
    const failure = await expectRejection(consumeWith({ project: failAfterWriting }), SimulatedProjectorFailure);
    console.log(`\n  projector threw: ${failure.message}`);
    const beliefAfterFailure = await titles(belief);
    assertEqual(await book.lastOffset(STREAM_NAME), partial.lastOffset, "cursor after the failed projection");
    assertEqual(beliefAfterFailure, beliefAfterPartial, "belief after the failed projection");
    console.log(`  cursor: still ${partial.lastOffset}  (unmoved)`);
    console.log(`  belief: ${beliefAfterFailure.join(", ")}  (unchanged — d3's write was rolled back)`);

    const crash = await expectRejection(consumeWith({ book: crashAfterCursorWrite(book) }), SimulatedCrashBeforeCommit);
    console.log(`\n  crashed after the cursor write: ${crash.message}`);
    assertEqual(crash.uncommittedCursor, "3_0", "cursor inside the transaction, before the crash");
    assertEqual(await book.lastOffset(STREAM_NAME), partial.lastOffset, "cursor after the crash before COMMIT");
    assertEqual(await titles(belief), beliefAfterPartial, "belief after the crash before COMMIT");
    console.log(`  cursor: ${crash.uncommittedCursor} inside the transaction, ${partial.lastOffset} after it  (write rolled back)`);
    console.log(`  belief: ${beliefAfterPartial.join(", ")}  (unchanged — d3 rolled back with its cursor)`);

    section("(3) Resume — exactly-once");
    const resumed = await consumeWith();
    const beliefAfterResume = await titles(belief);
    assertEqual(resumed, { processed: 1, lastOffset: "3_0" }, "resumed run");
    assertEqual(beliefAfterResume, ALL_TITLES, "belief after resume");
    console.log(`\n  processed ${resumed.processed} (just d3) — cursor at ${resumed.lastOffset}`);
    console.log(`  belief: ${beliefAfterResume.join(", ")}`);

    const clockAtEnd = await belief.recordedNow();
    const rerun = await consumeWith();
    assertEqual(rerun.processed, 0, "changes processed by a re-run at stream end");
    assertEqual(await belief.recordedNow(), clockAtEnd, "recorded clock across a re-run at stream end");
    console.log(`\n  re-run at stream end: processed ${rerun.processed}, recorded clock unchanged — no belief churn`);

    section("(4) Replay by offset");
    console.log("");
    for (const [index, change] of CHANGES.entries()) {
      const anchor = await book.anchorFor(STREAM_NAME, change.offset);
      if (anchor === undefined) throw new Error(`exactly-once: no anchor recorded for offset ${change.offset}`);
      const believed = await titles(belief.asOfRecorded(anchor));
      assertEqual(believed, ALL_TITLES.slice(0, index + 1), `belief replayed at offset ${change.offset}`);
      console.log(`  asOfRecorded(anchorFor ${change.offset}): ${believed.join(", ")}`);
    }

    console.log("\n" + RULE);
    console.log(" Projection and cursor commit as one. A crash leaves nothing to re-deliver.");
    console.log(RULE + "\n");
  } finally {
    await Promise.allSettled([belief.close(), cursorStore.close()]);
  }
}

runAsMain(import.meta.url, main);
