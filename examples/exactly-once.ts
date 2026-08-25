/**
 * Exactly-once materialization.
 *
 * The library's `consume()` is AT-LEAST-ONCE: the belief projection and the
 * durable cursor advance commit in two separate transactions, so a crash between
 * them re-delivers the change on resume, and an idempotent projector converges.
 * Safe, but every re-delivery re-touches the belief.
 *
 * This demo shows the stronger EXACTLY-ONCE variant: the projection and the
 * cursor advance commit in ONE transaction, so they can never disagree — there
 * is nothing to re-deliver. Achievable only when the source gives a resumable
 * offset PER change (a Postgres LSN changefeed, a Kafka offset). Electric tags a
 * whole catch-up batch with one offset, so an Electric source stays exactly-once
 * only at batch boundaries and at-least-once within a batch — hence the
 * per-change offsets in this demo.
 *
 * The primitives it leans on:
 *   - `store.withRecordedTransaction(externalTx, fn)` returns a receipt:
 *     `receipt.writes.total` for dropped-change detection, and
 *     `receipt.recorded` as the per-offset replay anchor — on a caller-owned
 *     transaction that the cursor write also enlists in.
 *   - `book.recordIn(externalTx, …)` advances the cursor inside that same
 *     transaction (a checkpoint store co-located on the one backend).
 *   - `coalesceUnchangedUpserts`: a re-delivered identical row is a no-op.
 *   - `tx.sqlAvailability`: the context tells the truth — raw SQL is disabled
 *     under history capture, which is WHY the cursor write goes through the
 *     external `db` handle, not `tx.sql`. The non-available arms omit `sql`
 *     entirely, so that mistake does not compile.
 *
 * Run with:  pnpm demo:exactly-once
 */
import {
  type AdapterHistoryStore,
  asNodeId,
  createAdapterStoreWithSchema,
  defineGraph,
  defineNode,
  type RecordedInstant,
} from "@nicia-ai/typegraph";
import { type AnySqliteDatabase, createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";
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
import { runAsMain } from "./_support";

// ============================================================
// A small belief graph — the focus here is the transaction, not the schema.
// ============================================================

const Doc = defineNode("Doc", { schema: z.object({ title: z.string() }) });
const docGraph = defineGraph({ id: "xo_docs", nodes: { Doc: { type: Doc } }, edges: {} });
// The adapter surface: this demo adopts the caller's `db` transaction, which
// lives on `AdapterHistoryStore`, not the portable `HistoryStore`.
type DocStore = AdapterHistoryStore<typeof docGraph, AnySqliteDatabase>;

const project: Projector<typeof docGraph> = async (tx, change) => {
  if (change.operation === "delete") {
    await tx.nodes.Doc.delete(asNodeId(change.key));
    return;
  }
  await tx.nodes.Doc.upsertById(change.key, { title: change.value.title as string });
};

// ============================================================
// Exactly-once consumer: projection + cursor advance in ONE transaction.
// ============================================================

type ExactlyOnceArgs = Readonly<{
  // The concrete better-sqlite3 handle: it frames the transaction (BEGIN/COMMIT)
  // and is the adapter-native transaction both stores enlist in.
  db: BetterSQLite3Database;
  belief: DocStore;
  book: AdoptingCheckpointBook<AnySqliteDatabase>;
  source: ShapeSource;
  project: Projector<typeof docGraph>;
  /** Stop after this many changes — simulates a crash mid-stream. */
  stopAfter?: number;
}>;

async function consumeExactlyOnce(args: ExactlyOnceArgs): Promise<{ processed: number; lastOffset: string | undefined }> {
  const stream = args.source.name;
  const fromOffset = await args.book.lastOffset(stream);
  const changes = await args.source.read(fromOffset);
  let processed = 0;
  // Carry-forward anchor for a no-op change (delete of an absent key, or a
  // coalesced re-delivery) whose transaction captured nothing. The one anchor
  // sourced from `recordedNow()` rather than a receipt: exact only under one
  // consumer per belief store (a concurrent writer could advance this clock).
  let anchor: RecordedInstant | undefined = await args.belief.recordedNow();

  for (let index = 0; index < changes.length; index += 1) {
    if (args.stopAfter !== undefined && processed >= args.stopAfter) break;
    const change = changes[index]!;

    // better-sqlite3 is synchronous, so the caller frames the transaction with
    // explicit BEGIN/COMMIT (its driver rejects an async db.transaction()
    // callback). On Postgres/libsql: `await db.transaction(async (pgTx) => { … })`
    // and pass `pgTx` where `db` is passed here.
    await args.db.run(sql`BEGIN IMMEDIATE`);
    try {
      // The belief store is history-enabled, so it adopts `db` via the recorded
      // path and hands back a receipt.
      const { receipt } = await args.belief.withRecordedTransaction(args.db, async (tx) => {
        await args.project(tx, change);
      });
      if (receipt.writes.total === 0 && change.operation !== "delete") {
        throw new ProjectorRecordedNothingError(stream, change);
      }
      if (receipt.recorded !== undefined) anchor = receipt.recorded;

      // The cursor advance enlists in the SAME `db` transaction. Every offset
      // here is distinct, so each change is its own boundary; a shared-offset
      // (Electric) batch would defer this to the boundary, exactly as consume().
      const next = changes[index + 1];
      const atBoundary = next === undefined || compareOffsets(next.offset, change.offset) > 0;
      if (atBoundary && anchor !== undefined) {
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

const CHANGES: readonly ShapeChange[] = [
  { offset: "1_0", shape: "doc", key: "d1", operation: "insert", value: { title: "Alpha" } },
  { offset: "2_0", shape: "doc", key: "d2", operation: "insert", value: { title: "Beta" } },
  { offset: "3_0", shape: "doc", key: "d3", operation: "insert", value: { title: "Gamma" } },
];

async function titles(view: { query: DocStore["query"] }): Promise<string[]> {
  const rows = await view.query().from("Doc", "d").select((c) => ({ title: c.d.title })).execute();
  return rows.map((r) => r.title).sort();
}

export async function main(): Promise<void> {
  const rule = "━".repeat(74);
  console.log(rule);
  console.log(" Exactly-once materialization — projection + cursor in one transaction");
  console.log(rule);

  // Belief store and checkpoint store share ONE backend, so they can share ONE
  // transaction. The belief store coalesces unchanged upserts.
  const { backend, db } = createLocalSqliteBackend();
  const [belief] = await createAdapterStoreWithSchema(docGraph, backend, {
    history: true,
    coalesceUnchangedUpserts: true,
  });
  const [cursorStore] = await createAdapterStoreWithSchema(checkpointGraph, backend);
  const book = typeGraphAdoptingCheckpoints(cursorStore);
  const source = mockShapeSource("docs", CHANGES);

  try {
    // Why the cursor write uses the external `db` handle, not `tx.sql`:
    await belief.transaction(async (tx) => {
      console.log(`\n  inside a history transaction: tx.sqlAvailability = "${tx.sqlAvailability}"`);
      console.log("  → raw SQL is disabled under history capture, so the cursor store adopts `db` directly.");
    });

    // (1) Consume, but "crash" after 2 of 3 changes.
    console.log("\n" + rule);
    console.log(" (1) Consume 2/3, then crash");
    console.log(rule);
    const partial = await consumeExactlyOnce({ db, belief, book, source, project, stopAfter: 2 });
    console.log(`\n  processed ${partial.processed} — durable cursor at ${await book.lastOffset("docs")}`);
    console.log(`  belief: ${(await titles(belief)).join(", ")}`);

    // (2) A projection that throws rolls back BOTH the belief AND the cursor.
    console.log("\n" + rule);
    console.log(" (2) A failing projection is atomic — neither belief nor cursor moves");
    console.log(rule);
    const boom: Projector<typeof docGraph> = async (tx, change) => {
      await tx.nodes.Doc.upsertById(change.key, { title: change.value.title as string });
      throw new Error("projector boom");
    };
    const cursorBefore = await book.lastOffset("docs");
    const titlesBefore = await titles(belief);
    try {
      await consumeExactlyOnce({ db, belief, book, source, project: boom });
    } catch {
      // expected
    }
    const cursorAfter = await book.lastOffset("docs");
    const titlesAfter = await titles(belief);
    console.log(`\n  cursor: ${cursorBefore} → ${cursorAfter}  (unmoved)`);
    console.log(`  belief: [${titlesBefore.join(", ")}] → [${titlesAfter.join(", ")}]  (unchanged — d3 not half-written)`);

    // (3) Resume with the real projector — exactly-once, nothing re-applied.
    console.log("\n" + rule);
    console.log(" (3) Resume — exactly-once");
    console.log(rule);
    const anchorBefore = await belief.recordedNow();
    const resumed = await consumeExactlyOnce({ db, belief, book, source, project });
    console.log(`\n  processed ${resumed.processed} (just d3) — cursor at ${await book.lastOffset("docs")}`);
    console.log(`  belief: ${(await titles(belief)).join(", ")}`);

    // Re-run at the end of the stream: nothing to do, no belief churn.
    const replay = await consumeExactlyOnce({ db, belief, book, source, project });
    const anchorAfter = await belief.recordedNow();
    console.log(`\n  re-run at stream end: processed ${replay.processed}; recorded clock unchanged: ${anchorBefore !== anchorAfter ? "advanced by d3 only" : "flat"}`);

    // (4) Replay the belief at a past offset via its recorded anchor.
    console.log("\n" + rule);
    console.log(" (4) Replay by offset");
    console.log(rule);
    const at1 = await book.anchorFor("docs", "1_0");
    const at3 = await book.anchorFor("docs", "3_0");
    console.log(`\n  asOfRecorded(anchorFor 1_0): ${(await titles(belief.asOfRecorded(at1!))).join(", ")}`);
    console.log(`  asOfRecorded(anchorFor 3_0): ${(await titles(belief.asOfRecorded(at3!))).join(", ")}`);

    console.log("\n" + rule);
    console.log(" Projection and cursor commit as one. A crash leaves nothing to re-deliver.");
    console.log(rule + "\n");
  } finally {
    await Promise.allSettled([belief.close(), cursorStore.close()]);
  }
}

runAsMain(import.meta.url, main);
