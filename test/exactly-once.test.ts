import {
  type AdapterHistoryStore,
  type AdapterHistoryTransactionContext,
  type AdapterTransactionContext,
  asNodeId,
  createAdapterStoreWithSchema,
  defineGraph,
  defineNode,
  isRecordedCaptureGuardError,
  type RecordedInstant,
} from "@nicia-ai/typegraph";
import {
  type AnySqliteDatabase,
  createLocalSqliteBackend,
} from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";
import { sql } from "drizzle-orm";
import type { BetterSQLite3Database } from "drizzle-orm/better-sqlite3";
import { beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";

import {
  type AdoptingCheckpointBook,
  checkpointGraph,
  typeGraphAdoptingCheckpoints,
} from "../src";

const Item = defineNode("Item", { schema: z.object({ label: z.string() }) });
const beliefGraph = defineGraph({
  id: "xo_belief",
  nodes: { Item: { type: Item } },
  edges: {},
});
type BeliefStore = AdapterHistoryStore<typeof beliefGraph, AnySqliteDatabase>;

// Type-level: `withTransaction` is a compile error on a history store. Asserted
// against the ADAPTER history store, the only surface that carries transaction
// adoption at all; on the portable `HistoryStore` the method is absent from
// every store. If TypeGraph ever re-allowed it, the unused `@ts-expect-error`
// would fail `tsc`.
function _historyWithTransactionIsACompileError(
  belief: AdapterHistoryStore<typeof beliefGraph, AnySqliteDatabase>,
  db: BetterSQLite3Database,
): void {
  // @ts-expect-error — use withRecordedTransaction on a history store
  belief.withTransaction(db);
}

// Type-level: the non-available `sqlAvailability` arms OMIT `sql`, so reading
// the handle under history capture does not compile — the mistake is caught
// before it can reach the fail-loud runtime getter. Never called; `tsc` is the
// assertion. Reading `sql` off the union is likewise an error until narrowed to
// "available".
function _rawSqlUnderHistoryIsACompileError(
  tx: AdapterHistoryTransactionContext<typeof beliefGraph, AnySqliteDatabase>,
  any: AdapterTransactionContext<typeof beliefGraph, AnySqliteDatabase>,
): void {
  // @ts-expect-error — history capture disables raw SQL; `sql` is absent
  void tx.sql;
  // @ts-expect-error — narrow sqlAvailability to "available" before reading sql
  void any.sql;
  if (any.sqlAvailability === "available") {
    const narrowed: AnySqliteDatabase = any.sql;
    void narrowed;
  }
}

describe("exactly-once building blocks", () => {
  let belief: BeliefStore;
  let book: AdoptingCheckpointBook<AnySqliteDatabase>;
  let db: BetterSQLite3Database;

  beforeEach(async () => {
    // Belief store and checkpoint store on ONE backend so they share ONE transaction.
    const local = createLocalSqliteBackend();
    db = local.db;
    [belief] = await createAdapterStoreWithSchema(beliefGraph, local.backend, {
      history: true,
      coalesceUnchangedUpserts: true,
    });
    const [cursorStore] = await createAdapterStoreWithSchema(
      checkpointGraph,
      local.backend,
    );
    book = typeGraphAdoptingCheckpoints(cursorStore);
  });

  it("recordIn advances the cursor atomically with the projection (commit)", async () => {
    await db.run(sql`BEGIN IMMEDIATE`);
    const { receipt } = await belief.withRecordedTransaction(db, async (tx) => {
      await tx.nodes.Item.upsertById("a", { label: "a1" });
    });
    // The receipt is what makes the adopted path exactly-once: drop-detection
    // and the per-offset anchor both come off it.
    expect(receipt.writes.total).toBe(1);
    expect(receipt.recorded).toBeDefined();

    await book.recordIn(db, "s", "001", receipt.recorded!);
    await db.run(sql`COMMIT`);

    expect(await book.lastOffset("s")).toBe("001");
    expect(await book.anchorFor("s", "001")).toBe(receipt.recorded);
    // The anchor reconstructs exactly the belief this offset committed.
    expect(await belief.nodes.Item.getById(asNodeId("a"))).toMatchObject({
      label: "a1",
    });
  });

  it("rolls back the cursor advance AND the projection together", async () => {
    // Seed a committed checkpoint at 001.
    await db.run(sql`BEGIN IMMEDIATE`);
    const first = await belief.withRecordedTransaction(db, async (tx) => {
      await tx.nodes.Item.upsertById("a", { label: "a1" });
    });
    await book.recordIn(db, "s", "001", first.receipt.recorded!);
    await db.run(sql`COMMIT`);

    // Now a transaction that projects, advances the cursor, then fails.
    await db.run(sql`BEGIN IMMEDIATE`);
    try {
      const second = await belief.withRecordedTransaction(db, async (tx) => {
        await tx.nodes.Item.upsertById("b", { label: "b1" });
      });
      await book.recordIn(db, "s", "002", second.receipt.recorded!);
      throw new Error("projector boom");
    } catch {
      await db.run(sql`ROLLBACK`);
    }

    // Both the belief write and the cursor advance are gone.
    expect(await book.lastOffset("s")).toBe("001");
    expect(await belief.nodes.Item.getById(asNodeId("b"))).toBeUndefined();
  });

  it("tx.measure scopes drop-detection past in-transaction bookkeeping", async () => {
    // A projector that DROPS the change, in a transaction that also writes an
    // in-graph cursor. Without measure the cursor write (total=1) masks the drop.
    const dropped = await belief.transactionWithReceipt(async (tx) => {
      const projected = await tx.measure(async () => {
        /* projector writes nothing */
      });
      await tx.nodes.Item.upsertById("cursor", { label: "offset-1" });
      return projected;
    });
    expect(dropped.result.receipt.writes.total).toBe(0); // projector: dropped
    expect(dropped.receipt.writes.total).toBe(1); // outer: just the bookkeeping

    // A projector that writes through the scoped context is attributed to it.
    const wrote = await belief.transactionWithReceipt(async (tx) => {
      const projected = await tx.measure(async (scoped) => {
        await scoped.nodes.Item.upsertById("d", { label: "d1" });
      });
      await tx.nodes.Item.upsertById("cursor", { label: "offset-2" });
      return projected;
    });
    expect(wrote.result.receipt.writes.total).toBe(1); // projector only
    expect(wrote.receipt.writes.total).toBe(2); // projector + bookkeeping
  });

  it("a scoped receipt never carries a recorded instant", async () => {
    // The commit instant is a per-transaction flush concern, unknowable mid-scope.
    const outcome = await belief.transactionWithReceipt(async (tx) => {
      return tx.measure(async (scoped) => {
        await scoped.nodes.Item.upsertById("d", { label: "d1" });
      });
    });
    expect(outcome.result.receipt.recorded).toBeUndefined();
    expect(outcome.receipt.recorded).toBeDefined(); // the outer transaction did commit
  });

  it("sqlAvailability tells the truth and the guard error is branchable", async () => {
    await belief.transaction(async (tx) => {
      // Under history capture raw SQL is disabled; the discriminant says so
      // rather than tx.sql reading truthy then throwing.
      expect(tx.sqlAvailability).toBe("history");
      let threw = false;
      try {
        await (
          Reflect.get(tx, "sql") as {
            run: (query: unknown) => Promise<unknown>;
          }
        ).run(sql`SELECT 1`);
      } catch (error) {
        threw = true;
        expect(isRecordedCaptureGuardError(error)).toBe(true);
      }
      expect(threw).toBe(true);
    });
  });

  it("anchors an exactly-once offset to its own transaction, not a concurrent writer's", async () => {
    // The receipt.recorded from withRecordedTransaction is this transaction's
    // instant, not the graph-global clock a concurrent writer would advance.
    await db.run(sql`BEGIN IMMEDIATE`);
    const { receipt } = await belief.withRecordedTransaction(db, async (tx) => {
      await tx.nodes.Item.upsertById("a", { label: "a1" });
    });
    const anchor: RecordedInstant = receipt.recorded!;
    await book.recordIn(db, "s", "001", anchor);
    await db.run(sql`COMMIT`);

    // A later, unrelated write advances the graph-global clock.
    await belief.nodes.Item.upsertById("z", { label: "later" });
    const global = await belief.recordedNow();
    expect(global).not.toBe(anchor);
    // Offset 001 still reconstructs the belief as of its own commit.
    const at1 = await book.anchorFor("s", "001");
    expect(
      await belief.asOfRecorded(at1!).nodes.Item.getById(asNodeId("z")),
    ).toBeUndefined();
  });
});
