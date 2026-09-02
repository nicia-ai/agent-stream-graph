/**
 * D1 refusal — the honest counterpart to `backend.ts`.
 *
 * `backend.ts` puts a REAL transactional TypeGraph backend on Durable Object
 * SQLite storage (`transactionMode: "do-sqlite"`, auto-detected). D1 is a
 * different story: D1 has no interactive multi-statement transaction of its
 * own. Per Cloudflare's own documentation, issuing `BEGIN TRANSACTION` (or
 * `SAVEPOINT`) through the D1 binding returns a `D1_ERROR` pointing you at
 * Durable Object storage transactions instead — D1 itself has no equivalent.
 * The one atomic primitive D1 offers is `D1Database.batch()`, which needs
 * every statement decided up front (no branching on an intermediate result
 * within the same atomic unit) — not the "commit a reservation row together
 * with the row it gates, having just read whether the reservation is free"
 * shape TypeGraph's constraint fencing needs.
 *   https://developers.cloudflare.com/d1/worker-api/d1-database/#batch
 *   https://github.com/cloudflare/workers-sdk/issues/2733
 *
 * `@nicia-ai/typegraph`'s `createSqliteBackend` KNOWS this: it auto-detects a
 * `drizzle-orm/d1` connection and reports `transactionMode: "none"` for it,
 * exactly like it auto-detects Durable Objects and reports "do-sqlite" (see
 * `backend.ts`). This script cannot open a real D1 database — no Cloudflare
 * account is available in this environment, and the constraints on this
 * package forbid attempting a live deploy — but it does not need to: the
 * refusal fires off `backend.capabilities.transactions`, and
 * `createSqliteBackend`'s own `executionProfile` option lets a caller state
 * that same transactionless profile directly. This script does exactly that,
 * against a real (in-memory) SQLite database, and lets TypeGraph's real
 * refusal machinery throw the real error a D1-backed deployment would get.
 * Nothing here is simulated or narrated — every error below is thrown by the
 * same code path the README documents, reproduced end to end and verified by
 * actually running it (see the README for the captured transcript).
 *
 * This file runs under plain Node, not workerd — it exercises the library's
 * backend-capability logic directly, with no Worker or Durable Object in the
 * loop. That is why it is typechecked separately (`tsconfig.verify.json`,
 * documented in the README) rather than through `pnpm typecheck`, which
 * covers only the code that actually deploys to workerd.
 *
 * Run with:  npx tsx src/d1-refusal.ts
 */
import { ConfigurationError, createAdapterStore, createAdapterStoreWithSchema } from "@nicia-ai/typegraph";
import { createSqliteBackend, generateSqliteMigrationSQL } from "@nicia-ai/typegraph/adapters/drizzle/sqlite";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";

import { beliefGraph } from "./graph.js";

async function main(): Promise<void> {
  const rawDb = new Database(":memory:");
  const db = drizzle(rawDb);
  // The same execution-profile hint the doc comment on `SqliteBackendOptions`
  // names for D1 explicitly: "Set `transactionMode: 'none'` for drivers
  // without transactions (e.g. Cloudflare D1)." A real `drizzle-orm/d1`
  // connection gets this SAME profile from auto-detection, not a hint —
  // stating it here reproduces exactly what that detection would report,
  // without requiring a live D1 database.
  const backend = createSqliteBackend(db, { executionProfile: { transactionMode: "none" } });

  console.log("=== 1. createStoreWithSchema refuses a transactionless backend outright ===");
  try {
    await createAdapterStoreWithSchema(beliefGraph, backend, { history: true, coalesceUnchangedUpserts: true });
    throw new Error("expected createStoreWithSchema to refuse a transactionless backend, but it succeeded");
  } catch (error) {
    if (!(error instanceof ConfigurationError)) throw error;
    console.log(`  refused: ${error.message}`);
  }

  console.log();
  console.log("=== 2. The documented workaround: unmanaged createStore over pre-applied DDL ===");
  console.log('  (README: "apply generateSqliteMigrationSQL output out of band and open the');
  console.log('   graph with the unmanaged createStore" — history disabled too: revision');
  console.log("   tracking needs a transaction just as much as schema commits do.)");
  rawDb.exec(generateSqliteMigrationSQL());
  const belief = createAdapterStore(beliefGraph, backend);
  console.log("  createStore succeeded — no schema commit, so no transaction was needed for THIS call.");

  console.log();
  console.log("=== 3. Unconstrained writes need no fence and keep working ===");
  const alice = await belief.nodes.Entity.upsertById("alice", { label: "Alice" });
  const bob = await belief.nodes.Entity.upsertById("bob", { label: "Bob" });
  console.log(`  node.upsertById OK: ${alice.id}, ${bob.id} — no unique constraint declared on Entity, so no fence applies.`);

  console.log();
  console.log("=== 4. edge.getOrCreateByEndpoints IS fenced — refused with CONSTRAINT_WRITE_FENCE_UNSUPPORTED ===");
  try {
    await belief.edges.relatesTo.getOrCreateByEndpoints(
      { kind: "Entity", id: "alice" },
      { kind: "Entity", id: "bob" },
      { label: "knows" },
    );
    throw new Error("expected the edge write to be refused on a transactionless backend, but it succeeded");
  } catch (error) {
    if (!(error instanceof ConfigurationError)) throw error;
    if (error.details.code !== "CONSTRAINT_WRITE_FENCE_UNSUPPORTED") {
      throw new Error(`expected error.details.code === "CONSTRAINT_WRITE_FENCE_UNSUPPORTED", got ${JSON.stringify(error.details)}`);
    }
    console.log(`  refused: ${error.message}`);
    console.log(`  error.details = ${JSON.stringify(error.details)}`);
    console.log(`  error.suggestion = ${error.suggestion ?? "(none)"}`);
  }

  console.log();
  console.log("This is exactly the README's claim, reproduced against a real transactionless");
  console.log("SQLite backend rather than narrated: edge writes are endpoint-matched, and both");
  console.log("the match-key fence and the coalescing it enables are transaction-scoped — a");
  console.log("backend with no transactions cannot commit a reservation row together with the");
  console.log("row it gates, so `getOrCreateByEndpoints` refuses instead of running unfenced.");
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
