/**
 * Wires a TypeGraph SQLite backend onto a Durable Object's own SQLite
 * storage.
 *
 * `drizzle(ctx.storage)` is drizzle-orm's Durable Object driver
 * (`drizzle-orm/durable-sqlite`) — a synchronous session over
 * `DurableObjectStorage.sql`. `createSqliteBackend` auto-detects that driver
 * and selects `transactionMode: "do-sqlite"` (TypeGraph 0.52+, upstream
 * issue #140): writes run through `ctx.storage.transaction(async ...)`, the
 * DO's own async storage-transaction runner, rather than SQL
 * `BEGIN`/`COMMIT` (which `ctx.storage.sql.exec` cannot issue) or
 * `ctx.storage.transactionSync` (which cannot span an `await`, and
 * TypeGraph's managed write path does). No hint is passed here — detection
 * is automatic for `drizzle(ctx.storage)`.
 *
 * This is what makes the Durable Object path genuinely different from D1
 * (`src/d1-refusal.ts`): this backend REPORTS transactions, so none of the
 * `CONSTRAINT_WRITE_FENCE_UNSUPPORTED` refusals described in the README's
 * "Limitations" section apply to it.
 *
 * Deliberately imports the driver-agnostic
 * `@nicia-ai/typegraph/adapters/drizzle/sqlite` entry point, not
 * `.../sqlite/local` — the `local` entry pulls in `better-sqlite3`, a native
 * Node addon that cannot load in workerd.
 */
import type { AdapterBackend } from "@nicia-ai/typegraph";
import { createSqliteBackend, type AnySqliteDatabase } from "@nicia-ai/typegraph/adapters/drizzle/sqlite";
import { drizzle } from "drizzle-orm/durable-sqlite";

/** Build a TypeGraph backend directly on a Durable Object's SQLite storage. */
export function durableObjectSqliteBackend(storage: DurableObjectStorage): AdapterBackend<AnySqliteDatabase> {
  return createSqliteBackend(drizzle(storage));
}
