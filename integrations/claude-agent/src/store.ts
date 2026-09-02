/**
 * Local SQLite-backed store construction for the demo and its tests, trimmed
 * to exactly the two shapes this package needs: one history-enabled belief
 * store, and one non-history checkpoint store.
 *
 * `examples/_support.ts` carries the same pattern for the library's own demos,
 * and this is deliberately a separate copy rather than an import. Each package
 * under `integrations/` is meant to be liftable out of this repo intact — a
 * demo whose setup lives in someone else's directory is harder to copy than it
 * is to re-read.
 *
 * The overloads are load-bearing: `history`'s LITERAL type is what selects the
 * return type, because an `AdapterHistoryStore` is not an `AdapterStore` (it
 * drops `withTransaction`). Collapsing them into one signature returning the
 * union would push a cast onto every call site.
 */
import { type AdapterHistoryStore, type AdapterStore, createAdapterStoreWithSchema, type GraphDef } from "@nicia-ai/typegraph";
import { type AnySqliteDatabase, createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";

async function freshBackend() {
  return createLocalSqliteBackend().backend;
}

export function newStore<G extends GraphDef>(graph: G, history: false): Promise<AdapterStore<G, AnySqliteDatabase>>;
export function newStore<G extends GraphDef>(graph: G, history: true): Promise<AdapterHistoryStore<G, AnySqliteDatabase>>;
export async function newStore<G extends GraphDef>(
  graph: G,
  history: boolean,
): Promise<AdapterStore<G, AnySqliteDatabase> | AdapterHistoryStore<G, AnySqliteDatabase>> {
  const [store] = await createAdapterStoreWithSchema(graph, await freshBackend(), {
    history,
    coalesceUnchangedUpserts: history,
  });
  return store;
}
