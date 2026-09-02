/**
 * In-process SQLite backends for the desk's stores.
 *
 * Mirrors the pattern `examples/_support.ts` uses for the library's own demos:
 * a fresh backend per store (so exports, imports, and branches never contend
 * for one connection — see the README's "give the export and the import
 * their own backend handles" trap), and a history flag threaded through an
 * overload so every call site gets back the exact store type it asked for.
 */
import {
  type AdapterBackend,
  type AdapterHistoryStore,
  type AdapterStore,
  createAdapterStoreWithSchema,
  type GraphDef,
} from "@nicia-ai/typegraph";
import { type AnySqliteDatabase, createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";

/** The adapter-native transaction handle every desk store adopts. */
export type DeskTransaction = AnySqliteDatabase;

/**
 * A desk store on the adapter surface. History is a runtime flag, so this is
 * the union of both shapes: an `AdapterHistoryStore` is NOT an `AdapterStore`
 * — it drops `withTransaction`, because raw adoption has no recorded-capture
 * flush point.
 */
export type DeskStore<G extends GraphDef> = AdapterStore<G, DeskTransaction> | AdapterHistoryStore<G, DeskTransaction>;

/** The history-enabled half of {@link DeskStore} — a bitemporal belief store. */
export type DeskHistoryStore<G extends GraphDef> = AdapterHistoryStore<G, DeskTransaction>;

/** A fresh in-process SQLite backend for a desk store or a merge branch. */
export async function makeBackend(): Promise<AdapterBackend<DeskTransaction>> {
  return createLocalSqliteBackend().backend;
}

/**
 * Build a desk store for `graph`, optionally history-enabled. History
 * defaults `coalesceUnchangedUpserts` on: a re-delivered, byte-identical row
 * is then a true no-op, which is what makes at-least-once redelivery safe to
 * replay through `consume`.
 */
export function newStore<G extends GraphDef>(graph: G): Promise<AdapterStore<G, DeskTransaction>>;
export function newStore<G extends GraphDef>(
  graph: G,
  history: false,
  coalesceUnchangedUpserts?: boolean,
): Promise<AdapterStore<G, DeskTransaction>>;
export function newStore<G extends GraphDef>(
  graph: G,
  history: true,
  coalesceUnchangedUpserts?: boolean,
): Promise<DeskHistoryStore<G>>;
export async function newStore<G extends GraphDef>(
  graph: G,
  history = false,
  coalesceUnchangedUpserts = history,
): Promise<DeskStore<G>> {
  const [store] = await createAdapterStoreWithSchema(graph, await makeBackend(), { history, coalesceUnchangedUpserts });
  return store;
}
