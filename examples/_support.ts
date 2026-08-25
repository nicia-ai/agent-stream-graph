// Shared boilerplate for the runnable demos in this directory. Not a demo
// itself (underscore-prefixed) — `pnpm demo*` never targets it.
import { pathToFileURL } from "node:url";

import { type AdapterBackend, type AdapterHistoryStore, type AdapterStore, createAdapterStoreWithSchema, type GraphDef } from "@nicia-ai/typegraph";
import { type AnySqliteDatabase, createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";

/**
 * The adapter-native transaction handle these demos adopt. Naming it once keeps
 * the SQLite driver type out of every demo that only needs "the handle my store
 * and my checkpoint store both enlist in".
 */
export type DemoTransaction = AnySqliteDatabase;

/**
 * A demo store on the adapter surface, so `.backend` and native-handle
 * interop stay available. History is a runtime flag here, so this is the union
 * of both shapes: an `AdapterHistoryStore` is NOT an `AdapterStore` — it
 * deliberately drops `withTransaction`, because raw adoption has no
 * recorded-capture flush point. Demos that must adopt a caller transaction name
 * `AdapterHistoryStore` directly and use `withRecordedTransaction`.
 */
export type DemoStore<G extends GraphDef> =
  | AdapterStore<G, DemoTransaction>
  | AdapterHistoryStore<G, DemoTransaction>;

/** The history-enabled half of {@link DemoStore} — a bitemporal belief store. */
export type DemoHistoryStore<G extends GraphDef> = AdapterHistoryStore<G, DemoTransaction>;

/** A fresh in-process SQLite backend for a demo store or branch. */
export async function makeBackend(): Promise<AdapterBackend<DemoTransaction>> {
  return createLocalSqliteBackend().backend;
}

/**
 * Build a demo store for `graph`, optionally history-enabled. A history store is
 * a replay/at-least-once belief materializer, so `coalesceUnchangedUpserts`
 * defaults on with `history`: re-delivering a byte-identical row is then a true
 * no-op (no history row, no recorded-clock churn) instead of rewriting the row.
 * The consumer needs no change — a coalesced upsert still counts one write intent
 * but captures nothing, the same shape a no-op delete already produces.
 *
 * Overloaded on the `history` literal so each call site gets the exact store
 * type back. That matters because a history store is a distinct type that drops
 * `withTransaction`, and only a non-history store can back a checkpoint book, so
 * collapsing the two would push a cast onto every caller.
 */
export function newStore<G extends GraphDef>(graph: G): Promise<AdapterStore<G, DemoTransaction>>;
export function newStore<G extends GraphDef>(
  graph: G,
  history: false,
  coalesceUnchangedUpserts?: boolean,
): Promise<AdapterStore<G, DemoTransaction>>;
export function newStore<G extends GraphDef>(
  graph: G,
  history: true,
  coalesceUnchangedUpserts?: boolean,
): Promise<DemoHistoryStore<G>>;
export async function newStore<G extends GraphDef>(
  graph: G,
  history = false,
  coalesceUnchangedUpserts = history,
): Promise<DemoStore<G>> {
  const [store] = await createAdapterStoreWithSchema(graph, await makeBackend(), {
    history,
    coalesceUnchangedUpserts,
  });
  return store;
}

/** Run `main()` when this module is the process entry point; exit non-zero on error. */
export function runAsMain(importMetaUrl: string, main: () => Promise<void>): void {
  if (importMetaUrl === pathToFileURL(process.argv[1] ?? "").href) {
    main().catch((error: unknown) => {
      console.error(error);
      process.exit(1);
    });
  }
}
