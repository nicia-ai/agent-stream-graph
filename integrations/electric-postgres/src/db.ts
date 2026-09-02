/**
 * Shared Postgres wiring for `seed.ts` and `demo.ts`.
 *
 * One backend, one connection pool, for BOTH stores this package needs: the
 * `agent_fleet` belief graph and the library's own `agent_stream_checkpoints`
 * cursor graph. That is safe on one physical database because TypeGraph's
 * Postgres tables carry a `graph_id` discriminator column — they are
 * multi-tenant by design, not one physical table set per graph — so two
 * `defineGraph`s coexist in the same `nodes`/`edges`/... tables without
 * collision. This is the SAME Postgres instance `agent_events` lives in
 * (`sql/schema.sql`); Electric replicates from it, TypeGraph just also has
 * tables in it.
 */
import {
  type AdapterBackend,
  type AdapterHistoryStore,
  type AdapterStore,
  createAdapterStoreWithSchema,
  type GraphDef,
} from "@nicia-ai/typegraph";
import { createPostgresBackend, type AnyPgTransaction } from "@nicia-ai/typegraph/adapters/drizzle/postgres";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";

/** Local compose port from `docker-compose.yml`; override for a non-default deployment. */
export const DEFAULT_DATABASE_URL = "postgresql://postgres:password@localhost:54321/agent_fleet?sslmode=disable";

/** The adapter-native transaction handle both stores in this package enlist in. */
export type FleetTransaction = AnyPgTransaction;

export type FleetStore<G extends GraphDef> = AdapterStore<G, FleetTransaction> | AdapterHistoryStore<G, FleetTransaction>;
export type FleetHistoryStore<G extends GraphDef> = AdapterHistoryStore<G, FleetTransaction>;

/** An open connection to the demo database: one pool, one backend, closed together. */
export type DemoDatabase = Readonly<{
  pool: Pool;
  backend: AdapterBackend<FleetTransaction>;
  close: () => Promise<void>;
}>;

/**
 * Open the demo database. `backend.close()` is TypeGraph's own teardown (flush
 * any buffered state); it does not own — and so does not close — the `pg.Pool`
 * this function constructs, so `close()` here ends both explicitly.
 */
export function connectDemoDatabase(databaseUrl: string = DEFAULT_DATABASE_URL): DemoDatabase {
  const pool = new Pool({ connectionString: databaseUrl });
  const backend = createPostgresBackend(drizzle(pool));
  return {
    pool,
    backend,
    async close() {
      await backend.close();
      await pool.end();
    },
  };
}

/**
 * Build a store for `graph` against `db`, optionally history-enabled.
 * Overloaded on the `history` literal, same reasoning as the root repo's
 * `examples/_support.ts`: a history store is a distinct type dropping
 * `withTransaction`, and only a non-history store backs a `CheckpointBook`.
 */
export function newStore<G extends GraphDef>(graph: G, db: DemoDatabase): Promise<AdapterStore<G, FleetTransaction>>;
export function newStore<G extends GraphDef>(
  graph: G,
  db: DemoDatabase,
  history: false,
  coalesceUnchangedUpserts?: boolean,
): Promise<AdapterStore<G, FleetTransaction>>;
export function newStore<G extends GraphDef>(
  graph: G,
  db: DemoDatabase,
  history: true,
  coalesceUnchangedUpserts?: boolean,
): Promise<FleetHistoryStore<G>>;
export async function newStore<G extends GraphDef>(
  graph: G,
  db: DemoDatabase,
  history = false,
  coalesceUnchangedUpserts = history,
): Promise<FleetStore<G>> {
  const [store] = await createAdapterStoreWithSchema(graph, db.backend, { history, coalesceUnchangedUpserts });
  return store;
}
