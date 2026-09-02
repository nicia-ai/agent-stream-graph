/**
 * `AgentMaterializer` — one Durable Object per agent, holding that agent's
 * whole belief graph in its own SQLite storage (see `backend.ts` for how).
 *
 * Why per-agent DO isolation is the right shape for THIS library, not just a
 * convenient one, is argued in `../README.md` ("Why a Durable Object per
 * agent"). In short: the library's own documented limitation is "one
 * consumer per belief store" (concurrent writers can mis-anchor a carry-
 * forward checkpoint). Routing every request for an agent to the same DO
 * instance (`idFromName(agentId)`, in `worker.ts`) makes that structural —
 * Cloudflare's runtime guarantees a DO id has at most one active instance —
 * rather than an operational rule an operator has to remember and enforce
 * themselves.
 */
import {
  checkpointGraph,
  consume,
  type ConsumeResult,
  graphProjector,
  mockShapeSource,
  type ShapeChange,
  typeGraphCheckpoints,
} from "@nicia-ai/agent-stream-graph";
import {
  type AdapterHistoryStore,
  createAdapterStoreWithSchema,
} from "@nicia-ai/typegraph";
import type { AnySqliteDatabase } from "@nicia-ai/typegraph/adapters/drizzle/sqlite";
import { DurableObject } from "cloudflare:workers";

import { durableObjectSqliteBackend } from "./backend.js";
import { beliefGraph, type BeliefGraph, type EventBatch } from "./graph.js";

export type Env = Readonly<{
  MATERIALIZER: DurableObjectNamespace<AgentMaterializer>;
}>;

/** What this DO exposes for the Worker's read route. */
export type AgentSnapshot = Readonly<{ agentId: string; entityCount: number }>;

type Ready = Readonly<{
  belief: AdapterHistoryStore<BeliefGraph, AnySqliteDatabase>;
  checkpoints: ReturnType<typeof typeGraphCheckpoints>;
}>;

export class AgentMaterializer extends DurableObject<Env> {
  readonly #agentId: string;
  readonly #ready: Promise<Ready>;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // The DO's own id IS the agent id — `worker.ts` routes with
    // `idFromName(agentId)`, so this is that same string coming back.
    this.#agentId = ctx.id.name ?? ctx.id.toString();
    // Schema provisioning does real I/O (DDL, a schema-version commit), so it
    // has to run before any request is served — `blockConcurrencyWhile`
    // queues incoming RPCs until this resolves, which is exactly the
    // "provision once at cold start" behavior a DO constructor wants.
    this.#ready = ctx.blockConcurrencyWhile(async () => {
      const backend = durableObjectSqliteBackend(ctx.storage);
      // One physical backend hosts BOTH graphs — `beliefGraph`'s tables and
      // `checkpointGraph`'s are distinguished by TypeGraph's own `graph_id`
      // column, not by separate connections. A Durable Object gets exactly
      // one SQLite database, so unlike the README's quick start (two
      // `createLocalSqliteBackend()` calls, two files) there is no second
      // connection to hand the cursor store — this is the pattern for
      // co-hosting them on one.
      const [belief] = await createAdapterStoreWithSchema(beliefGraph, backend, {
        history: true,
        coalesceUnchangedUpserts: true,
      });
      const [cursor] = await createAdapterStoreWithSchema(checkpointGraph, backend);
      return { belief, checkpoints: typeGraphCheckpoints(cursor) };
    });
  }

  /**
   * Apply one durable batch of events. Idempotent under retry: `consume()`
   * resumes from this stream's checkpoint, so a batch whose `seq` is at or
   * behind the durable cursor reads as empty and processes nothing — a
   * network-retried POST is a true no-op, not a re-application.
   */
  async ingest(batch: EventBatch): Promise<ConsumeResult> {
    const { belief, checkpoints } = await this.#ready;
    const change: ShapeChange<EventBatch["events"]> = {
      offset: String(batch.seq),
      shape: "agent-events",
      key: `${this.#agentId}:${batch.seq}`,
      operation: "insert",
      value: batch.events,
    };
    const source = mockShapeSource(this.#agentId, [change]);
    const project = graphProjector<BeliefGraph, EventBatch["events"]>(beliefGraph, (c) => c.value);
    return consume({ source, store: belief, checkpoints, project });
  }

  /** Read-only snapshot for the Worker's `GET /agents/:agentId` route. */
  async snapshot(): Promise<AgentSnapshot> {
    const { belief } = await this.#ready;
    return { agentId: this.#agentId, entityCount: await belief.nodes.Entity.count() };
  }
}
