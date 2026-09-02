/**
 * The child-process half of `examples/crash-resume.ts` — read that file first
 * for the narrative. This module is dual-purpose:
 *
 *   - Run directly (through tsx, via `runAsMain`) as the worker that gets
 *     SIGKILLed: drains most of the stream normally, then manually commits ONE
 *     more change straight to the belief store WITHOUT checkpointing it — the
 *     worst-case at-least-once crash window — signals the parent over a stdout
 *     JSON line, and hangs, waiting to die.
 *   - Imported by `crash-resume.ts` for the shared graph, stream, and
 *     projector, so the crashed run and the clean comparison run consume
 *     byte-identical input by construction, with nothing to keep in sync by
 *     hand.
 */
import {
  type AdapterHistoryStore,
  asNodeId,
  createAdapterStoreWithSchema,
  defineGraph,
  defineNode,
  recordedInstantRevision,
} from "@nicia-ai/typegraph";
import { type AnySqliteDatabase, createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";
import { z } from "zod";

import { checkpointGraph, consume, mockShapeSource, typeGraphCheckpoints, type Projector, type ShapeChange } from "../src";
import { runAsMain } from "./_support";

// ============================================================
// The shared belief graph, stream, and projector
// ============================================================

const Observation = defineNode("Observation", {
  schema: z.object({ tool: z.string(), detail: z.string() }),
});
export const observationGraph = defineGraph({
  id: "crash_resume_observations",
  nodes: { Observation: { type: Observation } },
  edges: {},
});
export type ObservationStore = AdapterHistoryStore<typeof observationGraph, AnySqliteDatabase>;
export type ObservationRow = Readonly<{ tool: string; detail: string }>;

export const STREAM_NAME = "agent-observations";

// An agent's own tool-call log — 14 observations, each carrying its own
// resumable offset (a real per-message source, not an Electric-style
// shared-offset batch), so every change is its own checkpoint boundary.
const OBSERVATIONS: readonly ObservationRow[] = [
  { tool: "search", detail: "queried docs for 'rate limit'" },
  { tool: "fetch", detail: "GET /v1/limits" },
  { tool: "search", detail: "queried docs for 'retry policy'" },
  { tool: "code", detail: "wrote backoff() helper" },
  { tool: "test", detail: "ran backoff.test.ts" },
  { tool: "search", detail: "queried docs for 'circuit breaker'" },
  { tool: "code", detail: "wrote circuitBreaker() helper" },
  { tool: "test", detail: "ran circuitBreaker.test.ts" },
  { tool: "review", detail: "self-reviewed the diff" },
  { tool: "fetch", detail: "GET /v1/status" },
  { tool: "code", detail: "wired circuitBreaker into the client" },
  { tool: "test", detail: "ran the client integration suite" },
  { tool: "commit", detail: "committed 'add resilience helpers'" },
  { tool: "report", detail: "posted summary to #eng" },
];

export const CHANGES: readonly ShapeChange<ObservationRow>[] = OBSERVATIONS.map(
  (value, index): ShapeChange<ObservationRow> => ({
    offset: String(index + 1).padStart(3, "0"),
    shape: "observation",
    key: `o${String(index + 1).padStart(2, "0")}`,
    operation: "insert",
    value,
  }),
);

/** Index of the change committed to belief but NEVER checkpointed before the kill. */
export const PRIME_INDEX = 8;

export const project: Projector<typeof observationGraph, ObservationRow> = async (tx, change) => {
  if (change.operation === "delete") {
    await tx.nodes.Observation.delete(asNodeId(change.key));
    return;
  }
  await tx.nodes.Observation.upsertById(change.key, { tool: change.value.tool, detail: change.value.detail });
};

type ObservationRowOut = Readonly<{ id: string; tool: string; detail: string }>;

/** Every Observation row, for the row-count and identical-graph checks. */
export async function rows(view: Pick<ObservationStore, "query">): Promise<readonly ObservationRowOut[]> {
  return view
    .query()
    .from("Observation", "o")
    .select((c) => ({ id: c.o.id, tool: c.o.tool, detail: c.o.detail }))
    .execute();
}

/** Sorted, serialized snapshot — the shape two independently-built stores are compared by. */
export function serializeRows(list: readonly ObservationRowOut[]): string {
  return JSON.stringify([...list].sort((left, right) => left.id.localeCompare(right.id)));
}

/**
 * Open the file-backed belief + checkpoint stores at the given paths. Shared
 * by the worker's own startup and by the parent's post-kill reopen of the
 * SAME files — the point of that reopen is that it is not a special code
 * path, just this same open again.
 */
export async function openFileBackedStores(beliefDbPath: string, checkpointDbPath: string) {
  const { backend: beliefBackend } = createLocalSqliteBackend({ path: beliefDbPath });
  const [belief] = await createAdapterStoreWithSchema(observationGraph, beliefBackend, {
    history: true,
    coalesceUnchangedUpserts: true,
  });
  const { backend: checkpointBackend } = createLocalSqliteBackend({ path: checkpointDbPath });
  const [cursor] = await createAdapterStoreWithSchema(checkpointGraph, checkpointBackend);
  return { belief, cursor, book: typeGraphCheckpoints(cursor) };
}

// ============================================================
// Wire protocol with the parent (examples/crash-resume.ts) — one JSON object
// per stdout line. Deliberately tiny: the worker reports facts, the parent
// narrates them, so both processes' output styles stay under one roof.
// ============================================================

export type WorkerMessage =
  | Readonly<{ type: "ready-normal"; processed: number; checkpoint: string | undefined }>
  | Readonly<{ type: "primed"; offset: string; checkpoint: string | undefined; revision: number }>;

function send(message: WorkerMessage): Promise<void> {
  return new Promise((resolve, reject) => {
    process.stdout.write(`${JSON.stringify(message)}\n`, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

// ============================================================
// Worker entry point
// ============================================================

async function main(): Promise<void> {
  const beliefDbPath = process.argv[2];
  const checkpointDbPath = process.argv[3];
  if (beliefDbPath === undefined || checkpointDbPath === undefined) {
    throw new Error("usage: tsx crash-resume-worker.ts <belief-db-path> <checkpoint-db-path>");
  }

  const { belief, book } = await openFileBackedStores(beliefDbPath, checkpointDbPath);
  const source = mockShapeSource(STREAM_NAME, CHANGES);

  // Drain everything up to (not including) the priming change, normally —
  // belief and durable cursor advance in lockstep, checkpointed at every
  // offset boundary, exactly as an unattended materializer would.
  const normal = await consume({ source, store: belief, checkpoints: book, project, stopAfter: PRIME_INDEX });
  await send({ type: "ready-normal", processed: normal.processed, checkpoint: await book.lastOffset(STREAM_NAME) });

  // Manually apply ONE more change directly to the belief store, bypassing
  // consume() entirely, so it commits to belief but the durable cursor never
  // moves past it. This is the worst-case at-least-once crash window: on
  // restart the source will re-deliver this exact change.
  const primeChange = CHANGES[PRIME_INDEX];
  if (primeChange === undefined) {
    throw new Error(`PRIME_INDEX ${PRIME_INDEX} is out of range for a ${CHANGES.length}-change stream`);
  }
  await belief.transaction((tx) => project(tx, primeChange));
  const anchor = await belief.recordedNow();
  await send({
    type: "primed",
    offset: primeChange.offset,
    checkpoint: await book.lastOffset(STREAM_NAME),
    revision: anchor === undefined ? -1 : recordedInstantRevision(anchor),
  });

  // Wait to be SIGKILLed by the parent. No signal handler, no cleanup, no
  // `finally` — that absence is the point: a real crash gets no chance to
  // flush or checkpoint anything further.
  await new Promise<never>(() => {});
}

runAsMain(import.meta.url, main);
