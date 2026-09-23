/**
 * The child-process half of `examples/crash-resume.ts` — read that file first
 * for the narrative. This module is dual-purpose:
 *
 *   - Run directly as the worker that gets SIGKILLed: drains most of the
 *     stream normally, then commits ONE more change straight to the belief
 *     store WITHOUT checkpointing it — the worst-case at-least-once crash
 *     window — reports that to the parent over stdout, and parks until killed.
 *   - Imported by `crash-resume.ts` for the shared graph, stream, and
 *     projector, so the crashed run and the clean comparison run consume
 *     identical input by construction.
 */
import { asNodeId, createAdapterStoreWithSchema, defineGraph, defineNode, recordedInstantRevision } from "@nicia-ai/typegraph";
import { createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";
import { z } from "zod";

import { checkpointGraph, consume, mockShapeSource, typeGraphCheckpoints, type Projector, type ShapeChange } from "../src";
import { type DemoHistoryStore, runAsMain } from "./_support";

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
type ObservationStore = DemoHistoryStore<typeof observationGraph>;
type ObservationRow = Readonly<{ tool: string; detail: string }>;

export const STREAM_NAME = "agent-observations";

// An agent's own tool-call log. Each change carries its own resumable offset (a
// per-message source, not an Electric-style shared-offset batch), so every
// change is its own checkpoint boundary.
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

export const CHANGES: readonly ShapeChange<ObservationRow>[] = OBSERVATIONS.map((value, index) => ({
  offset: String(index + 1).padStart(3, "0"),
  shape: "observation",
  key: `o${String(index + 1).padStart(2, "0")}`,
  operation: "insert",
  value,
}));

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

/** The belief's current recorded revision. Every caller runs after at least one commit. */
export async function currentRevision(belief: ObservationStore): Promise<number> {
  const anchor = await belief.recordedNow();
  if (anchor === undefined) throw new Error("crash-resume: the belief store has no recorded history yet");
  return recordedInstantRevision(anchor);
}

/**
 * Open the file-backed belief + checkpoint stores at the given paths. The
 * parent's post-kill reopen goes through this same function — resuming is not
 * a special code path, just the same open again.
 */
export async function openFileBackedStores(beliefDbPath: string, checkpointDbPath: string) {
  const [belief] = await createAdapterStoreWithSchema(
    observationGraph,
    createLocalSqliteBackend({ path: beliefDbPath }).backend,
    { history: true, coalesceUnchangedUpserts: true },
  );
  const [cursor] = await createAdapterStoreWithSchema(checkpointGraph, createLocalSqliteBackend({ path: checkpointDbPath }).backend);
  return { belief, cursor, book: typeGraphCheckpoints(cursor) };
}

// ============================================================
// Wire protocol with the parent — one JSON object per stdout line. The worker
// reports facts; the parent narrates them.
// ============================================================

export type WorkerMessage =
  | Readonly<{ type: "drained"; processed: number; checkpoint: string | undefined }>
  | Readonly<{ type: "primed"; offset: string; checkpoint: string | undefined; revision: number }>;

function send(message: WorkerMessage): Promise<void> {
  return new Promise((resolve, reject) => {
    process.stdout.write(`${JSON.stringify(message)}\n`, (error) => (error ? reject(error) : resolve()));
  });
}

// Far longer than the parent needs to deliver the SIGKILL, but finite, so a
// worker orphaned by a parent crash cannot outlive it indefinitely.
const PARK_TIMEOUT_MS = 30_000;

// ============================================================
// Worker entry point
// ============================================================

async function main(): Promise<void> {
  const [beliefDbPath, checkpointDbPath] = process.argv.slice(2);
  if (beliefDbPath === undefined || checkpointDbPath === undefined) {
    throw new Error("usage: node --import tsx crash-resume-worker.ts <belief-db-path> <checkpoint-db-path>");
  }
  const primeChange = CHANGES[PRIME_INDEX];
  if (primeChange === undefined) {
    throw new RangeError(`PRIME_INDEX ${PRIME_INDEX} is out of range for a ${CHANGES.length}-change stream`);
  }

  const { belief, book } = await openFileBackedStores(beliefDbPath, checkpointDbPath);
  const source = mockShapeSource(STREAM_NAME, CHANGES);

  // Belief and durable cursor advance in lockstep, exactly as an unattended
  // materializer would.
  const drained = await consume({ source, store: belief, checkpoints: book, project, stopAfter: PRIME_INDEX });
  await send({ type: "drained", processed: drained.processed, checkpoint: await book.lastOffset(STREAM_NAME) });

  // Bypass consume() for one change: it commits to belief, but the durable
  // cursor never moves past it, so on restart the source re-delivers it.
  await belief.transaction((tx) => project(tx, primeChange));
  await send({
    type: "primed",
    offset: primeChange.offset,
    checkpoint: await book.lastOffset(STREAM_NAME),
    revision: await currentRevision(belief),
  });

  // Park until the parent's SIGKILL. A bare pending promise would not do: with
  // nothing left on the event loop Node exits cleanly — running better-sqlite3's
  // close hooks — which is exactly the graceful shutdown this demo rules out.
  // The timer is what keeps the process alive to be killed.
  await new Promise((resolve) => setTimeout(resolve, PARK_TIMEOUT_MS));
  throw new Error(`crash-resume-worker: parked ${PARK_TIMEOUT_MS}ms without being killed — is the parent gone?`);
}

runAsMain(import.meta.url, main);
