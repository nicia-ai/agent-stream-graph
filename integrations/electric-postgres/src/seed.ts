/**
 * Writes a realistic `agent_events` stream into Postgres OVER TIME — inserts
 * spaced out with real delays, not one bulk `INSERT ... VALUES (...), (...)`.
 *
 * That is the whole point of this file rather than a fixture array: a single
 * bulk insert would land as one Postgres transaction, and Electric would
 * therefore always show it as one snapshot batch — a demo built on that could
 * never show `demo.ts`'s live-tailing story, only its catch-up story. Real
 * agents don't work that way either: they claim a task, work it for a while,
 * and report back — this mirrors that.
 *
 * Run standalone:      pnpm tsx src/seed.ts
 * Or import `SCRIPT`/`insertEvent`/`seedOverTime` from `demo.ts` to interleave
 * seeding with live consumption in one process (which is what `demo.ts` does).
 */
import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { DEFAULT_DATABASE_URL } from "./db.js";

/** One `agent_events` row's writable columns — everything but the server-assigned `id`/`occurred_at`. */
export type SeedEvent = Readonly<{
  agentId: string;
  agentName: string;
  taskId: string;
  taskTitle: string;
  eventType: "task_claimed" | "task_progress" | "task_completed" | "task_failed" | "finding_recorded";
  status: string;
  findingId?: string;
  findingSummary?: string;
  severity?: string;
}>;

/**
 * Three agents working four tasks, in the order a real fleet would produce
 * them: claim, progress, (sometimes a finding), then completed or failed.
 * Two events deliberately land close enough together (see `DELAYS_MS`) to
 * share one Electric batch — that pairing is what `demo.ts` points at to
 * prove per-batch checkpoint granularity concretely rather than by assertion.
 */
export const SCRIPT: readonly SeedEvent[] = [
  { agentId: "agent-ada", agentName: "Ada", taskId: "t-auth-audit", taskTitle: "Audit the auth flow for token leakage", eventType: "task_claimed", status: "in_progress" },
  { agentId: "agent-grace", agentName: "Grace", taskId: "t-index-fix", taskTitle: "Fix the missing index on agent_events.task_id", eventType: "task_claimed", status: "in_progress" },
  { agentId: "agent-ada", agentName: "Ada", taskId: "t-auth-audit", taskTitle: "Audit the auth flow for token leakage", eventType: "task_progress", status: "in_progress" },
  {
    agentId: "agent-ada",
    agentName: "Ada",
    taskId: "t-auth-audit",
    taskTitle: "Audit the auth flow for token leakage",
    eventType: "finding_recorded",
    status: "in_progress",
    findingId: "f-token-in-log",
    findingSummary: "Refresh tokens are written to the request logger at info level",
    severity: "high",
  },
  // These two share a moment on purpose — see DELAYS_MS[4].
  { agentId: "agent-grace", agentName: "Grace", taskId: "t-index-fix", taskTitle: "Fix the missing index on agent_events.task_id", eventType: "task_completed", status: "done" },
  { agentId: "agent-hopper", agentName: "Hopper", taskId: "t-retry-storm", taskTitle: "Diagnose the retry storm on the ingest queue", eventType: "task_claimed", status: "in_progress" },
  { agentId: "agent-ada", agentName: "Ada", taskId: "t-auth-audit", taskTitle: "Audit the auth flow for token leakage", eventType: "task_completed", status: "done" },
  { agentId: "agent-hopper", agentName: "Hopper", taskId: "t-retry-storm", taskTitle: "Diagnose the retry storm on the ingest queue", eventType: "task_progress", status: "in_progress" },
  {
    agentId: "agent-hopper",
    agentName: "Hopper",
    taskId: "t-retry-storm",
    taskTitle: "Diagnose the retry storm on the ingest queue",
    eventType: "finding_recorded",
    status: "in_progress",
    findingId: "f-no-backoff",
    findingSummary: "The consumer retries immediately with no backoff, amplifying the outage",
    severity: "critical",
  },
  { agentId: "agent-grace", agentName: "Grace", taskId: "t-schema-migration", taskTitle: "Add graph_id index to the checkpoint tables", eventType: "task_claimed", status: "in_progress" },
  { agentId: "agent-hopper", agentName: "Hopper", taskId: "t-retry-storm", taskTitle: "Diagnose the retry storm on the ingest queue", eventType: "task_failed", status: "blocked" },
  { agentId: "agent-grace", agentName: "Grace", taskId: "t-schema-migration", taskTitle: "Add graph_id index to the checkpoint tables", eventType: "task_completed", status: "done" },
];

/**
 * Delay in ms BEFORE `SCRIPT[i]`, for `i > 0`. Indices 3-4 are 0 — those two
 * land in the same Postgres commit-visibility window, so with any reasonable
 * poll cadence they land in the SAME Electric batch and share one offset;
 * everything else is spaced far enough apart to land in its own batch. Kept
 * short (demo, not a slow-motion simulation) but non-zero everywhere else so
 * `pnpm demo`'s live-tailing section has real separate batches to show.
 */
const DELAYS_MS: readonly number[] = [0, 500, 500, 400, 0, 600, 500, 500, 500, 600, 500, 500];

/** Shared with `demo.ts`, which polls on the same primitive while tailing live. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** One parameterized `INSERT INTO agent_events`, matching `sql/schema.sql` exactly. */
export async function insertEvent(pool: Pool, event: SeedEvent): Promise<void> {
  await pool.query(
    `INSERT INTO agent_events (agent_id, agent_name, task_id, task_title, event_type, status, finding_id, finding_summary, severity)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
    [
      event.agentId,
      event.agentName,
      event.taskId,
      event.taskTitle,
      event.eventType,
      event.status,
      event.findingId ?? null,
      event.findingSummary ?? null,
      event.severity ?? null,
    ],
  );
}

/**
 * Insert `events` in order, waiting `delaysMs[i]` before the i-th (0 for the
 * first). Calls `onInserted` after each write, so a caller running this
 * concurrently with live consumption (`demo.ts`) can narrate as it goes.
 */
export async function seedOverTime(
  pool: Pool,
  events: readonly SeedEvent[],
  delaysMs: readonly number[],
  onInserted?: (event: SeedEvent, index: number) => void,
): Promise<void> {
  for (const [index, event] of events.entries()) {
    const delay = delaysMs[index] ?? 0;
    if (delay > 0) await sleep(delay);
    await insertEvent(pool, event);
    onInserted?.(event, index);
  }
}

async function main(): Promise<void> {
  const rule = "─".repeat(74);
  console.log(rule);
  console.log(` Seeding ${SCRIPT.length} agent_events rows over time into agent_fleet`);
  console.log(rule);

  const pool = new Pool({ connectionString: DEFAULT_DATABASE_URL });
  try {
    await seedOverTime(pool, SCRIPT, DELAYS_MS, (event, index) => {
      const label = event.eventType === "finding_recorded" ? `finding "${event.findingId}"` : `${event.taskId} -> ${event.status}`;
      console.log(`  [${String(index + 1).padStart(2, "0")}/${SCRIPT.length}] ${event.agentName.padEnd(7)} ${event.eventType.padEnd(16)} ${label}`);
    });
    console.log(`\n  done — ${SCRIPT.length} events committed to agent_events, each its own transaction.`);
  } finally {
    await pool.end();
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exit(1);
  });
}
