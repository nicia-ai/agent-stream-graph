/**
 * Live adapter for Electric's Deep Survey example.
 *
 * Run Electric's Deep Survey app first (the README has the steps), then:
 *
 *   DARIX_URL=http://localhost:4437 DEEP_SURVEY_SWARM_ID=<swarm-id> pnpm demo:deep-survey-live
 *
 * It observes the swarm's shared state, reads its `wiki` and `xrefs`
 * collections, and runs them through the same convergence as the offline
 * flagship demo (`pnpm demo`). It needs a running swarm, so `pnpm demo:all`
 * does not run it.
 *
 * Configuration, from the environment:
 *
 *   DEEP_SURVEY_SWARM_ID          orchestrator entity id; may also be the first argument
 *   DEEP_SURVEY_SHARED_STATE_ID   shared-state id, used instead of `wiki-swarm-<swarm-id>`
 *   DARIX_URL                     Electric Agents server (default http://localhost:4437)
 *   DEEP_SURVEY_LIVE_TIMEOUT_MS   timeout per network step (default 10000)
 */
import { z } from "zod";

import { runDeepSurveyConvergence, type WikiEntry, type Xref } from "./deep-survey-convergence";
import { runAsMain } from "./_support";

type AgentsRuntimeClient = typeof import("@electric-ax/agents-runtime/client");
type ObservedSharedState = Awaited<ReturnType<ReturnType<AgentsRuntimeClient["createAgentsClient"]>["observe"]>>;

type LiveConfig = Readonly<{ darixUrl: string; sharedStateId: string; timeoutMs: number }>;

const DEFAULT_DARIX_URL = "http://localhost:4437";
const DEFAULT_TIMEOUT_MS = 10_000;
const SHARED_STATE_ID_PREFIX = "wiki-swarm-";

const liveWikiSchema = z.object({
  key: z.string().min(1),
  title: z.string().min(1),
  body: z.string().min(1),
  author: z.string().min(1),
  improved: z.boolean().default(false),
});

const liveXrefSchema = z.object({
  key: z.string().min(1),
  a: z.string().min(1),
  b: z.string().min(1),
});

/** Deep Survey's shared-state schema; `type` and `primaryKey` must match what the swarm writes. */
const swarmSharedSchema = {
  wiki: { schema: liveWikiSchema, type: "shared:wiki_entry", primaryKey: "key" },
  xrefs: { schema: liveXrefSchema, type: "shared:xref", primaryKey: "key" },
} as const;

export async function main(): Promise<void> {
  const config = readConfig();
  console.log(`Connecting to ${config.darixUrl}`);
  console.log(`Reading Deep Survey shared state ${config.sharedStateId}`);

  const runtime = await loadAgentsRuntimeClient();
  await assertRuntimeReachable(config);
  const observed = await observeSharedState(runtime, config);
  try {
    const wiki: readonly WikiEntry[] = toStreamRows(readCollection(observed, "wiki"), liveWikiSchema, 0);
    const xrefs: readonly Xref[] = toStreamRows(readCollection(observed, "xrefs"), liveXrefSchema, wiki.length);
    if (wiki.length === 0) {
      throw new Error(
        `Shared state ${config.sharedStateId} has no wiki rows yet. Wait for explorers to write entries, then rerun.`,
      );
    }

    await runDeepSurveyConvergence({
      title: `Live Deep Survey convergence: ${config.sharedStateId} -> canonical knowledge graph`,
      wiki,
      xrefs,
    });
  } finally {
    observed.close();
  }
}

// ============================================================
// Configuration
// ============================================================

function env(name: string): string | undefined {
  const value = process.env[name]?.trim();
  return value === "" ? undefined : value;
}

function positiveIntegerEnv(name: string, fallback: number): number {
  const value = env(name);
  if (value === undefined) return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new RangeError(`${name} must be a positive integer, received "${value}".`);
  }
  return parsed;
}

function sharedStateIdFromEnvOrArg(): string {
  const explicit = env("DEEP_SURVEY_SHARED_STATE_ID");
  if (explicit !== undefined) return explicit;

  const swarmId = env("DEEP_SURVEY_SWARM_ID") ?? process.argv[2]?.trim();
  if (swarmId === undefined || swarmId === "") {
    throw new Error(
      "Missing swarm id. Set DEEP_SURVEY_SWARM_ID=<swarm-id>, pass it as the first argument, " +
        `or set DEEP_SURVEY_SHARED_STATE_ID=<shared-state-id>. Deep Survey uses ${SHARED_STATE_ID_PREFIX}<swarm-id>.`,
    );
  }
  return `${SHARED_STATE_ID_PREFIX}${swarmId}`;
}

function readConfig(): LiveConfig {
  return {
    darixUrl: env("DARIX_URL") ?? DEFAULT_DARIX_URL,
    sharedStateId: sharedStateIdFromEnvOrArg(),
    timeoutMs: positiveIntegerEnv("DEEP_SURVEY_LIVE_TIMEOUT_MS", DEFAULT_TIMEOUT_MS),
  };
}

// ============================================================
// Talking to the Electric Agents runtime
// ============================================================

// Loaded dynamically so a missing runtime fails with setup instructions rather
// than a bare module-resolution error at startup.
async function loadAgentsRuntimeClient(): Promise<AgentsRuntimeClient> {
  try {
    return await import("@electric-ax/agents-runtime/client");
  } catch (error) {
    throw new Error(
      "Could not load @electric-ax/agents-runtime/client. Run this script from Electric's Deep Survey environment " +
        "or install the Electric Agents runtime package before using the live adapter.",
      { cause: error },
    );
  }
}

async function assertRuntimeReachable({ darixUrl, timeoutMs }: LiveConfig): Promise<void> {
  try {
    await fetch(darixUrl, { signal: AbortSignal.timeout(timeoutMs) });
  } catch (error) {
    throw new Error(
      `Could not reach Electric Agents server at ${darixUrl} within ${timeoutMs}ms. ` +
        "Start it with Electric's quickstart before running the live demo.",
      { cause: error },
    );
  }
}

function withTimeout<T>(description: string, timeoutMs: number, work: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${description} timed out after ${timeoutMs}ms.`)), timeoutMs);
  });
  return Promise.race([work, timeout]).finally(() => clearTimeout(timer));
}

/** Observe and fully preload the shared state; the caller owns closing it. */
async function observeSharedState(runtime: AgentsRuntimeClient, config: LiveConfig): Promise<ObservedSharedState> {
  const { darixUrl, sharedStateId, timeoutMs } = config;
  try {
    const client = runtime.createAgentsClient({ baseUrl: darixUrl });
    const observed = await withTimeout(
      `Observing shared state "${sharedStateId}"`,
      timeoutMs,
      client.observe(runtime.db(sharedStateId, swarmSharedSchema)),
    );
    try {
      await withTimeout(`Preloading shared state "${sharedStateId}"`, timeoutMs, observed.preload());
    } catch (error) {
      observed.close();
      throw error;
    }
    return observed;
  } catch (error) {
    throw new Error(
      `Could not observe shared state "${sharedStateId}" at ${darixUrl}. Make sure Electric Agents quickstart ` +
        "and the Deep Survey entity server are running, and that the swarm has created its shared state.",
      { cause: error },
    );
  }
}

function readCollection(observed: ObservedSharedState, name: keyof typeof swarmSharedSchema): readonly unknown[] {
  const collections: Readonly<Record<string, { values(): Iterable<unknown> } | undefined>> = observed.collections;
  const collection = collections[name];
  if (collection === undefined) {
    throw new Error(`Observed shared state did not expose a "${name}" collection.`);
  }
  return Array.from(collection.values());
}

// Collection iteration order is not a stream order, so rows are sorted by key
// and given synthetic ascending offsets: the convergence consumes them as a
// stream and replays the belief by offset. Wiki rows and xrefs share one
// sequence, xrefs numbered after `offsetsBefore` wiki rows, so an xref always
// follows the pages it joins.
function toStreamRows<Row extends Readonly<{ key: string }>>(
  rows: readonly unknown[],
  schema: z.ZodType<Row>,
  offsetsBefore: number,
): readonly (Row & Readonly<{ offset: string }>)[] {
  return rows
    .map((row) => schema.parse(row))
    .sort((left, right) => left.key.localeCompare(right.key))
    .map((row, index) => ({ ...row, offset: String(offsetsBefore + index + 1) }));
}

runAsMain(import.meta.url, main);
