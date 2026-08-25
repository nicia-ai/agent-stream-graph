/**
 * Live adapter for Electric's Deep Survey example.
 *
 * Run Electric's Deep Survey app first, then:
 *
 *   DARIX_URL=http://localhost:4437 DEEP_SURVEY_SWARM_ID=<swarm-id> pnpm demo:deep-survey-live
 *
 * The script observes `wiki-swarm-${DEEP_SURVEY_SWARM_ID}`, reads its `wiki`
 * and `xrefs` collections, and runs the same TypeGraph convergence projection
 * as the deterministic demo.
 */
import { z } from "zod";

import { runDeepSurveyConvergence, type WikiEntry, type Xref } from "./deep-survey-convergence";
import { runAsMain } from "./_support";

type RuntimeClientModule = Readonly<{
  createAgentsClient: (options: { baseUrl: string }) => {
    observe: (target: unknown) => Promise<ObservedDb>;
  };
  db: (id: string, schema: unknown) => unknown;
}>;

type CollectionLike<T> = Readonly<{
  values?: () => Iterable<T>;
  toArray?: readonly T[] | (() => readonly T[]);
}>;

type ObservedDb = Readonly<{
  preload?: () => Promise<void>;
  close?: () => void;
  collections?: Record<string, CollectionLike<unknown>>;
}> &
  Readonly<Record<string, unknown>>;

type LiveWikiRow = Readonly<{
  key: string;
  title: string;
  body: string;
  author: string;
  improved?: boolean;
}>;

type LiveXrefRow = Readonly<{
  key: string;
  a: string;
  b: string;
}>;

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

const swarmSharedSchema = {
  wiki: {
    schema: liveWikiSchema,
    type: "shared:wiki_entry",
    primaryKey: "key",
  },
  xrefs: {
    schema: liveXrefSchema,
    type: "shared:xref",
    primaryKey: "key",
  },
} as const;

async function loadAgentsRuntimeClient(): Promise<RuntimeClientModule> {
  try {
    // The Electric Agents runtime client is a dev-only dependency (absent in a
    // consumer install). The dynamic import keeps it out of the hot path; the
    // cast narrows to the slice this adapter needs.
    return (await import("@electric-ax/agents-runtime/client")) as unknown as RuntimeClientModule;
  } catch (error) {
    throw new Error(
      "Could not load @electric-ax/agents-runtime/client. Run this script from Electric's Deep Survey environment " +
        "or install the Electric Agents runtime package before using the live adapter. Original error: " +
        (error instanceof Error ? error.message : String(error)),
    );
  }
}

function env(name: string): string | undefined {
  const value = process.env[name]?.trim();
  return value === "" ? undefined : value;
}

function envNumber(name: string, fallback: number): number {
  const value = env(name);
  if (value === undefined) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

async function assertRuntimeReachable(darixUrl: string, timeoutMs: number): Promise<void> {
  try {
    await fetch(darixUrl, { signal: AbortSignal.timeout(timeoutMs) });
  } catch (error) {
    throw new Error(
      `Could not reach Electric Agents server at ${darixUrl} within ${timeoutMs}ms. ` +
        "Start it with Electric's quickstart before running the live demo. Original error: " +
        (error instanceof Error ? error.message : String(error)),
    );
  }
}

function withTimeout<T>(description: string, timeoutMs: number, work: Promise<T>): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  const timer = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => {
      reject(new Error(`${description} timed out after ${timeoutMs}ms.`));
    }, timeoutMs);
  });
  return Promise.race([work, timer]).finally(() => {
    if (timeout !== undefined) clearTimeout(timeout);
  });
}

function swarmIdFromEnvOrArg(): string | undefined {
  const swarmId = env("DEEP_SURVEY_SWARM_ID") ?? process.argv[2]?.trim();
  return swarmId === "" ? undefined : swarmId;
}

function sharedStateTarget(): { swarmId: string | undefined; sharedStateId: string } {
  const explicit = env("DEEP_SURVEY_SHARED_STATE_ID");
  const swarmId = swarmIdFromEnvOrArg();
  if (explicit !== undefined) {
    return { swarmId, sharedStateId: explicit };
  }
  if (swarmId === undefined) {
    throw new Error(
      "Missing swarm id. Set DEEP_SURVEY_SWARM_ID=<swarm-id>, pass it as the first argument, " +
        "or set DEEP_SURVEY_SHARED_STATE_ID=<shared-state-id>. " +
        "Deep Survey uses shared state id wiki-swarm-<swarm-id>.",
    );
  }
  return { swarmId, sharedStateId: `wiki-swarm-${swarmId}` };
}

function readCollection<T>(db: ObservedDb, name: string): readonly T[] {
  const collection = (db.collections?.[name] ?? db[name]) as CollectionLike<T> | undefined;
  if (collection === undefined) {
    throw new Error(`Observed shared DB did not expose a "${name}" collection.`);
  }

  if (typeof collection.values === "function") {
    return Array.from(collection.values());
  }
  if (typeof collection.toArray === "function") {
    return collection.toArray();
  }
  if (Array.isArray(collection.toArray)) {
    return collection.toArray;
  }
  throw new Error(`Collection "${name}" does not expose values() or toArray.`);
}

function normalizeWiki(rows: readonly LiveWikiRow[]): readonly WikiEntry[] {
  return rows
    .map((row, index) => {
      const parsed = liveWikiSchema.parse(row);
      return {
        ...parsed,
        offset: `live-wiki-${String(index + 1).padStart(3, "0")}`,
      };
    })
    .sort((left, right) => left.key.localeCompare(right.key));
}

function normalizeXrefs(rows: readonly LiveXrefRow[]): readonly Xref[] {
  return rows
    .map((row, index) => {
      const parsed = liveXrefSchema.parse(row);
      return {
        ...parsed,
        offset: `live-xref-${String(index + 1).padStart(3, "0")}`,
      };
    })
    .sort((left, right) => left.key.localeCompare(right.key));
}

export async function main(): Promise<void> {
  const runtime = await loadAgentsRuntimeClient();
  const darixUrl = env("DARIX_URL") ?? "http://localhost:4437";
  const { sharedStateId } = sharedStateTarget();
  const timeoutMs = envNumber("DEEP_SURVEY_LIVE_TIMEOUT_MS", 10_000);

  console.log(`Connecting to ${darixUrl}`);
  console.log(`Reading Deep Survey shared state ${sharedStateId}`);

  await assertRuntimeReachable(darixUrl, timeoutMs);

  const client = runtime.createAgentsClient({ baseUrl: darixUrl });
  let observed: ObservedDb;
  try {
    observed = await withTimeout(
      `Observing shared state "${sharedStateId}"`,
      timeoutMs,
      client.observe(runtime.db(sharedStateId, swarmSharedSchema)),
    );
    await withTimeout(`Preloading shared state "${sharedStateId}"`, timeoutMs, observed.preload?.() ?? Promise.resolve());
  } catch (error) {
    throw new Error(
      `Could not observe shared state "${sharedStateId}" at ${darixUrl}. ` +
        "Make sure Electric Agents quickstart and the Deep Survey entity server are running, " +
        "and that the swarm has created its shared state. Original error: " +
        (error instanceof Error ? error.message : String(error)),
    );
  }
  try {
    const wiki = normalizeWiki(readCollection<LiveWikiRow>(observed, "wiki"));
    const xrefs = normalizeXrefs(readCollection<LiveXrefRow>(observed, "xrefs"));

    if (wiki.length === 0) {
      throw new Error(`Shared state ${sharedStateId} has no wiki rows yet. Wait for explorers to write entries, then rerun.`);
    }

    await runDeepSurveyConvergence({
      title: `Live Deep Survey convergence: ${sharedStateId} -> canonical knowledge graph`,
      wiki,
      xrefs,
    });
  } finally {
    observed.close?.();
  }
}

runAsMain(import.meta.url, main);
