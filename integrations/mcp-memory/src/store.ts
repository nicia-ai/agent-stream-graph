/**
 * Opens the memory graph: a file-backed, history-enabled TypeGraph store
 * plus the durable checkpoint book that tracks each observation stream's
 * offset within it. Memory persists across process restarts because the
 * SQLite files do; a fresh database is seeded from `fixtures.ts` exactly
 * once, gated on the checkpoint book already covering the seed streams
 * rather than on file presence, so re-opening a partially-seeded store
 * (e.g. after a crash mid-seed) finishes the job instead of skipping it.
 */
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { checkpointGraph, consume, mockShapeSource, typeGraphCheckpoints, type CheckpointBook, type Projector } from "@nicia-ai/agent-stream-graph";
import { asNodeId, createStoreWithSchema, type HistoryStore } from "@nicia-ai/typegraph";
import { createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";

import {
  BACKGROUND_SCAN_SOURCE,
  CRM_CHANGES,
  ID_CHECK_SOURCE,
  LINKEDIN_CHANGES,
  STREAM_CRM,
  STREAM_LINKEDIN,
  STREAM_VERIFICATION,
  VERIFICATION_CHANGES,
  type ObservationValue,
  type VerificationValue,
} from "./fixtures.js";
import { factId, justificationId, mergeAliases, memoryGraph, orgId, personId, VERIFIED_PREDICATE, type MemoryGraph } from "./graph.js";

const PACKAGE_ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

/** Default data directory for a real (non-demo, non-test) server process. */
export const DEFAULT_DATA_DIR = join(PACKAGE_ROOT, ".data", "server");

export type MemoryStore = HistoryStore<MemoryGraph>;

export type OpenMemoryStoreResult = Readonly<{
  store: MemoryStore;
  book: CheckpointBook;
  /** Whether this call seeded a fresh store — surfaced so a caller (mainly
   * `demo.ts`) can narrate first-run vs. warm-restart behavior honestly. */
  seeded: boolean;
  close: () => Promise<void>;
}>;

/** Exported so tests can `consume()` additional observations beyond the
 * fixtures (e.g. to seed enough `Person` rows to exercise pagination) through
 * the same write path a real stream would use, rather than writing to the
 * store directly. */
export const projectObservation: Projector<MemoryGraph, ObservationValue> = async (tx, change) => {
  const value = change.value;
  const id = personId(value.personEmail);
  const existing = await tx.nodes.Person.getById(asNodeId(id));
  await tx.nodes.Person.upsertById(id, {
    name: value.personName,
    email: value.personEmail,
    title: value.title ?? existing?.title ?? "",
    aliases: mergeAliases(existing?.aliases ?? [], [value.personName]),
  });

  if (value.orgDomain !== undefined && value.orgName !== undefined) {
    const oId = orgId(value.orgDomain);
    const existingOrg = await tx.nodes.Org.getById(asNodeId(oId));
    await tx.nodes.Org.upsertById(oId, {
      name: value.orgName,
      domain: value.orgDomain,
      aliases: mergeAliases(existingOrg?.aliases ?? [], [value.orgName]),
    });
    await tx.edges.worksAt.getOrCreateByEndpoints({ kind: "Person", id }, { kind: "Org", id: oId }, {});
  }
};

const projectVerification: Projector<MemoryGraph, VerificationValue> = async (tx, change) => {
  const value = change.value;
  const subject = personId(value.personEmail);
  const fId = factId(subject, VERIFIED_PREDICATE);
  const jId = justificationId(value.sourceId, fId);

  await tx.nodes.Source.upsertById(value.sourceId, { label: value.sourceLabel, retracted: false });
  await tx.nodes.Fact.upsertById(fId, { predicate: VERIFIED_PREDICATE, value: "true" });
  await tx.nodes.Justification.upsertById(jId, { rule: `${value.sourceLabel} verified identity` });
  await tx.edges.premiseOf.getOrCreateByEndpoints({ kind: "Source", id: value.sourceId }, { kind: "Justification", id: jId }, {});
  await tx.edges.derives.getOrCreateByEndpoints({ kind: "Justification", id: jId }, { kind: "Fact", id: fId }, {});
  await tx.edges.about.getOrCreateByEndpoints({ kind: "Fact", id: fId }, { kind: "Person", id: subject }, {});
};

async function seedIfNeeded(store: MemoryStore, book: CheckpointBook): Promise<boolean> {
  // The checkpoint book, not file presence, is the seeded/unseeded signal:
  // it is exactly the thing `consume()` itself resumes from, so "already
  // covers the seed streams" and "consume() would do nothing" are the same
  // question asked two ways. Gated on the LAST stream consumed below, so a
  // crash mid-seed is detected as unseeded and the whole sequence reruns
  // (every write here is an idempotent upsert, so replaying earlier streams
  // is harmless).
  const alreadySeeded = (await book.lastOffset(STREAM_VERIFICATION)) !== undefined;
  if (alreadySeeded) return false;

  // crm-import lands before linkedin-scrape's correction, so the two
  // sources' overlapping observations resolve the way the demo narrates:
  // one shared entity whose LIVE title is linkedin-scrape's corrected one.
  await consume({ source: mockShapeSource(STREAM_CRM, CRM_CHANGES), store, checkpoints: book, project: projectObservation });
  await consume({ source: mockShapeSource(STREAM_LINKEDIN, LINKEDIN_CHANGES), store, checkpoints: book, project: projectObservation });
  await consume({ source: mockShapeSource(STREAM_VERIFICATION, VERIFICATION_CHANGES), store, checkpoints: book, project: projectVerification });
  return true;
}

export type OpenMemoryStoreOptions = Readonly<{
  /** Directory to hold `memory.db` and `checkpoints.db`. Defaults to a
   * directory inside this package, so a real server run persists without
   * any configuration. */
  dataDir?: string;
}>;

export async function openMemoryStore(options: OpenMemoryStoreOptions = {}): Promise<OpenMemoryStoreResult> {
  const dataDir = options.dataDir ?? DEFAULT_DATA_DIR;
  mkdirSync(dataDir, { recursive: true });

  const { backend: memoryBackend } = createLocalSqliteBackend({ path: join(dataDir, "memory.db") });
  const { backend: checkpointBackend } = createLocalSqliteBackend({ path: join(dataDir, "checkpoints.db") });

  const [store] = await createStoreWithSchema(memoryGraph, memoryBackend, {
    history: true,
    coalesceUnchangedUpserts: true,
  });
  const [cursor] = await createStoreWithSchema(checkpointGraph, checkpointBackend);
  const book = typeGraphCheckpoints(cursor);

  const seeded = await seedIfNeeded(store, book);

  return {
    store,
    book,
    seeded,
    close: async () => {
      await Promise.allSettled([store.close(), cursor.close()]);
    },
  };
}
