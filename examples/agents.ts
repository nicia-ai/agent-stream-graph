/**
 * Demo — the whole pipeline end to end: two agents' change streams → a durable,
 * resumable consumer → one bitemporal belief graph PER AGENT → a single
 * entity-resolved canonical graph.
 *
 *   (a) The raw stream timelines each agent emits.
 *   (b) Crash-safe consumption: each offset is checkpointed with the recorded
 *       instant it committed at, so a restart resumes from the cursor and
 *       at-least-once re-delivery converges instead of duplicating.
 *   (c) Per-agent time travel: what did THAT agent believe at a given offset?
 *   (d) Entity resolution: merge the beliefs into canonical — aliases collapse,
 *       disagreements are flagged, and provenance traces each canonical entity
 *       back to the stream offsets behind it.
 *
 * The transport is a `mockShapeSource`; `electricShapeSource` is the drop-in.
 * Run with:  pnpm demo:mechanics
 */
import { defineEdge, defineGraph, defineNode, searchable, type RecordedInstant, type Store } from "@nicia-ai/typegraph";
import {
  asBranchId,
  ingestionBranch,
  isOk,
  mergeIncremental,
  openProvenanceStore,
  type PropertyConflict,
  type ProvenanceGraph,
  readProvenance,
  unwrap,
} from "@nicia-ai/typegraph/graph-merge";
import { exportGraphStream, importGraphStream } from "@nicia-ai/typegraph/interchange";
import { z } from "zod";

import {
  type CheckpointBook,
  checkpointGraph,
  consume,
  type Decoder,
  graphProjector,
  mockShapeSource,
  typeGraphCheckpoints,
  type Projector,
  type ShapeChange,
} from "../src";
import { type DemoHistoryStore, type DemoStore, makeBackend, newStore, runAsMain, section } from "./_support";

// ============================================================
// The entity graph each agent materializes into
// ============================================================

const Person = defineNode("Person", {
  schema: z.object({
    name: searchable({ language: "english" }),
    email: z.string(),
    title: z.string(),
  }),
});
const Company = defineNode("Company", {
  schema: z.object({
    name: searchable({ language: "english" }),
    domain: z.string(),
    stage: z.string(),
  }),
});
const worksAt = defineEdge("worksAt", { schema: z.object({}) });

const intelGraph = defineGraph({
  id: "agent_intel",
  nodes: {
    Person: {
      type: Person,
      unique: [{ name: "person_email", fields: ["email"], scope: "kind", collation: "caseInsensitive" }],
    },
    Company: {
      type: Company,
      unique: [{ name: "company_domain", fields: ["domain"], scope: "kind", collation: "caseInsensitive" }],
    },
  },
  edges: { worksAt: { type: worksAt, from: [Person], to: [Company] } },
});
// Spans the non-history fork point and the history-enabled belief/canonical
// stores, so it names only what they share.
type IntelStore = DemoStore<typeof intelGraph>;
type IntelBelief = DemoHistoryStore<typeof intelGraph>;

// ============================================================
// Two agents observing the same world through different eyes
// ============================================================

/** The row shape these streams carry; typing it spares the decoder `as string` casts. */
type IntelRow = Readonly<{
  name?: string;
  email?: string;
  title?: string;
  domain?: string;
  stage?: string;
  person?: string;
  company?: string;
}>;

const SALES_CHANGES: readonly ShapeChange<IntelRow>[] = [
  { offset: "001", shape: "person", key: "p1", operation: "insert", value: { name: "Jane Doe", email: "jane@acme.com", title: "VP Engineering" } },
  { offset: "002", shape: "company", key: "c1", operation: "insert", value: { name: "Acme Corp", domain: "acme.com", stage: "Series A" } },
  { offset: "003", shape: "employment", key: "e1", operation: "insert", value: { person: "p1", company: "c1" } },
  { offset: "004", shape: "person", key: "p1", operation: "update", value: { name: "Jane Doe", email: "jane@acme.com", title: "VP Eng & Product" } },
];

const SUPPORT_CHANGES: readonly ShapeChange<IntelRow>[] = [
  { offset: "001", shape: "person", key: "x1", operation: "insert", value: { name: "J. Doe", email: "jane@acme.com", title: "VP Eng" } },
  { offset: "002", shape: "company", key: "y1", operation: "insert", value: { name: "Acme", domain: "acme.com", stage: "Series B" } },
  // A competitor only the support agent has seen — new to the canonical graph.
  { offset: "003", shape: "company", key: "y2", operation: "insert", value: { name: "Globex", domain: "globex.io", stage: "Series C" } },
  // A bad sighting gets retracted. The past belief still reconstructs it.
  { offset: "004", shape: "company", key: "y3", operation: "insert", value: { name: "Umbrella", domain: "umbrella.example", stage: "unverified" } },
  { offset: "005", shape: "company", key: "y3", operation: "delete", value: {} },
];

const SALES_BOT = mockShapeSource("sales-bot", SALES_CHANGES);
const SUPPORT_BOT = mockShapeSource("support-bot", SUPPORT_CHANGES);

/** Each stream's changes by name, so provenance can cite the offsets behind a canonical entity. */
const STREAM_CHANGES: Readonly<Record<string, readonly ShapeChange<IntelRow>[]>> = {
  [SALES_BOT.name]: SALES_CHANGES,
  [SUPPORT_BOT.name]: SUPPORT_CHANGES,
};

/** How many sales-bot changes the first consumer run applies before it "crashes". */
const CRASH_AFTER = 2;

// ============================================================
// Idempotent projection: decode one shape change into graph events
// ============================================================
//
// The decoder is a PURE function — it never touches the store, so it is
// testable without a database. `graphProjector` applies what it returns, and
// owns the rules that are easy to get wrong: idempotent upserts, nodes created
// before the edges that reference them, edges removed before their endpoints.

const decode: Decoder<typeof intelGraph, IntelRow> = (change, g) => {
  switch (change.shape) {
    case "person": {
      if (change.operation === "delete") return [g.nodes.Person.remove(change.key)];
      const { name = "", email = "", title = "" } = change.value;
      return [g.nodes.Person.upsert(change.key, { name, email, title })];
    }
    case "company": {
      if (change.operation === "delete") return [g.nodes.Company.remove(change.key)];
      const { name = "", domain = "", stage = "" } = change.value;
      return [g.nodes.Company.upsert(change.key, { name, domain, stage })];
    }
    case "employment": {
      // Endpoints are the agent-local keys, which are the node ids above. The
      // emitter constrains them to worksAt's declared Person -> Company.
      return [
        g.edges.worksAt.upsert(
          { kind: "Person", id: change.value.person ?? "" },
          { kind: "Company", id: change.value.company ?? "" },
        ),
      ];
    }
    default:
      return [];
  }
};

const project: Projector<typeof intelGraph, IntelRow> = graphProjector(intelGraph, decode);

// ============================================================
// Snapshot a belief graph into a branch, then merge into canonical
// ============================================================

async function mergeBeliefInto(
  forkPoint: IntelStore,
  canonical: IntelStore,
  agentId: string,
  belief: IntelStore,
): Promise<{ anchor: RecordedInstant; conflicts: readonly PropertyConflict<typeof intelGraph>[] }> {
  const branchId = asBranchId(agentId);
  // An INGESTION branch, not a plain `branch()`: an agent's belief routinely
  // ALIASES canonical rows (same email, different id) — exactly what the merge
  // exists to reconcile — and a plain branch enforces the fork point's node
  // uniqueness at staging, rejecting the alias before merge planning sees it.
  // This fork point is empty, so nothing collides yet; a populated canonical
  // is the case that needs it.
  const agentBranch = unwrap(await ingestionBranch(forkPoint, makeBackend, { id: branchId }));
  try {
    // Streaming interchange copies the belief in bounded chunks, nodes before
    // edges, with ids preserved so provenance can attribute a merged entity to
    // its source row. `includeTemporal` keeps each fact's valid-time window
    // instead of re-stamping it with the copy's wall clock; soft-deleted rows
    // stay behind, so a retracted sighting cannot resurface in canonical.
    await importGraphStream(agentBranch, exportGraphStream(belief, { includeTemporal: true }), {
      onConflict: "update",
    });

    const result = await mergeIncremental({
      forkPoint,
      target: canonical,
      branches: [agentBranch],
      options: {
        resolve: {
          Person: { similarity: { kind: "fulltext", fields: ["name"] }, threshold: 0.9 },
          Company: { similarity: { kind: "fulltext", fields: ["name"] }, threshold: 0.9 },
        },
        onPropertyConflict: "flag",
        onBasePropertyConflict: "flag",
        branchOrder: [branchId],
        persistProvenance: true,
      },
    });
    if (!isOk(result)) throw result.error;
    const anchor = await canonical.recordedNow();
    if (anchor === undefined) {
      throw new Error(`mergeBeliefInto(${agentId}): canonical store recorded no anchor after merge`);
    }
    return { anchor, conflicts: result.data.conflicts };
  } finally {
    await agentBranch.close();
  }
}

// ============================================================
// Reporting helpers
// ============================================================

type IntelView = { query: IntelStore["query"] };

type PersonRow = Readonly<{ id: string; name: string; email: string; title: string }>;
type CompanyRow = Readonly<{ id: string; name: string; domain: string; stage: string }>;

async function personRows(view: IntelView): Promise<readonly PersonRow[]> {
  return view
    .query()
    .from("Person", "p")
    .select((c) => ({ id: c.p.id, name: c.p.name, email: c.p.email, title: c.p.title }))
    .execute();
}

async function companyRows(view: IntelView): Promise<readonly CompanyRow[]> {
  return view
    .query()
    .from("Company", "c")
    .select((c) => ({ id: c.c.id, name: c.c.name, domain: c.c.domain, stage: c.c.stage }))
    .execute();
}

async function describePeople(view: IntelView): Promise<string> {
  const rows = await personRows(view);
  return rows.map((row) => `${row.name} (${row.title})`).join(", ") || "—";
}

async function describeCompanies(view: IntelView): Promise<string> {
  const rows = await companyRows(view);
  return rows.map((row) => `${row.name} (${row.stage})`).join(", ") || "—";
}

async function describeBelief(view: IntelView): Promise<string> {
  return `people: ${await describePeople(view)} | companies: ${await describeCompanies(view)}`;
}

async function employmentCount(view: IntelView): Promise<number> {
  const links = await view
    .query()
    .from("Person", "p")
    .traverse("worksAt", "e")
    .to("Company", "c")
    .select((ctx) => ({ pid: ctx.p.id, cid: ctx.c.id }))
    .execute();
  return links.length;
}

type EntityCounts = Readonly<{ people: number; companies: number }>;

async function entityCounts(view: IntelView): Promise<EntityCounts> {
  return { people: (await personRows(view)).length, companies: (await companyRows(view)).length };
}

function formatCounts({ people, companies }: EntityCounts): string {
  return `${people} ${people === 1 ? "person" : "people"}, ${companies} ${companies === 1 ? "company" : "companies"}`;
}

function formatValue(value: unknown): string {
  return typeof value === "string" ? `"${value}"` : JSON.stringify(value);
}

// Every wave here merges one branch against what canonical already holds, so
// `values` carries only the incoming branch's side; the kept value is `resolution`.
function describeConflict(conflict: PropertyConflict<typeof intelGraph>): string {
  const incoming = conflict.values.map((entry) => `${entry.branchId}=${formatValue(entry.value)}`).join(", ");
  return `${conflict.kind}.${conflict.property} on ${conflict.entityId}: ${incoming}; kept ${formatValue(conflict.resolution)}`;
}

function describeChange(change: ShapeChange<IntelRow>): string {
  const { offset, operation, shape, key, value } = change;
  if (operation === "delete") return `@${offset} delete ${shape} ${key}`;
  switch (shape) {
    case "person":
      return `@${offset} ${operation} person ${value.name} <${value.email}> (${value.title})`;
    case "company":
      return `@${offset} ${operation} company ${value.name} (${value.stage})`;
    case "employment":
      return `@${offset} ${operation} employment ${value.person} works-at ${value.company}`;
    default:
      return `@${offset} ${operation} ${shape} ${key}`;
  }
}

function printStreamTimeline(name: string, changes: readonly ShapeChange<IntelRow>[]): void {
  console.log(`\n  ${name}`);
  for (const change of changes) console.log(`    ${describeChange(change)}`);
}

async function printSources(provenanceStore: Store<ProvenanceGraph>, canonicalId: string): Promise<void> {
  const rows = await readProvenance(provenanceStore, { canonicalId, role: "node" });
  // Only agent streams: the synthetic `__committed_base__` rows mean "already
  // existed", not an author.
  for (const row of rows) {
    const changes = STREAM_CHANGES[row.branchId];
    if (changes === undefined) continue;
    const offsets = changes.filter((change) => change.key === row.sourceId).map((change) => change.offset);
    console.log(`      ${row.branchId} source ${row.sourceId} @ offsets ${offsets.join(", ")}`);
  }
}

/** Replay `belief` as it stood when `stream` checkpointed `offset`. */
async function describeBeliefAt(belief: IntelBelief, book: CheckpointBook, stream: string, offset: string): Promise<string> {
  const anchor = await book.anchorFor(stream, offset);
  if (anchor === undefined) throw new Error(`no checkpointed anchor for ${stream} @ ${offset}`);
  return describeBelief(belief.asOfRecorded(anchor));
}

export async function main(): Promise<void> {
  section("Electric durable streams → per-agent belief + entity-resolved canonical");

  const cursorStore = await newStore(checkpointGraph);
  const book = typeGraphCheckpoints(cursorStore);
  const salesBelief = await newStore(intelGraph, true);
  const supportBelief = await newStore(intelGraph, true);
  const forkPoint = await newStore(intelGraph, false);
  const canonical = await newStore(intelGraph, true);
  const stores: { close: () => Promise<void> }[] = [cursorStore, salesBelief, supportBelief, forkPoint, canonical];

  try {
    section("(a) Stream timelines — durable observations before graph materialization");
    printStreamTimeline(SALES_BOT.name, SALES_CHANGES);
    printStreamTimeline(SUPPORT_BOT.name, SUPPORT_CHANGES);

    section("(b) Durable consumer — resume from checkpoint, replay safely after crash");

    const partial = await consume({ source: SALES_BOT, store: salesBelief, checkpoints: book, project, stopAfter: CRASH_AFTER });
    const cursorAtCrash = await book.lastOffset(SALES_BOT.name);
    console.log(`\n  consumer ran, then crashed after ${partial.processed} messages`);
    console.log(`    durable cursor: last offset = ${cursorAtCrash}`);
    console.log(`    ${SALES_BOT.name} belief so far: ${formatCounts(await entityCounts(salesBelief))}`);

    // The nastiest crash window: the next change was projected, but its
    // checkpoint write never happened. Restart must replay it without duplicating.
    const [uncheckpointed] = await SALES_BOT.read(cursorAtCrash);
    if (uncheckpointed === undefined) throw new Error(`${SALES_BOT.name} has no change after ${cursorAtCrash}`);
    await salesBelief.transaction((tx) => project(tx, uncheckpointed));
    console.log(`\n  crash window: projected ${uncheckpointed.offset}, then died before checkpointing it`);
    console.log(`    durable cursor is still:          ${await book.lastOffset(SALES_BOT.name)}`);
    console.log(`    belief already has worksAt edges: ${await employmentCount(salesBelief)}`);

    const resumed = await consume({ source: SALES_BOT, store: salesBelief, checkpoints: book, project });
    const edgesAfterReplay = await employmentCount(salesBelief);
    if (resumed.processed !== SALES_CHANGES.length - CRASH_AFTER) {
      throw new Error(`resume should re-deliver every change past the cursor; processed ${resumed.processed}`);
    }
    if (edgesAfterReplay !== 1) throw new Error(`replaying ${uncheckpointed.offset} duplicated the worksAt edge: ${edgesAfterReplay}`);
    console.log(`\n  restarted from ${resumed.fromOffset} — replayed ${uncheckpointed.offset}, processed ${resumed.processed} messages`);
    console.log(`    ${SALES_BOT.name} belief now: ${formatCounts(await entityCounts(salesBelief))} — ${await describePeople(salesBelief)}`);
    console.log(`    worksAt edges after replay: ${edgesAfterReplay} (no duplicate edge)`);

    const rerun = await consume({ source: SALES_BOT, store: salesBelief, checkpoints: book, project });
    if (rerun.processed !== 0) throw new Error(`a caught-up consumer re-applied ${rerun.processed} changes`);
    console.log(`\n  re-run (at-least-once): ${rerun.processed} messages processed; belief unchanged: ${formatCounts(await entityCounts(salesBelief))}`);

    await consume({ source: SUPPORT_BOT, store: supportBelief, checkpoints: book, project });

    section("(c) What did each agent believe, at which offset?");

    console.log(`\n  ${SALES_BOT.name}'s own belief graph:`);
    console.log(`    @offset 001: ${await describeBeliefAt(salesBelief, book, SALES_BOT.name, "001")}`);
    console.log(`    @offset 004: ${await describeBeliefAt(salesBelief, book, SALES_BOT.name, "004")}  (title corrected)`);

    console.log(`\n  ${SUPPORT_BOT.name}'s own belief graph (same person, different surface form):`);
    console.log(`    @offset 004: ${await describeBeliefAt(supportBelief, book, SUPPORT_BOT.name, "004")}`);
    console.log(`    @offset 005: ${await describeBeliefAt(supportBelief, book, SUPPORT_BOT.name, "005")}  (Umbrella retracted)`);
    console.log("\n  → Same email, but neither agent alone knows 'Jane Doe' and 'J. Doe'");
    console.log("    are one person. The retracted company also remains visible in past belief.");

    section("(d) Entity resolution — merge the beliefs into one canonical graph");

    const wave1 = await mergeBeliefInto(forkPoint, canonical, SALES_BOT.name, salesBelief);
    console.log(`\n  [wave 1] merged ${SALES_BOT.name}   — conflicts: ${wave1.conflicts.length}`);
    const wave2 = await mergeBeliefInto(forkPoint, canonical, SUPPORT_BOT.name, supportBelief);
    console.log(`  [wave 2] merged ${SUPPORT_BOT.name} — conflicts: ${wave2.conflicts.length}`);
    for (const conflict of wave2.conflicts) console.log(`    conflict: ${describeConflict(conflict)}`);

    // Jane collapses to one person; Acme collapses to one company; Globex is new;
    // the retracted Umbrella stays out.
    const merged = await entityCounts(canonical);
    if (merged.people !== 1 || merged.companies !== 2) {
      throw new Error(`expected 1 person and 2 companies after entity resolution, got ${formatCounts(merged)}`);
    }
    if (wave2.conflicts.length === 0) throw new Error("the agents disagree on names and titles, but no conflict was flagged");
    console.log(`\n  canonical now: ${formatCounts(merged)} — ${await describePeople(canonical)}`);

    console.log("\n  why does canonical believe this?");
    const provenanceStore = await openProvenanceStore(canonical);
    for (const person of await personRows(canonical)) {
      console.log(`    person ${person.name} <${person.email}>`);
      await printSources(provenanceStore, person.id);
    }
    for (const company of await companyRows(canonical)) {
      console.log(`    company ${company.name} <${company.domain}>`);
      await printSources(provenanceStore, company.id);
    }

    console.log("\n  canonical, time-travelled:");
    console.log(`    asOfRecorded(after wave 1): ${formatCounts(await entityCounts(canonical.asOfRecorded(wave1.anchor)))}`);
    console.log(`    asOfRecorded(after wave 2): ${formatCounts(await entityCounts(canonical.asOfRecorded(wave2.anchor)))}`);

    section("Durable streams → per-agent bitemporal belief → entity-resolved\n canonical. Resumable, idempotent, and replayable by offset.");
    console.log();
  } finally {
    await Promise.allSettled(stores.map((store) => store.close()));
  }
}

runAsMain(import.meta.url, main);
