/**
 * Demo — Electric durable streams → per-agent belief graphs → entity-resolved
 * canonical, built on @nicia-ai/agent-stream-graph.
 *
 *   (a) A durable, resumable consumer over shape changes: it checkpoints the
 *       last offset and the recorded-time anchor per message, so a crashed
 *       materializer restarts from where it left off and at-least-once
 *       re-delivery is idempotent.
 *
 *   (b) A persistent, history-enabled belief graph PER AGENT. Each agent's
 *       stream builds its own bitemporal view, so you can ask what THAT agent
 *       believed at any of its offsets — and see two agents hold different
 *       beliefs about the same entity until they are merged.
 *
 * The transport is a `mockShapeSource`; `electricShapeSource` is the drop-in.
 * Run with:  pnpm demo
 */
import {
  createStoreWithSchema,
  defineEdge,
  defineGraph,
  defineNode,
  searchable,
  type RecordedInstant,
  recordedInstantRevision,
} from "@nicia-ai/typegraph";
import {
  asBranchId,
  ingestionBranch,
  isOk,
  mergeIncremental,
  openProvenanceStore,
  readProvenance,
  type PropertyConflict,
  unwrap,
} from "@nicia-ai/typegraph/graph-merge";
import { exportGraphStream, importGraphStream } from "@nicia-ai/typegraph/interchange";
import { z } from "zod";

import {
  checkpointGraph,
  consume,
  type Decoder,
  graphProjector,
  mockShapeSource,
  typeGraphCheckpoints,
  type Projector,
  type ShapeChange,
} from "../src";
import { type DemoStore, makeBackend, newStore, runAsMain } from "./_support";

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
// The union: this alias spans both the non-history fork point and the
// history-enabled belief/canonical stores, so it names only what they share.
type IntelStore = DemoStore<typeof intelGraph>;

// ============================================================
// Two agents observing the same world through different eyes.
// ============================================================

/**
 * The row shape these agents' streams carry. Typing it is what removes the
 * `as string` casts a decoder would otherwise need — `ShapeChange<V>` has
 * always supported this; the value type just defaults to `Record<string, unknown>`.
 */
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

const STREAM_CHANGES: Record<string, readonly ShapeChange<IntelRow>[]> = {
  "sales-bot": SALES_CHANGES,
  "support-bot": SUPPORT_CHANGES,
};

const SALES_BOT = mockShapeSource("sales-bot", SALES_CHANGES);
const SUPPORT_BOT = mockShapeSource("support-bot", SUPPORT_CHANGES);

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
  // An INGESTION branch, not an ordinary one: an agent's belief is untrusted
  // input, and its rows routinely ALIAS what canonical already holds — the same
  // person under a different id, which is exactly what the merge below exists to
  // reconcile. An ordinary branch inherits the fork point's node uniqueness
  // constraints and would reject such a row at staging, before merge planning
  // ever saw it. An ingestion branch defers node uniqueness to the resolved
  // write set instead. (This fork point starts empty, so nothing collides here
  // yet; a canonical that has been merged into for a while is the case that
  // needs it.)
  const agentBranch = unwrap(await ingestionBranch(forkPoint, makeBackend, { id: branchId }));

  // The branch holds its own native backend handle; close it once the merge has
  // consumed it, so a looped/long-lived caller does not leak one per wave.
  try {
    // Bulk-copy the agent's belief into the merge branch through streaming
    // interchange: bounded chunks (nodes before edges, so endpoint validation
    // resolves), ids preserved so provenance can attribute a merged entity back
    // to the source row, and no graph-sized value held in memory.
    // `includeTemporal` carries the belief's valid-time window across, so the
    // branch does not re-stamp every fact with the copy's own wall clock.
    // Soft-deleted rows stay behind (`includeDeleted` defaults false) — a
    // retracted sighting must not resurface in the canonical merge. The import
    // targets the opaque handle directly; it never sees the branch's Store.
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

async function describePerson(view: IntelView): Promise<string> {
  const rows = await personRows(view);
  return rows.map((row) => `${row.name} (${row.title})`).join(", ") || "—";
}

async function describeCompanies(view: IntelView): Promise<string> {
  const rows = await companyRows(view);
  return rows.map((row) => `${row.name} (${row.stage})`).join(", ") || "—";
}

async function describeBelief(view: IntelView): Promise<string> {
  return `people: ${await describePerson(view)} | companies: ${await describeCompanies(view)}`;
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

async function entityCounts(view: IntelView): Promise<string> {
  const people = await personRows(view);
  const companies = await companyRows(view);
  return `${people.length} person, ${companies.length} ${companies.length === 1 ? "company" : "companies"}`;
}

function formatValue(value: unknown): string {
  return typeof value === "string" ? `"${value}"` : JSON.stringify(value);
}

function branchLabel(branchId: string): string {
  return branchId === "__committed_target__" ? "canonical before this wave" : branchId;
}

function describeConflict(conflict: PropertyConflict<typeof intelGraph>): string {
  const values = conflict.values
    .map((entry) => `${branchLabel(entry.branchId)}=${formatValue(entry.value)}`)
    .join(", ");
  return `${conflict.kind}.${conflict.property} on ${conflict.entityId}: ${values}; kept ${formatValue(conflict.resolution)}`;
}

function describeChange(change: ShapeChange<IntelRow>): string {
  const value = change.value;
  if (change.operation === "delete") return `@${change.offset} delete ${change.shape} ${change.key}`;
  switch (change.shape) {
    case "person":
      return `@${change.offset} ${change.operation} person ${value.name as string} <${value.email as string}> (${value.title as string})`;
    case "company":
      return `@${change.offset} ${change.operation} company ${value.name as string} (${value.stage as string})`;
    case "employment":
      return `@${change.offset} ${change.operation} employment ${(value.person as string)} works-at ${(value.company as string)}`;
    default:
      return `@${change.offset} ${change.operation} ${change.shape} ${change.key}`;
  }
}

function printStreamTimeline(name: string, changes: readonly ShapeChange<IntelRow>[]): void {
  console.log(`\n  ${name}`);
  for (const change of changes) {
    console.log(`    ${describeChange(change)}`);
  }
}

function sourceOffsets(branchId: string, sourceId: string): string {
  const offsets = STREAM_CHANGES[branchId]?.filter((change) => change.key === sourceId).map((change) => change.offset) ?? [];
  return offsets.length === 0 ? "not in this demo stream" : offsets.join(", ");
}

export async function main(): Promise<void> {
  const rule = "━".repeat(74);
  console.log(rule);
  console.log(" Electric durable streams → per-agent belief + entity-resolved canonical");
  console.log(rule);

  const [cursorStore] = await createStoreWithSchema(checkpointGraph, await makeBackend());
  const book = typeGraphCheckpoints(cursorStore);

  const salesBelief = await newStore(intelGraph, true);
  const supportBelief = await newStore(intelGraph, true);
  // A cleanup list spanning belief, checkpoint, and merge stores — closing is
  // the only thing asked of it, so it names only that.
  const stores: Pick<IntelStore, "close">[] = [salesBelief, supportBelief, cursorStore];

  try {
    // ----------------------------------------------------------
    // (a) Raw stream timelines
    // ----------------------------------------------------------
    console.log("\n" + rule);
    console.log(" (a) Stream timelines — durable observations before graph materialization");
    console.log(rule);
    printStreamTimeline("sales-bot", SALES_CHANGES);
    printStreamTimeline("support-bot", SUPPORT_CHANGES);

    // ----------------------------------------------------------
    // (b) Resumable, crash-safe consumption
    // ----------------------------------------------------------
    console.log("\n" + rule);
    console.log(" (b) Durable consumer — resume from checkpoint, replay safely after crash");
    console.log(rule);

    // Process the sales-bot stream, but "crash" after 2 of 4 messages.
    const partial = await consume({ source: SALES_BOT, store: salesBelief, checkpoints: book, project, stopAfter: 2 });
    console.log(`\n  consumer ran, then crashed after ${partial.processed} messages`);
    console.log(`    durable cursor: last offset = ${await book.lastOffset("sales-bot")}`);
    console.log(`    sales-bot belief so far: ${await entityCounts(salesBelief)}`);

    // Simulate the nastiest crash window: the projector wrote offset 003, but the
    // checkpoint write did not happen. Restart must replay 003 without duplicating.
    const uncheckpointed = (await SALES_BOT.read(await book.lastOffset("sales-bot")))[0]!;
    await salesBelief.transaction((tx) => project(tx, uncheckpointed));
    const uncheckpointedAnchor = await salesBelief.recordedNow();
    console.log(`\n  crash window: projected ${uncheckpointed.offset}, then died before checkpoint`);
    // The 40-character anchor string is noise here; its revision is the part
    // that says "the belief moved past the durable cursor".
    console.log(
      `    uncheckpointed belief at revision: ${
        uncheckpointedAnchor === undefined ?
          "none"
        : recordedInstantRevision(uncheckpointedAnchor)
      }`,
    );
    console.log(`    durable cursor is still:       ${await book.lastOffset("sales-bot")}`);
    console.log(`    belief already has worksAt edges: ${await employmentCount(salesBelief)}`);

    // Restart: a fresh consumer reads the durable cursor and resumes.
    const resumed = await consume({ source: SALES_BOT, store: salesBelief, checkpoints: book, project });
    console.log(`\n  restarted — replayed 003, then processed 004 (${resumed.processed} messages)`);
    console.log(`    sales-bot belief now: ${await entityCounts(salesBelief)} — ${await describePerson(salesBelief)}`);
    console.log(`    worksAt edges after replay: ${await employmentCount(salesBelief)} (no duplicate edge)`);

    // At-least-once: re-run with the cursor at the end — nothing to do, and even
    // replaying applied rows would upsert (no duplicates).
    const replay = await consume({ source: SALES_BOT, store: salesBelief, checkpoints: book, project });
    console.log(`\n  re-run (at-least-once): ${replay.processed} messages processed; belief unchanged: ${await entityCounts(salesBelief)}`);

    // Bring the support-bot fully up to date too.
    await consume({ source: SUPPORT_BOT, store: supportBelief, checkpoints: book, project });

    // ----------------------------------------------------------
    // (c) Per-agent belief, time-travelled by offset
    // ----------------------------------------------------------
    console.log("\n" + rule);
    console.log(" (c) What did each agent believe, at which offset?");
    console.log(rule);

    // `anchorFor` returns a branded RecordedInstant — replay needs no cast.
    const salesAt1 = await book.anchorFor("sales-bot", "001");
    const salesAt4 = await book.anchorFor("sales-bot", "004");
    console.log("\n  sales-bot's own belief graph:");
    console.log(`    @offset 001: ${await describeBelief(salesBelief.asOfRecorded(salesAt1!))}`);
    console.log(`    @offset 004: ${await describeBelief(salesBelief.asOfRecorded(salesAt4!))}  (title corrected)`);

    const supportAt4 = await book.anchorFor("support-bot", "004");
    const supportAt5 = await book.anchorFor("support-bot", "005");
    console.log("\n  support-bot's own belief graph (same person, different surface form):");
    console.log(`    @offset 004: ${await describeBelief(supportBelief.asOfRecorded(supportAt4!))}`);
    console.log(`    @offset 005: ${await describeBelief(supportBelief.asOfRecorded(supportAt5!))}  (Umbrella retracted)`);
    console.log("\n  → Same email, but neither agent alone knows 'Jane Doe' and 'J. Doe'");
    console.log("    are one person. The retracted company also remains visible in past belief.");

    // ----------------------------------------------------------
    // Merge the per-agent beliefs into a canonical, entity-resolved graph
    // ----------------------------------------------------------
    console.log("\n" + rule);
    console.log(" Entity resolution — merge the beliefs into one canonical graph");
    console.log(rule);

    const forkPoint = await newStore(intelGraph, false);
    const canonical = await newStore(intelGraph, true);
    stores.push(forkPoint, canonical);

    const wave1 = await mergeBeliefInto(forkPoint, canonical, "sales-bot", salesBelief);
    console.log(`\n  [wave 1] merged sales-bot  — conflicts: ${wave1.conflicts.length}`);
    const wave2 = await mergeBeliefInto(forkPoint, canonical, "support-bot", supportBelief);
    console.log(`  [wave 2] merged support-bot — conflicts: ${wave2.conflicts.length}`);
    for (const conflict of wave2.conflicts) {
      console.log(`    conflict: ${describeConflict(conflict)}`);
    }

    console.log(`\n  canonical now: ${await entityCounts(canonical)} — ${await describePerson(canonical)}`);
    const provenanceStore = await openProvenanceStore(canonical);
    for (const agent of ["sales-bot", "support-bot"]) {
      const touched = await readProvenance(provenanceStore, { branchId: asBranchId(agent) });
      console.log(`    provenance — ${agent} contributed to ${touched.length} canonical entities`);
    }

    console.log("\n  why does canonical believe this?");
    for (const person of await personRows(canonical)) {
      console.log(`    person ${person.name} <${person.email}>`);
      const provenance = (await readProvenance(provenanceStore, { canonicalId: person.id, role: "node" })).filter(
        (row) => STREAM_CHANGES[row.branchId] !== undefined,
      );
      for (const row of provenance) {
        console.log(`      ${branchLabel(row.branchId)} source ${row.sourceId} @ offsets ${sourceOffsets(row.branchId, row.sourceId)}`);
      }
    }
    for (const company of await companyRows(canonical)) {
      console.log(`    company ${company.name} <${company.domain}>`);
      const provenance = (await readProvenance(provenanceStore, { canonicalId: company.id, role: "node" })).filter(
        (row) => STREAM_CHANGES[row.branchId] !== undefined,
      );
      for (const row of provenance) {
        console.log(`      ${branchLabel(row.branchId)} source ${row.sourceId} @ offsets ${sourceOffsets(row.branchId, row.sourceId)}`);
      }
    }

    console.log("\n  canonical, time-travelled:");
    console.log(`    asOfRecorded(after wave 1): ${await entityCounts(canonical.asOfRecorded(wave1.anchor))}`);
    console.log(`    asOfRecorded(after wave 2): ${await entityCounts(canonical.asOfRecorded(wave2.anchor))}`);

    console.log("\n" + rule);
    console.log(" Durable streams → per-agent bitemporal belief → entity-resolved");
    console.log(" canonical. Resumable, idempotent, and replayable by offset.");
    console.log(rule + "\n");
  } finally {
    await Promise.allSettled(stores.map((store) => store.close()));
  }
}

runAsMain(import.meta.url, main);
