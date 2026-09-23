/**
 * Demo — two agents, one entity, no silent winner.
 *
 * Two agents each materialize their OWN belief from their OWN stream about the
 * same real-world subject, seen through different surface forms but sharing
 * the one field that ties them together: an email address. Merging those
 * beliefs must (a) COLLAPSE what agrees — same email, one canonical entity —
 * and (b) FLAG what disagrees: the conflicting risk score is never silently
 * overwritten by "whoever merged last"; it comes back as a `PropertyConflict`
 * for a human or policy layer to resolve.
 *
 * `examples/agents.ts` shows the full pipeline at length; this is its single
 * central claim, distilled.
 *
 * Run with:  pnpm demo:contradiction
 */
import { defineGraph, defineNode, searchable, type Store } from "@nicia-ai/typegraph";
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
  checkpointGraph,
  consume,
  type Decoder,
  graphProjector,
  mockShapeSource,
  typeGraphCheckpoints,
  type Projector,
  type ShapeChange,
} from "../src";
import { type DemoStore, makeBackend, newStore, runAsMain, section } from "./_support";

// ============================================================
// The entity each agent forms a belief about
// ============================================================

const Subject = defineNode("Subject", {
  schema: z.object({
    name: searchable({ language: "english" }),
    email: z.string(),
    riskScore: z.number(),
  }),
});

const dossierGraph = defineGraph({
  id: "risk_dossier",
  nodes: {
    Subject: { type: Subject, unique: [{ name: "subject_email", fields: ["email"], scope: "kind", collation: "caseInsensitive" }] },
  },
  edges: {},
});
type DossierStore = DemoStore<typeof dossierGraph>;

// ============================================================
// Two agents, two streams, one subject
// ============================================================

type SubjectRow = Readonly<{ name?: string; email?: string; riskScore?: number }>;

const INTAKE_CHANGES: readonly ShapeChange<SubjectRow>[] = [
  { offset: "001", shape: "subject", key: "s1", operation: "insert", value: { name: "J. Doe", email: "doe@vendor-corp.example", riskScore: 22 } },
];
const AUDIT_CHANGES: readonly ShapeChange<SubjectRow>[] = [
  { offset: "001", shape: "subject", key: "a1", operation: "insert", value: { name: "Jane Doe", email: "doe@vendor-corp.example", riskScore: 87 } },
];

const INTAKE_AGENT = mockShapeSource("intake-agent", INTAKE_CHANGES);
const AUDIT_AGENT = mockShapeSource("audit-agent", AUDIT_CHANGES);

/** Each stream's changes by name, so provenance can cite the offset behind an alias. */
const STREAM_CHANGES: Readonly<Record<string, readonly ShapeChange<SubjectRow>[]>> = {
  [INTAKE_AGENT.name]: INTAKE_CHANGES,
  [AUDIT_AGENT.name]: AUDIT_CHANGES,
};

/** The property the two agents disagree on — the one that must come back flagged. */
const DISPUTED_PROPERTY = "riskScore";

const decode: Decoder<typeof dossierGraph, SubjectRow> = (change, g) => {
  if (change.operation === "delete") return [g.nodes.Subject.remove(change.key)];
  const { name = "", email = "", riskScore = 0 } = change.value;
  return [g.nodes.Subject.upsert(change.key, { name, email, riskScore })];
};
const project: Projector<typeof dossierGraph, SubjectRow> = graphProjector(dossierGraph, decode);

// ============================================================
// Stage a belief into an ingestion branch, then merge into canonical
// ============================================================

async function mergeBeliefInto(
  forkPoint: DossierStore,
  canonical: DossierStore,
  agentId: string,
  belief: DossierStore,
): Promise<readonly PropertyConflict<typeof dossierGraph>[]> {
  const branchId = asBranchId(agentId);
  // An INGESTION branch, not a plain `branch()`: a plain branch enforces the
  // fork point's node uniqueness at staging, so a row that ALIASES canonical
  // (same email, different id — exactly what entity resolution reconciles)
  // would be rejected before merge planning saw it. See README's
  // "Forking a stream at a checkpoint".
  const agentBranch = unwrap(await ingestionBranch(forkPoint, makeBackend, { id: branchId }));
  try {
    await importGraphStream(agentBranch, exportGraphStream(belief, { includeTemporal: true }), { onConflict: "update" });
    const result = await mergeIncremental({
      forkPoint,
      target: canonical,
      branches: [agentBranch],
      options: {
        resolve: { Subject: { similarity: { kind: "fulltext", fields: ["name"] }, threshold: 0.9 } },
        onPropertyConflict: "flag",
        onBasePropertyConflict: "flag",
        branchOrder: [branchId],
        persistProvenance: true,
      },
    });
    if (!isOk(result)) throw result.error;
    return result.data.conflicts;
  } finally {
    await agentBranch.close();
  }
}

// ============================================================
// Reporting helpers
// ============================================================

type SubjectViewRow = Readonly<{ id: string; name: string; email: string; riskScore: number }>;

async function subjectRows(view: { query: DossierStore["query"] }): Promise<readonly SubjectViewRow[]> {
  return view
    .query()
    .from("Subject", "s")
    .select((c) => ({ id: c.s.id, name: c.s.name, email: c.s.email, riskScore: c.s.riskScore }))
    .execute();
}

function describeSubject(subject: SubjectViewRow): string {
  return `"${subject.name}" <${subject.email}> riskScore=${subject.riskScore}`;
}

function formatValue(value: unknown): string {
  return typeof value === "string" ? `"${value}"` : JSON.stringify(value);
}

// Each wave merges one branch against what canonical already holds, so `values`
// carries only the incoming branch's side; the kept value is `resolution`.
function describeConflict(conflict: PropertyConflict<typeof dossierGraph>): string {
  const incoming = conflict.values.map((entry) => `${entry.branchId}=${formatValue(entry.value)}`).join(", ");
  return `${conflict.kind}.${conflict.property} on ${conflict.entityId}: ${incoming}; kept ${formatValue(conflict.resolution)}`;
}

async function printAliases(provenanceStore: Store<ProvenanceGraph>, subject: SubjectViewRow): Promise<void> {
  console.log(`\n  aliases absorbed into ${subject.name}:`);
  for (const row of await readProvenance(provenanceStore, { canonicalId: subject.id, role: "node" })) {
    // Only agent streams: the synthetic `__committed_base__` rows mean
    // "already existed", not an author.
    const offset = STREAM_CHANGES[row.branchId]?.find((change) => change.key === row.sourceId)?.offset;
    if (offset !== undefined) console.log(`    ${row.branchId} source ${row.sourceId} @ offset ${offset}`);
  }
}

export async function main(): Promise<void> {
  section("Two agents, one entity, no silent winner");

  const cursorStore = await newStore(checkpointGraph);
  const book = typeGraphCheckpoints(cursorStore);
  const intakeBelief = await newStore(dossierGraph, true);
  const auditBelief = await newStore(dossierGraph, true);
  const forkPoint = await newStore(dossierGraph, false);
  const canonical = await newStore(dossierGraph, true);
  const stores: { close: () => Promise<void> }[] = [cursorStore, intakeBelief, auditBelief, forkPoint, canonical];

  try {
    section("Two agents watch the same subject through different feeds");
    const agents = [
      { source: INTAKE_AGENT, belief: intakeBelief },
      { source: AUDIT_AGENT, belief: auditBelief },
    ];
    console.log();
    for (const { source, belief } of agents) {
      await consume({ source, store: belief, checkpoints: book, project });
      const label = `${source.name} believes:`;
      for (const subject of await subjectRows(belief)) console.log(`  ${label.padEnd(24)}${describeSubject(subject)}`);
    }
    console.log("  -> same email, different name, different risk. Neither agent alone knows the other exists.");

    section("Merge — entity resolution collapses identity, but never a conflict");
    const wave1 = await mergeBeliefInto(forkPoint, canonical, INTAKE_AGENT.name, intakeBelief);
    console.log(`\n  [wave 1] merged ${INTAKE_AGENT.name} — conflicts: ${wave1.length}`);
    const wave2 = await mergeBeliefInto(forkPoint, canonical, AUDIT_AGENT.name, auditBelief);
    console.log(`  [wave 2] merged ${AUDIT_AGENT.name}  — conflicts: ${wave2.length}`);

    const subjects = await subjectRows(canonical);
    const [subject] = subjects;
    if (subject === undefined || subjects.length !== 1) {
      throw new Error(`expected the two beliefs to collapse into ONE entity, got ${subjects.length}`);
    }
    console.log(`\n  canonical entity count: ${subjects.length}`);
    console.log(`  surviving identity: ${describeSubject(subject)}`);

    const disputed = wave2.find((conflict) => conflict.property === DISPUTED_PROPERTY);
    if (disputed === undefined) {
      throw new Error(`merge silently resolved the ${DISPUTED_PROPERTY} disagreement — no PropertyConflict reported`);
    }
    console.log(`\n  ${wave2.length} conflict(s) flagged, not silently resolved:`);
    for (const conflict of wave2) console.log(`    ${describeConflict(conflict)}`);

    await printAliases(await openProvenanceStore(canonical), subject);

    const incoming = disputed.values.map((entry) => formatValue(entry.value)).join(", ");
    section(`No silent winner: ${DISPUTED_PROPERTY} ${formatValue(disputed.resolution)} vs ${incoming} was FLAGGED, not averaged or overwritten`);
    console.log(" A human or policy layer reviews conflicts like this before trusting canonical.");
    console.log(" To review BEFORE the write lands, `planMergeIncremental` returns the same");
    console.log(" conflicts in a reviewable `MergePlanArtifact` for `applyMergePlan` to commit");
    console.log(" (README: \"Forking a stream at a checkpoint\").\n");
  } finally {
    await Promise.allSettled(stores.map((store) => store.close()));
  }
}

runAsMain(import.meta.url, main);
