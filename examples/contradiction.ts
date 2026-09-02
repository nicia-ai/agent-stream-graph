/**
 * Demo — two agents, one entity, no silent winner.
 *
 * Two agents materialize their OWN belief store from their OWN stream, about
 * the same real-world subject seen through different surface forms — but
 * sharing the one field that ties them together: an email address. Merging
 * those beliefs must (a) COLLAPSE what agrees — same email -> one canonical
 * entity — and (b) FLAG what disagrees: a conflicting property (here, a risk
 * score) is never silently overwritten by "whoever merged last"; it comes
 * back as a `PropertyConflict` for a human or policy layer to resolve.
 *
 * This is the distilled claim behind `examples/agents.ts`, which shows the
 * full pipeline (durable resumable consumption, per-agent bitemporal belief,
 * entity resolution) at length. Read that file for the mechanics; this one
 * exists so the single claim above is impossible to miss.
 *
 * Run with: pnpm tsx examples/contradiction.ts
 */
import { createStoreWithSchema, defineGraph, defineNode, searchable } from "@nicia-ai/typegraph";
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
const STREAM_CHANGES: Record<string, readonly ShapeChange<SubjectRow>[]> = { "intake-agent": INTAKE_CHANGES, "audit-agent": AUDIT_CHANGES };

const INTAKE_AGENT = mockShapeSource("intake-agent", INTAKE_CHANGES);
const AUDIT_AGENT = mockShapeSource("audit-agent", AUDIT_CHANGES);

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
  // An INGESTION branch, not a plain `branch()`: a plain branch inherits the
  // fork point's node uniqueness constraints, so staging a row that ALIASES
  // canonical (same email, different id — exactly what entity resolution
  // exists to reconcile) would fail the constraint before merge planning ever
  // saw it. See README's fork/merge section.
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

function formatValue(value: unknown): string {
  return typeof value === "string" ? `"${value}"` : JSON.stringify(value);
}

function branchLabel(branchId: string): string {
  return branchId === "__committed_target__" ? "canonical before this wave" : branchId;
}

function describeConflict(conflict: PropertyConflict<typeof dossierGraph>): string {
  const values = conflict.values.map((entry) => `${branchLabel(entry.branchId)}=${formatValue(entry.value)}`).join(", ");
  return `${conflict.kind}.${conflict.property} on ${conflict.entityId}: ${values}; kept ${formatValue(conflict.resolution)}`;
}

function sourceOffset(branchId: string, sourceId: string): string {
  return STREAM_CHANGES[branchId]?.find((change) => change.key === sourceId)?.offset ?? "?";
}

const RULE = "━".repeat(74);
function banner(title: string): void {
  console.log(`\n${RULE}\n ${title}\n${RULE}`);
}

export async function main(): Promise<void> {
  banner("Two agents, one entity, no silent winner");

  const [cursorStore] = await createStoreWithSchema(checkpointGraph, await makeBackend());
  const book = typeGraphCheckpoints(cursorStore);
  const intakeBelief = await newStore(dossierGraph, true);
  const auditBelief = await newStore(dossierGraph, true);
  const forkPoint = await newStore(dossierGraph, false);
  const canonical = await newStore(dossierGraph, true);
  const stores: Pick<DossierStore, "close">[] = [cursorStore, intakeBelief, auditBelief, forkPoint, canonical];

  try {
    // Each agent materializes its own belief from its own stream.
    banner("Two agents watch the same subject through different feeds");
    await consume({ source: INTAKE_AGENT, store: intakeBelief, checkpoints: book, project });
    await consume({ source: AUDIT_AGENT, store: auditBelief, checkpoints: book, project });
    console.log(`\n  intake-agent believes: "J. Doe" <doe@vendor-corp.example> riskScore=22`);
    console.log(`  audit-agent believes:  "Jane Doe" <doe@vendor-corp.example> riskScore=87`);
    console.log("  -> same email, different name, different risk. Neither agent alone knows the other exists.");

    // Merge both beliefs into one canonical, entity-resolved graph.
    banner("Merge — entity resolution collapses identity, but never a conflict");
    const wave1 = await mergeBeliefInto(forkPoint, canonical, "intake-agent", intakeBelief);
    console.log(`\n  [wave 1] merged intake-agent — conflicts: ${wave1.length}`);
    const wave2 = await mergeBeliefInto(forkPoint, canonical, "audit-agent", auditBelief);
    console.log(`  [wave 2] merged audit-agent  — conflicts: ${wave2.length}`);

    // (2) Identity: the two rows collapse to ONE canonical entity.
    const subjects = await subjectRows(canonical);
    if (subjects.length !== 1) {
      throw new Error(`expected the two beliefs to collapse into ONE entity, got ${subjects.length}`);
    }
    const subject = subjects[0];
    if (subject === undefined) throw new Error("subject row missing after length check");
    console.log(`\n  canonical entity count: ${subjects.length}`);
    console.log(`  surviving identity: "${subject.name}" <${subject.email}> riskScore=${subject.riskScore}`);

    // (3) The disagreement must be FLAGGED, never silently resolved.
    if (wave2.length === 0) {
      throw new Error("merge silently resolved the riskScore disagreement — no PropertyConflict reported");
    }
    console.log(`\n  ${wave2.length} conflict(s) flagged, not silently resolved:`);
    for (const conflict of wave2) console.log(`    ${describeConflict(conflict)}`);

    // (4) Provenance: which agent, at which offset, contributed which value.
    const provenanceStore = await openProvenanceStore(canonical);
    for (const agentId of ["intake-agent", "audit-agent"]) {
      const touched = await readProvenance(provenanceStore, { branchId: asBranchId(agentId) });
      console.log(`\n  provenance — ${agentId} contributed to ${touched.length} canonical entit${touched.length === 1 ? "y" : "ies"}`);
    }
    const aliases = (await readProvenance(provenanceStore, { canonicalId: subject.id, role: "node" })).filter(
      (row) => STREAM_CHANGES[row.branchId] !== undefined,
    );
    console.log(`\n  aliases absorbed into ${subject.name}:`);
    for (const row of aliases) {
      console.log(`    ${branchLabel(row.branchId)} source ${row.sourceId} @ offset ${sourceOffset(row.branchId, row.sourceId)}`);
    }

    banner("No silent winner: riskScore=22 vs riskScore=87 was FLAGGED, not averaged");
    console.log(" or overwritten. A human or policy layer reviews conflicts like the one above");
    console.log(" before trusting canonical. (planMergeIncremental + applyMergePlan — see");
    console.log(" README's fork/merge section — let that review happen BEFORE the write set");
    console.log(" lands: same conflicts, inside a reviewable MergePlanArtifact, instead of");
    console.log(" committing in one call.)\n");
  } finally {
    await Promise.allSettled(stores.map((store) => store.close()));
  }
}

runAsMain(import.meta.url, main);
