/**
 * Demo — agent stream → justified belief → source retraction with cascading
 * belief revision, built on `@nicia-ai/typegraph/provenance` + this package's
 * durable consumer.
 *
 * Where `examples/agents.ts` projects a flat stream (each change is a direct
 * observation), this demo models a DERIVED belief: facts are grounded by
 * sources through explicit justification nodes. Retracting a source makes the
 * facts only it supported non-current (soft-deleted), while facts with
 * alternate support survive — and recorded time keeps the before/after audit
 * trail.
 *
 *   Source(s) ──premiseOf──▶ Justification ──derives──▶ Fact(s)
 *
 * Provenance surfaces exercised:
 *  - two source kinds (`ScannerSource`, `VendorSource`) via `source.kinds`
 *  - a TERMINAL fact (`DeployDecision`) that is never a premise, so it is left
 *    out of `premiseOf.from` — a cascade still reaches it through `derives`
 *  - `retractMany` / `unRetractMany` for bulk retraction
 *  - graph-typed refs: `{ kind: "ScannerSource", id }` is checked against the
 *    graph's node kinds at compile time
 *
 * Every belief state and replay below is asserted, not just printed.
 *
 * Run with:  pnpm demo:provenance
 */
import {
  asNodeId,
  defineEdge,
  defineGraph,
  defineNode,
  type HistoryStore,
} from "@nicia-ai/typegraph";
import {
  createRetractionCapability,
  type ProvenanceFactRef,
  type RetractionCapability,
  type RetractionReport,
} from "@nicia-ai/typegraph/provenance";
import { z } from "zod";

import {
  checkpointGraph,
  consume,
  mockShapeSource,
  typeGraphCheckpoints,
  type Projector,
  type ShapeChange,
} from "../src";
import { newStore, RULE, runAsMain } from "./_support";

// ============================================================
// Provenance graph: two source kinds, two fact kinds, AND-justifications
// ============================================================

const ScannerSource = defineNode("ScannerSource", {
  schema: z.object({
    label: z.string(),
    retracted: z.boolean().default(false),
  }),
});

const VendorSource = defineNode("VendorSource", {
  schema: z.object({
    label: z.string(),
    retracted: z.boolean().default(false),
  }),
});

const Vulnerability = defineNode("Vulnerability", {
  schema: z.object({ cve: z.string(), pkg: z.string() }),
});

const DeployDecision = defineNode("DeployDecision", {
  schema: z.object({ action: z.string() }),
});

const Justification = defineNode("Justification", {
  schema: z.object({ rule: z.string() }),
});

const premiseOf = defineEdge("premiseOf", { schema: z.object({}) });
const derives = defineEdge("derives", { schema: z.object({}) });

const securityGraph = defineGraph({
  id: "security_provenance",
  nodes: {
    ScannerSource: { type: ScannerSource },
    VendorSource: { type: VendorSource },
    Vulnerability: { type: Vulnerability },
    DeployDecision: { type: DeployDecision },
    Justification: { type: Justification },
  },
  edges: {
    // Sources and non-terminal facts are premises; the terminal DeployDecision
    // is not, so the schema admits no meaningless DeployDecision premise edge.
    premiseOf: { type: premiseOf, from: [ScannerSource, VendorSource, Vulnerability], to: [Justification] },
    derives: { type: derives, from: [Justification], to: [Vulnerability, DeployDecision] },
  },
});

type SecurityStore = HistoryStore<typeof securityGraph>;

// Kinds are graph-typed, so a typo like "ScanerSource" is a compile error
// rather than a runtime ConfigurationError.
const retractionConfig = {
  source: { kinds: ["ScannerSource", "VendorSource"] },
  justification: { kind: "Justification" },
  fact: { kinds: ["Vulnerability", "DeployDecision"] },
  premiseOf: { kind: "premiseOf" },
  derives: { kind: "derives" },
} as const;

// ============================================================
// The stream: two scanner findings, one vendor advisory, one policy
// ============================================================

const STREAM_NAME = "security-intel";

const SCANNER_1 = { kind: "ScannerSource", id: "scanner-1" } as const;
const SCANNER_2 = { kind: "ScannerSource", id: "scanner-2" } as const;
const VENDOR = { kind: "VendorSource", id: "vendor-1" } as const;
const VULN_ID = "vuln-libvector";
const DECISION_ID = "block-deploy";

const LIBVECTOR_CVE = { vulnId: VULN_ID, cve: "CVE-2026-1234", pkg: "libvector 4.2" } as const;

type FindingValue = Readonly<{
  sourceKind: "ScannerSource" | "VendorSource";
  sourceId: string;
  sourceLabel: string;
  vulnId: string;
  cve: string;
  pkg: string;
}>;
type PolicyValue = Readonly<{ vulnId: string; decisionId: string; action: string }>;
type IntelValue = FindingValue | PolicyValue;

const INTEL_CHANGES: readonly ShapeChange<IntelValue>[] = [
  { offset: "001", shape: "finding", key: SCANNER_1.id, operation: "insert", value: { sourceKind: SCANNER_1.kind, sourceId: SCANNER_1.id, sourceLabel: "Unverified scanner #1", ...LIBVECTOR_CVE } },
  { offset: "002", shape: "finding", key: SCANNER_2.id, operation: "insert", value: { sourceKind: SCANNER_2.kind, sourceId: SCANNER_2.id, sourceLabel: "Unverified scanner #2", ...LIBVECTOR_CVE } },
  { offset: "003", shape: "advisory", key: VENDOR.id, operation: "insert", value: { sourceKind: VENDOR.kind, sourceId: VENDOR.id, sourceLabel: "Vendor security advisory", ...LIBVECTOR_CVE } },
  { offset: "004", shape: "policy", key: "policy-1", operation: "insert", value: { vulnId: VULN_ID, decisionId: DECISION_ID, action: "Block the production deploy" } },
];

// ============================================================
// Idempotent projection: shape change → Source/Fact/Justification scaffold
// ============================================================

function isFinding(value: IntelValue): value is FindingValue {
  return "sourceKind" in value;
}

function justificationId(premiseId: string, factId: string): string {
  return `${premiseId}>>${factId}`;
}

const project: Projector<typeof securityGraph, IntelValue> = async (tx, change) => {
  const value = change.value;

  if (isFinding(value)) {
    const sourceRef = { kind: value.sourceKind, id: value.sourceId } as const;
    const vulnRef = { kind: "Vulnerability", id: value.vulnId } as const;
    const jId = justificationId(value.sourceId, value.vulnId);
    const jRef = { kind: "Justification", id: jId } as const;

    // The source kind is data-driven from the stream; branching on it reaches
    // the typed collection without indexing `tx.nodes` dynamically. A retraction
    // is a judgement about the source, not something it reports, so a
    // re-delivered finding carries the existing flag forward instead of
    // resetting it.
    const sources = value.sourceKind === "ScannerSource" ? tx.nodes.ScannerSource : tx.nodes.VendorSource;
    const existingSource = await sources.getById(asNodeId(value.sourceId));
    await sources.upsertById(value.sourceId, { label: value.sourceLabel, retracted: existingSource?.retracted ?? false });
    await tx.nodes.Vulnerability.upsertById(value.vulnId, { cve: value.cve, pkg: value.pkg });
    await tx.nodes.Justification.upsertById(jId, { rule: `${value.sourceLabel} reports ${value.cve}` });
    await tx.edges.premiseOf.getOrCreateByEndpoints(sourceRef, jRef, {});
    await tx.edges.derives.getOrCreateByEndpoints(jRef, vulnRef, {});
    return;
  }

  const vulnRef = { kind: "Vulnerability", id: value.vulnId } as const;
  const decisionRef = { kind: "DeployDecision", id: value.decisionId } as const;
  const jId = justificationId(value.vulnId, value.decisionId);
  const jRef = { kind: "Justification", id: jId } as const;

  await tx.nodes.DeployDecision.upsertById(value.decisionId, { action: value.action });
  await tx.nodes.Justification.upsertById(jId, { rule: `${value.action} when ${value.vulnId} is believed` });
  await tx.edges.premiseOf.getOrCreateByEndpoints(vulnRef, jRef, {});
  await tx.edges.derives.getOrCreateByEndpoints(jRef, decisionRef, {});
};

// ============================================================
// Reporting and assertion helpers
// ============================================================

const NONE = "(none)";
const NON_CURRENT = "(retracted — non-current)";

type Holding = Pick<RetractionCapability<typeof securityGraph>, "holding">;

function formatRefs(refs: readonly ProvenanceFactRef<typeof securityGraph>[]): string {
  if (refs.length === 0) return NONE;
  return refs.map((r) => `${r.kind}/${r.id}`).sort().join(", ");
}

function formatReport(report: RetractionReport<typeof securityGraph>): string {
  const survived = report.survivedVia.length === 0
    ? NONE
    : report.survivedVia.map((s) => `${s.fact.id} via ${s.via.map((j) => j.id).join(" + ")}`).sort().join("; ");
  return [
    `    died:       ${formatRefs(report.died)}`,
    `    survived:   ${survived}`,
    `    unaffected: ${formatRefs(report.unaffected)}`,
  ].join("\n");
}

const BOTH_FACTS_HELD = `DeployDecision/${DECISION_ID}, Vulnerability/${VULN_ID}`;

/**
 * Prints the current derived belief and throws unless it matches `expected`.
 * The vulnerability and the decision derived from it always share one fate
 * here, so a single expectation covers both.
 */
async function showBelief(provenance: Holding, store: SecurityStore, expected: "held" | "non-current"): Promise<void> {
  const holding = formatRefs(await provenance.holding());
  const vuln = await store.nodes.Vulnerability.getById(asNodeId(VULN_ID));
  const decision = await store.nodes.DeployDecision.getById(asNodeId(DECISION_ID));
  console.log(`    holding():       ${holding}`);
  console.log(`    vulnerability:   ${vuln === undefined ? NON_CURRENT : `${vuln.cve} on ${vuln.pkg}`}`);
  console.log(`    deploy decision: ${decision?.action ?? NON_CURRENT}`);

  const held = expected === "held";
  const matches =
    holding === (held ? BOTH_FACTS_HELD : NONE) && (vuln !== undefined) === held && (decision !== undefined) === held;
  if (!matches) {
    throw new Error(`expected the vulnerability and deploy decision to be ${expected}; holding() = ${holding}`);
  }
}

// ============================================================
// Main
// ============================================================

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" Agent stream → justified belief → source retraction");
  console.log(RULE);

  const belief = await newStore(securityGraph, true);
  const cursor = await newStore(checkpointGraph);

  try {
    // --- (a) Durable consumption: stream → provenance graph ----------------
    const book = typeGraphCheckpoints(cursor);
    const source = mockShapeSource(STREAM_NAME, INTEL_CHANGES);
    const result = await consume({ source, store: belief, checkpoints: book, project });
    const consumedOffset = result.lastOffset;
    const consumedAnchor = consumedOffset === undefined ? undefined : await book.anchorFor(STREAM_NAME, consumedOffset);
    if (consumedAnchor === undefined) throw new Error(`no checkpoint anchor after consuming ${STREAM_NAME}`);
    console.log(`\n  Durable consumer projected ${result.processed} changes into the provenance graph.`);
    console.log(`    cursor at offset: ${consumedOffset}`);

    // --- (b) Initial well-founded belief -----------------------------------
    const provenance = createRetractionCapability(belief, retractionConfig);
    console.log(`\n  Initial derived belief:`);
    await showBelief(provenance, belief, "held");

    // --- (c) Bulk-retract both scanners: the vendor advisory still holds ----
    // The vulnerability survives through the vendor advisory, and the deploy
    // decision survives because its premise (the vulnerability) is still held.
    console.log(`\n  retractMany([scanner-1, scanner-2]) — bulk-retract both unverified scanners.`);
    console.log(formatReport(await provenance.retractMany([SCANNER_1, SCANNER_2])));
    await showBelief(provenance, belief, "held");

    // --- (d) Retract the vendor advisory too: cascade -----------------------
    // The vulnerability loses its last support and dies; the deploy decision,
    // whose only premise was that vulnerability, dies with it.
    console.log(`\n  retract(vendor-1) — cascade: vulnerability and deploy decision die.`);
    console.log(formatReport(await provenance.retract(VENDOR)));
    await showBelief(provenance, belief, "non-current");
    const afterVendorRetraction = await belief.recordedNow();
    if (afterVendorRetraction === undefined) throw new Error("expected a recorded instant after the retraction");

    // --- (e) Recorded-time replay: the audit trail --------------------------
    // Retraction soft-deletes, so the pre-retraction belief is still
    // reconstructible — here from the consumer's own checkpoint anchor.
    const decisionAsConsumed = await belief.asOfRecorded(consumedAnchor).nodes.DeployDecision.getById(asNodeId(DECISION_ID));
    const decisionAfter = await belief.asOfRecorded(afterVendorRetraction).nodes.DeployDecision.getById(asNodeId(DECISION_ID));
    console.log(`\n  Recorded-time replay of the deploy decision:`);
    console.log(`    @offset ${consumedOffset} (as consumed): ${decisionAsConsumed?.action ?? NON_CURRENT}`);
    console.log(`    @after vendor retraction:  ${decisionAfter?.action ?? NON_CURRENT}`);
    if (decisionAsConsumed === undefined || decisionAfter !== undefined) {
      throw new Error("expected replay to show the decision current as consumed and non-current after the retraction");
    }

    // --- (f) Bulk un-retract both scanners: belief reopens ------------------
    console.log(`\n  unRetractMany([scanner-1, scanner-2]) — bulk-restore both scanners.`);
    console.log(formatReport(await provenance.unRetractMany([SCANNER_1, SCANNER_2])));
    await showBelief(provenance, belief, "held");

    console.log(`\n${RULE}`);
    console.log(" Retraction revises belief currency; recorded time keeps the audit trail.");
    console.log(`${RULE}\n`);
  } finally {
    await Promise.allSettled([belief.close(), cursor.close()]);
  }
}

runAsMain(import.meta.url, main);
