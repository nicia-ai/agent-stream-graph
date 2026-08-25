/**
 * Demo — agent stream → justified belief → source retraction with cascading
 * belief revision, built on `@nicia-ai/typegraph/provenance` + this package's
 * durable consumer.
 *
 * Where `examples/agents.ts` projects a flat stream (each change is a direct
 * observation), this demo models a DERIVED belief: facts are grounded by
 * sources through explicit justification nodes. When a bad source is retracted,
 * facts that only it supported go non-current (soft-deleted), while facts with
 * alternate support survive — and the recorded-time history keeps the
 * before/after audit trail.
 *
 *   Source(s) ──premiseOf──▶ Justification ──derives──▶ Fact(s)
 *
 * This demo exercises the provenance API's multi-source-kind, terminal-fact,
 * bulk-retract, and graph-typed-ref surfaces:
 *  - TWO source kinds (`ScannerSource`, `VendorSource`) via `source.kinds`
 *  - a TERMINAL fact (`DeployDecision`) that is never a premise — it need not
 *    appear in `premiseOf.from`, so the schema admits no meaningless edges
 *  - `retractMany` / `unRetractMany` for bulk source retraction
 *  - graph-typed refs: `retract({ kind: "ScannerSource", id })` typechecks
 *    without `as never` casts — `"ScannerSource"` is a valid `NodeKind<G>`
 *  - `asNodeId(id)` brands a plain id for point reads (`getById`), so no
 *    read surface in this demo needs an `as never` cast either
 *
 * Run with:  pnpm demo:provenance
 */
import {
  asNodeId,
  createStoreWithSchema,
  defineEdge,
  defineGraph,
  defineNode,
  type HistoryStore,
} from "@nicia-ai/typegraph";
import {
  createRetractionCapability,
  type ProvenanceFactRef,
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
import { makeBackend, runAsMain } from "./_support";

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

// A TERMINAL fact: derived from a vulnerability, never itself a premise, so it
// does not appear in `premiseOf.from` below.
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
    // DeployDecision is deliberately NOT a premise kind — terminal facts no
    // longer need to be listed here. Sources + non-terminal facts are premises.
    premiseOf: { type: premiseOf, from: [ScannerSource, VendorSource, Vulnerability], to: [Justification] },
    derives: { type: derives, from: [Justification], to: [Vulnerability, DeployDecision] },
  },
});

type SecurityStore = HistoryStore<typeof securityGraph>;

// source.kinds admits both source kinds; the kinds are graph-typed, so a
// typo like "ScanerSource" is a compile error, not a runtime ConfigurationError.
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
  { offset: "001", shape: "finding", key: "scanner-1", operation: "insert", value: { sourceKind: "ScannerSource", sourceId: "scanner-1", sourceLabel: "Unverified scanner #1", vulnId: "vuln-libvector", cve: "CVE-2026-1234", pkg: "libvector 4.2" } },
  { offset: "002", shape: "finding", key: "scanner-2", operation: "insert", value: { sourceKind: "ScannerSource", sourceId: "scanner-2", sourceLabel: "Unverified scanner #2", vulnId: "vuln-libvector", cve: "CVE-2026-1234", pkg: "libvector 4.2" } },
  { offset: "003", shape: "advisory", key: "vendor-1", operation: "insert", value: { sourceKind: "VendorSource", sourceId: "vendor-1", sourceLabel: "Vendor security advisory", vulnId: "vuln-libvector", cve: "CVE-2026-1234", pkg: "libvector 4.2" } },
  { offset: "004", shape: "policy", key: "policy-1", operation: "insert", value: { vulnId: "vuln-libvector", decisionId: "block-deploy", action: "Block the production deploy" } },
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
    // The source kind is data-driven from the stream; narrow to the typed
    // collection by branching on sourceKind rather than indexing dynamically.
    const sourceRef = { kind: value.sourceKind, id: value.sourceId } as const;
    const vulnRef = { kind: "Vulnerability", id: value.vulnId } as const;
    const jId = justificationId(value.sourceId, value.vulnId);
    const jRef = { kind: "Justification", id: jId } as const;

    if (value.sourceKind === "ScannerSource") {
      await tx.nodes.ScannerSource.upsertById(value.sourceId, { label: value.sourceLabel, retracted: false });
    } else {
      await tx.nodes.VendorSource.upsertById(value.sourceId, { label: value.sourceLabel, retracted: false });
    }
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
// Reporting helpers
// ============================================================

function formatRefs(refs: readonly ProvenanceFactRef<typeof securityGraph>[]): string {
  if (refs.length === 0) return "(none)";
  return refs.map((r) => `${r.kind}/${r.id}`).sort().join(", ");
}

function formatReport(report: RetractionReport<typeof securityGraph>): string {
  const survived = report.survivedVia.length === 0
    ? "(none)"
    : report.survivedVia.map((s) => `${s.fact.id} via ${s.via.map((j) => j.id).join(" + ")}`).sort().join("; ");
  return [
    `    died:       ${formatRefs(report.died)}`,
    `    survived:   ${survived}`,
    `    unaffected: ${formatRefs(report.unaffected)}`,
  ].join("\n");
}

async function currentVuln(store: SecurityStore, id: string): Promise<string> {
  const row = await store.nodes.Vulnerability.getById(asNodeId(id));
  return row === undefined ? "(retracted — non-current)" : `${row.cve} on ${row.pkg}`;
}

async function currentDecision(store: SecurityStore, id: string): Promise<string> {
  const row = await store.nodes.DeployDecision.getById(asNodeId(id));
  return row === undefined ? "(retracted — non-current)" : row.action;
}

// ============================================================
// Main
// ============================================================

export async function main(): Promise<void> {
  const rule = "=".repeat(74);
  console.log(rule);
  console.log(" Agent stream → justified belief → source retraction");
  console.log(rule);

  const [belief] = await createStoreWithSchema(securityGraph, await makeBackend(), {
    history: true,
    coalesceUnchangedUpserts: true,
  });
  const [cursor] = await createStoreWithSchema(checkpointGraph, await makeBackend());
  const book = typeGraphCheckpoints(cursor);

  try {
    // --- (a) Durable consumption: stream → provenance graph ----------------
    const source = mockShapeSource<IntelValue>("security-intel", INTEL_CHANGES);
    const result = await consume({ source, store: belief, checkpoints: book, project });
    console.log(`\n  Durable consumer projected ${result.processed} changes into the provenance graph.`);
    console.log(`    cursor at offset: ${await book.lastOffset("security-intel")}`);

    // --- (b) Initial well-founded belief -----------------------------------
    const provenance = createRetractionCapability(belief, retractionConfig);
    console.log(`\n  Initial derived belief (holding()):`);
    console.log(`    ${formatRefs(await provenance.holding())}`);
    console.log(`    vulnerability: ${await currentVuln(belief, "vuln-libvector")}`);
    console.log(`    deploy decision: ${await currentDecision(belief, "block-deploy")}`);

    // --- (c) Bulk-retract both scanner sources at once ----------------------
    // retractMany([scanner-1, scanner-2]) — both unverified scanners retracted
    // in one transition. The vulnerability survives through the vendor advisory;
    // the deploy decision survives because its premise (the vulnerability) is
    // still held. The graph-typed refs need no `as never` cast: "ScannerSource"
    // is a valid NodeKind<typeof securityGraph>.
    const beforeScanners = await belief.recordedNow();
    console.log(`\n  retractMany([scanner-1, scanner-2]) — bulk-retract both unverified scanners.`);
    console.log(`  ${formatReport(await provenance.retractMany([
      { kind: "ScannerSource", id: "scanner-1" },
      { kind: "ScannerSource", id: "scanner-2" },
    ]))}`);
    console.log(`    holding(): ${formatRefs(await provenance.holding())}`);
    console.log(`    vulnerability: ${await currentVuln(belief, "vuln-libvector")}`);
    console.log(`    deploy decision: ${await currentDecision(belief, "block-deploy")}`);

    // --- (d) Retract the vendor advisory too — cascade ----------------------
    // Now the vulnerability loses all support and dies; the deploy decision,
    // whose only premise was that vulnerability, dies with it (terminal fact,
    // not in premiseOf.from — the cascade still reaches it through derives).
    console.log(`\n  retract(vendor-1) — cascade: vulnerability and deploy-decision die.`);
    console.log(`  ${formatReport(await provenance.retract({ kind: "VendorSource", id: "vendor-1" }))}`);
    console.log(`    holding(): ${formatRefs(await provenance.holding())}`);
    console.log(`    vulnerability: ${await currentVuln(belief, "vuln-libvector")}`);
    console.log(`    deploy decision: ${await currentDecision(belief, "block-deploy")}`);
    const afterVendor = await belief.recordedNow();

    // --- (e) Recorded-time replay: the audit trail --------------------------
    // The deploy decision was current before the vendor retraction and
    // non-current after — both states reconstruct from recorded time.
    const decisionBefore = beforeScanners !== undefined
      ? await belief.asOfRecorded(beforeScanners).nodes.DeployDecision.getById(asNodeId("block-deploy"))
      : undefined;
    const decisionAfter = afterVendor !== undefined
      ? await belief.asOfRecorded(afterVendor).nodes.DeployDecision.getById(asNodeId("block-deploy"))
      : undefined;
    console.log(`\n  Recorded-time replay of the deploy decision:`);
    console.log(`    @before scanner retraction: ${decisionBefore === undefined ? "non-current" : decisionBefore.action}`);
    console.log(`    @after vendor retraction:   ${decisionAfter === undefined ? "non-current" : decisionAfter.action}`);

    // Also replay via the durable consumer's offset anchor — the belief at the
    // consumed offset (before any retraction) still shows the vulnerability.
    const anchorAt4 = await book.anchorFor("security-intel", "004");
    if (anchorAt4 !== undefined) {
      const atConsumption = belief.asOfRecorded(anchorAt4);
      const vulnAtConsumption = await atConsumption.nodes.Vulnerability.getById(asNodeId("vuln-libvector"));
      console.log(`    @consumer offset 004:        ${vulnAtConsumption === undefined ? "non-current" : `${vulnAtConsumption.cve} on ${vulnAtConsumption.pkg}`}`);
    }

    // --- (f) Bulk un-retract both scanners — belief reopens -----------------
    // unRetractMany([scanner-1, scanner-2]) — both scanners restored in one
    // transition. The vulnerability reopens via the scanners; the deploy
    // decision reopens because its premise is held again.
    console.log(`\n  unRetractMany([scanner-1, scanner-2]) — bulk-restore both scanners.`);
    console.log(`  ${formatReport(await provenance.unRetractMany([
      { kind: "ScannerSource", id: "scanner-1" },
      { kind: "ScannerSource", id: "scanner-2" },
    ]))}`);
    console.log(`    holding(): ${formatRefs(await provenance.holding())}`);
    console.log(`    vulnerability: ${await currentVuln(belief, "vuln-libvector")}`);
    console.log(`    deploy decision: ${await currentDecision(belief, "block-deploy")}`);

    console.log(`\n${rule}`);
    console.log(" Retraction revises belief currency; recorded time keeps the audit trail.");
    console.log(`${rule}\n`);
  } finally {
    await Promise.allSettled([belief.close(), cursor.close()]);
  }
}

runAsMain(import.meta.url, main);
