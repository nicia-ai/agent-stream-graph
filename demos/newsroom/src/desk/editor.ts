/**
 * desk/editor.ts — the editor's desk.
 *
 * A reporter's belief is untrusted input: it may repeat what canonical
 * already knows under a different id (that's what entity resolution is for),
 * and it may flatly contradict another reporter. Nobody's belief should land
 * in canonical unreviewed, so this module splits the merge the rest of the
 * library treats as one call into the two steps an editor actually performs:
 *
 *   1. {@link buildReviewPlan} stages the reporter's belief into an
 *      `ingestionBranch` and asks `planMergeIncremental` what committing it
 *      WOULD do — a durable, JSON-serializable `MergePlanArtifact` — without
 *      touching canonical. Nothing is written yet.
 *   2. {@link commitReviewedPlan} applies that exact plan with
 *      `applyMergePlan`, which re-checks the plan's digest and the target
 *      fence it was built against before writing anything. If canonical
 *      moved since the plan was built — another reporter's belief landed
 *      first, say — the apply is REFUSED rather than silently committing a
 *      stale decision. `desk/run.ts` demonstrates exactly that refusal: it
 *      builds ash's plan, lets brook's commit land first, and only then
 *      tries to apply the now-stale ash plan.
 *
 * `formatReviewQueue` is what makes step 1 legible to a human before step 2
 * happens — the review beat the rest of this package is built to show off.
 *
 * The branch is named after the REPORTER (`asBranchId(reporterId)`), not a
 * generic id — see `graph.ts`'s header comment: the branch IS the byline.
 * `provenanceBylines` reads that back out through `readProvenance`, so "who
 * said this first" is answered from merge provenance, never from an edge
 * this package would otherwise have to maintain.
 */
import {
  asBranchId,
  ingestionBranch,
  isOk,
  type MergePlanArtifact,
  openProvenanceStore,
  planMergeIncremental,
  applyMergePlan as applyMergePlanRaw,
  type MergeReport,
  type PropertyConflict,
  readProvenance,
  unwrap,
} from "@nicia-ai/typegraph/graph-merge";
import { exportGraphStream, importGraphStream } from "@nicia-ai/typegraph/interchange";
import type { JsonValue } from "@nicia-ai/typegraph";

import type { ReporterId } from "../../fixtures/dispatches.js";
import { makeBackend, type DeskHistoryStore, type DeskStore } from "../backend.js";
import { newsroomGraph, type NewsroomGraph } from "../graph.js";

// ============================================================
// Step 1 — build a reviewable plan. Nothing is written to canonical.
// ============================================================

/**
 * Stage `belief` into an ingestion branch named after `reporterId` and ask
 * what merging it into `canonical` WOULD do. `forkPoint` must be the same
 * empty, immutable store across every call for the life of the desk — it is
 * the diff reference `mergeIncremental`'s precondition re-verifies inside the
 * eventual commit, not a place anything is written.
 *
 * The branch is closed before returning: a `MergePlanArtifact` is a
 * self-contained, JSON-serializable write set, so nothing about reviewing or
 * later applying it needs the branch's backend to stay open.
 */
export async function buildReviewPlan(
  forkPoint: DeskStore<NewsroomGraph>,
  canonical: DeskHistoryStore<NewsroomGraph>,
  reporterId: ReporterId,
  belief: DeskHistoryStore<NewsroomGraph>,
): Promise<MergePlanArtifact> {
  const branchId = asBranchId(reporterId);
  // An INGESTION branch, not a plain one: a reporter's belief routinely
  // ALIASES a Subject canonical already holds (same handle, a different id —
  // exactly what `graph.ts`'s `subject_handle` uniqueness constraint exists
  // to collapse), and a plain branch would enforce that constraint at staging
  // time, before the merge ever got to propose the alias as a match.
  const staged = unwrap(await ingestionBranch(forkPoint, makeBackend, { id: branchId }));
  try {
    await importGraphStream(staged, exportGraphStream(belief, { includeTemporal: true }), { onConflict: "update" });

    const planResult = await planMergeIncremental({
      forkPoint,
      target: canonical,
      branches: [staged],
      options: {
        resolve: { Subject: { similarity: { kind: "fulltext", fields: ["name"] }, threshold: 0.9 } },
        onPropertyConflict: "flag",
        onBasePropertyConflict: "flag",
        branchOrder: [branchId],
        persistProvenance: true,
      },
    });
    if (!isOk(planResult)) throw planResult.error;
    return planResult.data;
  } finally {
    await staged.close();
  }
}

// ============================================================
// Step 2 — commit an already-reviewed plan.
// ============================================================

/**
 * Apply a plan an editor has already reviewed. Throws on refusal — the
 * ordinary path, once a plan is trusted. `desk/run.ts` calls
 * `applyMergePlan` directly (re-exported below) for the ONE step where a
 * refusal is the expected, asserted outcome.
 */
export async function commitReviewedPlan(
  canonical: DeskHistoryStore<NewsroomGraph>,
  plan: MergePlanArtifact,
): Promise<MergeReport<NewsroomGraph>> {
  const result = await applyMergePlanRaw(canonical, plan);
  if (!isOk(result)) throw result.error;
  return result.data;
}

/** Re-exported so `desk/run.ts` can attempt an apply it EXPECTS to be refused, without a try/catch. */
export const applyMergePlan = applyMergePlanRaw;

// ============================================================
// Formatting the review queue — legible before anything commits.
// ============================================================

type ReviewedConflict = Readonly<{
  kind: string;
  property: string;
  entityId: string;
  resolution: JsonValue;
  values: readonly Readonly<{ branchId: string; value: JsonValue }>[];
}>;

// A hand-written predicate, not chained `typeof`/`Array.isArray` narrowing: TS
// cannot narrow a `readonly JsonValue[]` union member away via `Array.isArray`
// (its predicate is `arg is any[]`, and a readonly array isn't assignable to
// that), so the exclusion has to be stated explicitly here instead.
function isJsonRecord(value: JsonValue): value is Readonly<Record<string, JsonValue>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asRecord(value: JsonValue): Readonly<Record<string, JsonValue>> | undefined {
  return isJsonRecord(value) ? value : undefined;
}

function asConflictValues(value: JsonValue | undefined): readonly Readonly<{ branchId: string; value: JsonValue }>[] {
  if (!Array.isArray(value)) return [];
  const entries: Readonly<{ branchId: string; value: JsonValue }>[] = [];
  for (const entry of value) {
    const record = asRecord(entry);
    const branchId = record?.branchId;
    if (record !== undefined && typeof branchId === "string") {
      entries.push({ branchId, value: record.value ?? null });
    }
  }
  return entries;
}

/** Best-effort structural read of one review-queue conflict — see `MergePlanReview.conflicts`'s `JsonValue` type. */
function asReviewedConflict(value: JsonValue): ReviewedConflict | undefined {
  const record = asRecord(value);
  if (record === undefined) return undefined;
  const { kind, property, entityId, resolution, values } = record;
  if (typeof kind !== "string" || typeof property !== "string") return undefined;
  return {
    kind,
    property,
    entityId: typeof entityId === "string" ? entityId : JSON.stringify(entityId ?? null),
    resolution: resolution ?? null,
    values: asConflictValues(values),
  };
}

function formatJson(value: JsonValue): string {
  return typeof value === "string" ? `"${value}"` : JSON.stringify(value);
}

function formatConflict(conflict: ReviewedConflict): string {
  const values = conflict.values.length === 0
    ? ""
    : ` (${conflict.values.map((entry) => `${entry.branchId}=${formatJson(entry.value)}`).join(" vs. ")})`;
  return `      FLAGGED  ${conflict.kind}.${conflict.property} on ${conflict.entityId}: kept ${formatJson(conflict.resolution)}${values}`;
}

/**
 * The editor's review queue: a plain-text rendering of a `MergePlanArtifact`
 * — proposed write counts, the conflicts flagged rather than silently
 * resolved, and the plan's own digest, which is exactly what
 * `applyMergePlan` re-checks before committing.
 */
export function formatReviewQueue(reporterId: ReporterId, plan: MergePlanArtifact): string {
  const lines: string[] = [];
  lines.push(`  ── review queue: ${reporterId} → canonical (plan ${plan.digest.value.slice(0, 12)}…) ──`);
  lines.push(
    `    target: ${plan.target.graphId} @ revision ${plan.target.revision.revision ?? "(none yet)"} (mode: ${plan.mode})`,
  );
  lines.push(
    `    proposed: ${plan.proposed.nodes.upserts} node upsert(s), ${plan.proposed.nodes.deletions} node deletion(s), ` +
      `${plan.proposed.edges.upserts} edge upsert(s), ${plan.proposed.edges.deletions} edge deletion(s)`,
  );
  if (plan.review.conflicts.length === 0) {
    lines.push("    conflicts: none");
  } else {
    lines.push(`    conflicts: ${plan.review.conflicts.length} flagged for editorial review`);
    for (const raw of plan.review.conflicts) {
      const conflict = asReviewedConflict(raw);
      lines.push(conflict === undefined ? `      (unrecognised conflict shape: ${JSON.stringify(raw)})` : formatConflict(conflict));
    }
  }
  for (const warning of plan.review.warnings) {
    lines.push(`    warning: ${warning}`);
  }
  return lines.join("\n");
}

/** One line per merged/flagged/dropped count — the after-the-fact summary of a commit. */
export function formatCommitOutcome(reporterId: ReporterId, report: MergeReport<NewsroomGraph>): string {
  return (
    `  committed ${reporterId}: ${report.merged.nodes} node(s), ${report.merged.edges} edge(s) written; ` +
    `${report.conflicts.length} conflict(s) on record; ${report.resolutions.length} entity resolution(s)`
  );
}

/** Reusable formatter for a typed `PropertyConflict` off a committed `MergeReport` (not the plan's `JsonValue` form). */
export function describeConflict(conflict: PropertyConflict<NewsroomGraph>): string {
  const values = conflict.values.map((entry) => `${entry.branchId}=${formatJson(entry.value)}`).join(" vs. ");
  return `${conflict.kind}.${conflict.property} on ${conflict.entityId}: ${values}; kept ${formatJson(conflict.resolution)}`;
}

// ============================================================
// "The branch IS the byline" — provenance-derived attribution.
// ============================================================

/**
 * Which reporters' branches contributed to each of `canonicalIds`, keyed by
 * canonical id. Branch ids ARE reporter ids — except the library's own
 * internal `__committed_…__` sentinel, which marks "this row already existed
 * in canonical before this merge" rather than naming a reporter, and is
 * filtered out here: it is not a byline.
 *
 * `openProvenanceStore` does real I/O (a sidecar ownership preflight, and on
 * first use a schema-write transaction) — opened ONCE here and shared across
 * every id, rather than once per caller-side lookup.
 */
export async function provenanceBylines(
  canonical: DeskHistoryStore<NewsroomGraph>,
  canonicalIds: readonly string[],
): Promise<ReadonlyMap<string, readonly string[]>> {
  const provenanceStore = await openProvenanceStore(canonical);
  const entries = await Promise.all(
    canonicalIds.map(async (canonicalId) => {
      const rows = await readProvenance(provenanceStore, { canonicalId });
      const branchIds = rows.map((row) => row.branchId).filter((branchId) => !branchId.startsWith("__"));
      return [canonicalId, [...new Set(branchIds)]] as const;
    }),
  );
  return new Map(entries);
}
