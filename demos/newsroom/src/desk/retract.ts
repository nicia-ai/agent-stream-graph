/**
 * desk/retract.ts — burn a source, cascade the belief revision, and prove the
 * cascade is DISCRIMINATING rather than blunt.
 *
 * `graph.ts` has no `retracted` flag on a claim, on purpose: whether a claim
 * survives is DERIVED from the justification graph
 * (`@nicia-ai/typegraph/provenance`'s `createRetractionCapability`), not
 * stored redundantly. Retracting a source is a write to the SOURCE row; what
 * happens to the claims that cite it falls out of the graph.
 *
 * `fixtures/dispatches.ts`'s `BURNED_SOURCE` is the anonymous tip both ash
 * and cass cited. ash's claim also rests on the budget filing, independently
 * — TWO justifications, one per source (`decode.ts`'s `ground()` gives every
 * premise its own). cass's claim rests on the tip ALONE — one justification.
 * Burning the tip should therefore kill cass's claim (and the draft story
 * that rests on it) while leaving ash's claim standing. That asymmetry is
 * the entire argument for justification nodes over a flat `sources: string[]`
 * — this module doesn't just narrate it, it ASSERTS it: a claim that moves
 * the wrong way throws rather than prints a misleading "success".
 */
import { asNodeId } from "@nicia-ai/typegraph";
import { createRetractionCapability, type ProvenanceFactRef, type RetractionReport } from "@nicia-ai/typegraph/provenance";

import { BURNED_SOURCE } from "../../fixtures/dispatches.js";
import type { DeskHistoryStore } from "../backend.js";
import { type NewsroomGraph, retractionConfig } from "../graph.js";

const SURVIVING_CLAIM = "claim-award-ash";
const DYING_CLAIM = "claim-kickback";
const DYING_STORY = "story-kickback";

export type CurrentState = Readonly<{
  survivingClaim: string | undefined;
  dyingClaim: string | undefined;
  dyingStory: string | undefined;
}>;

export type RetractionOutcome = Readonly<{
  before: CurrentState;
  report: RetractionReport<NewsroomGraph>;
  after: CurrentState;
  beforeRecordedRevision: string | undefined;
  afterRecordedRevision: string | undefined;
}>;

async function readCurrentState(canonical: DeskHistoryStore<NewsroomGraph>): Promise<CurrentState> {
  const [award, kickback, story] = await Promise.all([
    canonical.nodes.Claim.getById(asNodeId(SURVIVING_CLAIM)),
    canonical.nodes.Claim.getById(asNodeId(DYING_CLAIM)),
    canonical.nodes.Story.getById(asNodeId(DYING_STORY)),
  ]);
  return { survivingClaim: award?.value, dyingClaim: kickback?.value, dyingStory: story?.headline };
}

function refIn(refs: readonly ProvenanceFactRef<NewsroomGraph>[], kind: string, id: string): boolean {
  return refs.some((ref) => ref.kind === kind && ref.id === id);
}

/**
 * Burn `BURNED_SOURCE` in `canonical` and assert the asymmetric cascade.
 * Throws — loudly, not a console warning — if either half of the assertion
 * fails: that is the one invariant this whole package exists to demonstrate.
 */
export async function retractBurnedSource(canonical: DeskHistoryStore<NewsroomGraph>): Promise<RetractionOutcome> {
  const provenance = createRetractionCapability(canonical, retractionConfig);

  const before = await readCurrentState(canonical);
  const beforeRecordedRevision = await canonical.recordedNow();

  const report = await provenance.retract(BURNED_SOURCE);

  const after = await readCurrentState(canonical);
  const afterRecordedRevision = await canonical.recordedNow();

  if (after.survivingClaim === undefined) {
    throw new Error(
      `retractBurnedSource: ${SURVIVING_CLAIM} went non-current, but it is independently grounded in the budget ` +
        `filing and should have survived the tip's retraction — justification nodes are not doing their job.`,
    );
  }
  if (after.dyingClaim !== undefined) {
    throw new Error(
      `retractBurnedSource: ${DYING_CLAIM} is still current, but its ONLY source was the burned tip — it should ` +
        `have gone non-current.`,
    );
  }
  if (after.dyingStory !== undefined) {
    throw new Error(
      `retractBurnedSource: ${DYING_STORY} is still current, but its only claim (${DYING_CLAIM}) just died — the ` +
        `cascade should have reached it too.`,
    );
  }
  if (!refIn(report.died, "Claim", DYING_CLAIM) || !refIn(report.died, "Story", DYING_STORY)) {
    throw new Error(
      `retractBurnedSource: RetractionReport.died did not name both ${DYING_CLAIM} and ${DYING_STORY} — got ` +
        `${JSON.stringify(report.died)}`,
    );
  }
  if (refIn(report.died, "Claim", SURVIVING_CLAIM)) {
    throw new Error(`retractBurnedSource: RetractionReport.died wrongly names ${SURVIVING_CLAIM}`);
  }

  return { before, report, after, beforeRecordedRevision, afterRecordedRevision };
}

// ============================================================
// Formatting
// ============================================================

function formatRefs(refs: readonly ProvenanceFactRef<NewsroomGraph>[]): string {
  return refs.length === 0 ? "(none)" : refs.map((ref) => `${ref.kind}/${ref.id}`).sort().join(", ");
}

export function formatRetractionReport(report: RetractionReport<NewsroomGraph>): string {
  const survived = report.survivedVia.length === 0
    ? "(none)"
    : report.survivedVia.map((s) => `${s.fact.id} via ${s.via.map((j) => j.id).join(" + ")}`).sort().join("; ");
  return [
    `    died:       ${formatRefs(report.died)}`,
    `    survived:   ${survived}`,
    `    unaffected: ${formatRefs(report.unaffected)}`,
  ].join("\n");
}
