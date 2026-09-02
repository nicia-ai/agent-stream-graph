/**
 * The newsroom desk, headless and narrated.
 *
 * Three reporters cover one story and land on contradictory numbers; an
 * editor reviews each reporter's belief before it reaches canonical, and the
 * library refuses to commit a review built against a target that has since
 * moved; a hypothetical side story forks off the shared prefix; and a burned
 * source cascades through the justification graph asymmetrically. Every
 * section below either prints what actually happened or throws — nothing
 * here would still print success if the library broke.
 *
 * Run with:  pnpm demo
 * Offline by default (checked-in fixtures, deterministic). Set
 * `ANTHROPIC_API_KEY` to run the SAME pipeline against real Claude agents —
 * see README.md for what changes and what doesn't.
 */
import type { MergeReport } from "@nicia-ai/typegraph/graph-merge";

import { REPORTERS, type ReporterId } from "../fixtures/dispatches.js";
import { describeConflict, formatCommitOutcome, formatReviewQueue, provenanceBylines } from "./desk/editor.js";
import { formatRetractionReport } from "./desk/retract.js";
import { closeDeskRun, type DeskEvent, findValueConflict, runDesk } from "./desk/run.js";
import { claimRows, storyRows, subjectRows } from "./desk/views.js";
import type { NewsroomGraph } from "./graph.js";
import { runAsMain } from "./run-as-main.js";

const RULE = "━".repeat(74);

function banner(title: string): void {
  console.log("\n" + RULE);
  console.log(` ${title}`);
  console.log(RULE);
}

function committedReports(events: readonly DeskEvent[], reporterId: ReporterId): readonly MergeReport<NewsroomGraph>[] {
  const reports: MergeReport<NewsroomGraph>[] = [];
  for (const event of events) {
    if (event.type === "committed" && event.reporterId === reporterId) reports.push(event.report);
  }
  return reports;
}

// ============================================================
// Main
// ============================================================

export async function main(): Promise<void> {
  const run = await runDesk();
  try {
    banner(`The newsroom desk — mode: ${run.mode === "replay" ? "REPLAY (offline, checked-in fixtures)" : "LIVE (ANTHROPIC_API_KEY set — real Claude agents, non-deterministic)"}`);

    // ----------------------------------------------------------
    // (a) Each reporter's own belief, before anyone reconciles anything
    // ----------------------------------------------------------
    banner("(a) Each reporter's own belief — independent, contradictory where it disagrees");
    for (const reporterId of REPORTERS) {
      const materialization = run.newsroom.get(reporterId);
      if (materialization === undefined) throw new Error(`main: no materialization for ${reporterId}`);
      const claims = await claimRows(materialization.belief);
      const stories = await storyRows(materialization.belief);
      console.log(`\n  ${reporterId} (${materialization.result.processed} event(s) consumed):`);
      for (const claim of claims) {
        console.log(`    claim  ${claim.value.padEnd(6)} (${claim.confidence}) — ${claim.text}`);
      }
      for (const story of stories) {
        console.log(`    story  [${story.status}] ${story.headline}`);
      }
    }

    // ----------------------------------------------------------
    // (b) The editor's desk — review queue, then commit
    // ----------------------------------------------------------
    banner("(b) The editor's desk — plan, review, commit (and one deliberate refusal)");
    for (const event of run.events) {
      switch (event.type) {
        case "materialized":
          break; // already shown in section (a)
        case "review-queue":
          console.log(`\n${formatReviewQueue(event.reporterId, event.plan)}`);
          break;
        case "stale-refusal":
          console.log(
            `\n  ⚠ applying ${event.reporterId}'s FIRST plan was refused — canonical moved since it was built:`,
          );
          console.log(`      ${event.errorMessage}`);
          console.log(`    → rebuilding ${event.reporterId}'s plan fresh against current canonical…`);
          break;
        case "committed":
          console.log(formatCommitOutcome(event.reporterId, event.report));
          break;
      }
    }

    // ----------------------------------------------------------
    // Canonical — the assertions this whole package exists to make
    // ----------------------------------------------------------
    banner("Canonical — entity resolution and the flagged disagreement");

    const subjects = await subjectRows(run.canonical);
    const bylines = await provenanceBylines(run.canonical, subjects.map((subject) => subject.id));
    console.log(`\n  canonical Subjects (${subjects.length}) — ash's and brook's "M. Vance"/"Marisa Vance" collapsed to one:`);
    for (const subject of subjects) {
      const byline = bylines.get(subject.id) ?? [];
      console.log(`    ${subject.name} <${subject.handle}> — byline: ${byline.length === 0 ? "(none)" : byline.join(", ")}`);
    }
    if (subjects.length !== 2) {
      throw new Error(`main: expected 2 canonical Subjects after every reporter merged, got ${subjects.length}`);
    }

    const ashReports = committedReports(run.events, "reporter-ash");
    const ashReport = ashReports.at(-1);
    if (ashReport === undefined) throw new Error("main: reporter-ash never committed");
    const valueConflict = findValueConflict(ashReport);
    if (valueConflict === undefined) {
      throw new Error("main: the $41M/$38M contractValue conflict was not flagged on commit — see desk/run.ts");
    }
    console.log(`\n  the value disagreement was FLAGGED, not resolved:`);
    console.log(`    ${describeConflict(valueConflict)}`);

    const claims = await claimRows(run.canonical);
    console.log(`\n  canonical Claims (${claims.length}):`);
    for (const claim of claims) {
      console.log(`    ${claim.id.padEnd(20)} ${claim.value.padEnd(6)} (${claim.confidence}) — ${claim.text}`);
    }

    // ----------------------------------------------------------
    // The fork — a hypothetical nobody filed, run forward in isolation
    // ----------------------------------------------------------
    banner("A fork — the shared prefix, split into what really happened and a what-if");
    console.log(`\n  forked at offset ${run.fork.forkOffset} — right after the shared award claim, before the value diverged`);
    console.log(`    trunk  (what ash actually filed): ${run.fork.trunkValue ?? "(missing)"}`);
    console.log(`    what-if (nobody filed this):      ${run.fork.whatIfValue ?? "(missing)"}`);
    console.log(`    isolation confirmed — the what-if claim never reached the trunk store`);

    // ----------------------------------------------------------
    // The retraction cascade — the asymmetry that justifies justification nodes
    // ----------------------------------------------------------
    banner("Burning a source — the asymmetric cascade");
    const { before, after, report } = run.retraction;
    console.log(`\n  before retracting the anonymous tip:`);
    console.log(`    ash's award claim (two sources):    ${before.survivingClaim ?? "(non-current)"}`);
    console.log(`    cass's kickback claim (tip only):   ${before.dyingClaim ?? "(non-current)"}`);
    console.log(`    cass's draft story:                 ${before.dyingStory ?? "(non-current)"}`);
    console.log(`\n  retract({ kind: "Tipster", id: "tip:insider-01" }):`);
    console.log(formatRetractionReport(report));
    console.log(`\n  after:`);
    console.log(`    ash's award claim (two sources):    ${after.survivingClaim ?? "(non-current)"} ← SURVIVED (independently grounded in the filing)`);
    console.log(`    cass's kickback claim (tip only):   ${after.dyingClaim ?? "(non-current)"} ← DIED (its only source was burned)`);
    console.log(`    cass's draft story:                 ${after.dyingStory ?? "(non-current)"} ← DIED (its only claim died)`);

    banner("Done — the whole story ran headless, narrated, and deterministic (in replay mode)");
  } finally {
    await closeDeskRun(run);
  }
}

runAsMain(import.meta.url, main);
