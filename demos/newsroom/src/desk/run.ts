/**
 * desk/run.ts — the whole newsroom story, assembled in order, as data.
 *
 * `src/main.ts` (console) and `src/server.ts` (HTTP/SSE) are two different
 * PRESENTATIONS of the exact same run: both call {@link runDesk} once and
 * format what it returns for their own medium. Nothing about "what happens,
 * in what order" lives in either presentation layer — it lives here, so the
 * two can never narrate a different story than the one that actually ran.
 *
 * The sequence:
 *   1. materialize every reporter's stream into its own belief (`materialize.ts`)
 *   2. the editor's desk (`editor.ts`) reviews and commits each reporter's
 *      belief into canonical — INCLUDING the deliberate stale-plan refusal:
 *      ash's plan is built, brook's belief commits first (advancing
 *      canonical), and only then is ash's now-stale plan applied — and
 *      refused, before a freshly-rebuilt one commits for real.
 *   3. the fork/what-if side story (`fork.ts`)
 *   4. the retraction cascade and its asymmetry assertion (`retract.ts`)
 *
 * `events` is the flattened, presentation-agnostic account of step 1–2 — the
 * "recorded-time timeline" `server.ts` exposes and `main.ts` narrates from.
 */
import { checkpointGraph, typeGraphCheckpoints, type CheckpointBook, type ShapeSource } from "@nicia-ai/agent-stream-graph";
import { asNodeId } from "@nicia-ai/typegraph";
import { isErr, StaleMergePlanError, type MergePlanArtifact, type MergeReport, type PropertyConflict } from "@nicia-ai/typegraph/graph-merge";

import { REPORTERS, type ReporterId } from "../../fixtures/dispatches.js";
import { type DeskHistoryStore, type DeskStore, newStore } from "../backend.js";
import type { ReporterEvent } from "../decode.js";
import { newsroomGraph, type NewsroomGraph } from "../graph.js";
import { replaySource } from "../reporters/replay.js";
import { applyMergePlan, buildReviewPlan, commitReviewedPlan } from "./editor.js";
import { forkAtDivergence, type ForkResult } from "./fork.js";
import { closeNewsroom, materializeDesk, requireMaterialization, type Newsroom } from "./materialize.js";
import { retractBurnedSource, type RetractionOutcome } from "./retract.js";

export type DeskMode = "replay" | "live";

/** `live` only when `ANTHROPIC_API_KEY` is set — the default is always offline. */
export function detectMode(): DeskMode {
  return process.env.ANTHROPIC_API_KEY === undefined ? "replay" : "live";
}

async function sourceFactory(mode: DeskMode): Promise<(reporterId: ReporterId) => ShapeSource<ReporterEvent>> {
  if (mode === "replay") return replaySource;
  // Loaded only in live mode, so an offline run never touches the Agent SDK.
  const live = await import("../reporters/live.js");
  return live.liveSource;
}

export type DeskEvent =
  | Readonly<{ type: "materialized"; reporterId: ReporterId; processed: number; lastOffset: string }>
  | Readonly<{ type: "review-queue"; reporterId: ReporterId; plan: MergePlanArtifact }>
  | Readonly<{ type: "stale-refusal"; reporterId: ReporterId; errorMessage: string }>
  | Readonly<{ type: "committed"; reporterId: ReporterId; report: MergeReport<NewsroomGraph> }>;

export type DeskRun = Readonly<{
  mode: DeskMode;
  newsroom: Newsroom;
  canonical: DeskHistoryStore<NewsroomGraph>;
  forkPoint: DeskStore<NewsroomGraph>;
  cursor: DeskStore<typeof checkpointGraph>;
  checkpoints: CheckpointBook;
  events: readonly DeskEvent[];
  fork: ForkResult;
  retraction: RetractionOutcome;
}>;

/**
 * Locate the flagged $41M/$38M conflict on `claim-value.value` — the demo's
 * central disagreement. Exported so `main.ts` can describe the exact same
 * conflict object this module already validated, rather than re-deriving the
 * search predicate in the presentation layer.
 */
export function findValueConflict(report: MergeReport<NewsroomGraph>): PropertyConflict<NewsroomGraph> | undefined {
  return report.conflicts.find((conflict) => conflict.property === "value" && conflict.entityId === asNodeId("claim-value"));
}

export async function runDesk(): Promise<DeskRun> {
  const mode = detectMode();
  const sourceFor = await sourceFactory(mode);

  const cursor = await newStore(checkpointGraph, false);
  const checkpoints = typeGraphCheckpoints(cursor);

  // ---- 1. Materialize every reporter's own belief ----
  const newsroom = await materializeDesk(sourceFor, checkpoints);
  const events: DeskEvent[] = [];
  for (const reporterId of REPORTERS) {
    const { result } = requireMaterialization(newsroom, reporterId);
    if (result.lastOffset === undefined) {
      throw new Error(`runDesk: ${reporterId} materialized with no checkpointed offset`);
    }
    events.push({ type: "materialized", reporterId, processed: result.processed, lastOffset: result.lastOffset });
  }

  // ---- 2. The editor's desk ----
  const forkPoint = await newStore(newsroomGraph, false);
  const canonical = await newStore(newsroomGraph, true);

  // Build ash's plan FIRST, but do not commit it yet.
  const staleAshPlan = await buildReviewPlan(forkPoint, canonical, "reporter-ash", requireMaterialization(newsroom, "reporter-ash").belief);
  events.push({ type: "review-queue", reporterId: "reporter-ash", plan: staleAshPlan });

  // brook's review-and-commit lands first, advancing canonical out from under ash's plan.
  const brookPlan = await buildReviewPlan(forkPoint, canonical, "reporter-brook", requireMaterialization(newsroom, "reporter-brook").belief);
  events.push({ type: "review-queue", reporterId: "reporter-brook", plan: brookPlan });
  const brookReport = await commitReviewedPlan(canonical, brookPlan);
  events.push({ type: "committed", reporterId: "reporter-brook", report: brookReport });

  // Applying the now-stale ash plan MUST be refused — canonical moved since it was built.
  const staleAttempt = await applyMergePlan(canonical, staleAshPlan);
  if (!isErr(staleAttempt)) {
    throw new Error(
      "runDesk: applyMergePlan committed a plan built against a target that had since moved — it should have " +
        "refused (StaleMergePlanError).",
    );
  }
  if (!(staleAttempt.error instanceof StaleMergePlanError)) {
    throw new Error(
      `runDesk: applyMergePlan refused ash's stale plan, but with the wrong error — expected StaleMergePlanError, ` +
        `got ${staleAttempt.error.constructor.name}: ${staleAttempt.error.message}`,
    );
  }
  events.push({ type: "stale-refusal", reporterId: "reporter-ash", errorMessage: staleAttempt.error.message });

  // Rebuild ash's plan fresh against current canonical, and commit it for real.
  const freshAshPlan = await buildReviewPlan(forkPoint, canonical, "reporter-ash", requireMaterialization(newsroom, "reporter-ash").belief);
  events.push({ type: "review-queue", reporterId: "reporter-ash", plan: freshAshPlan });
  const ashReport = await commitReviewedPlan(canonical, freshAshPlan);
  events.push({ type: "committed", reporterId: "reporter-ash", report: ashReport });

  // ash and brook both wrote a claim literally ID'd "claim-value" — the point
  // of the fixture — so merging ash's belief in must have flagged, not
  // silently resolved, the $41M vs $38M disagreement.
  const valueConflict = findValueConflict(ashReport);
  if (valueConflict === undefined) {
    throw new Error(
      "runDesk: merging ash's belief did not flag a conflict on claim-value.value — the $41M/$38M disagreement " +
        "should have been FLAGGED, not silently resolved.",
    );
  }
  // brook committed first, so canonical's $38M is what `onBasePropertyConflict:
  // "flag"` KEEPS (the conflict's `resolution`); ash's differing $41M is what
  // gets FLAGGED (recorded in `values`) rather than silently discarded.
  const flaggedValues = new Set(valueConflict.values.map((entry) => entry.value));
  if (valueConflict.resolution !== "$38M" || !flaggedValues.has("$41M")) {
    throw new Error(
      `runDesk: the claim-value conflict did not preserve both filed figures — resolution=${JSON.stringify(valueConflict.resolution)}, ` +
        `values=${JSON.stringify(valueConflict.values)}`,
    );
  }

  // cass last.
  const cassPlan = await buildReviewPlan(forkPoint, canonical, "reporter-cass", requireMaterialization(newsroom, "reporter-cass").belief);
  events.push({ type: "review-queue", reporterId: "reporter-cass", plan: cassPlan });
  const cassReport = await commitReviewedPlan(canonical, cassPlan);
  events.push({ type: "committed", reporterId: "reporter-cass", report: cassReport });

  // Every reporter has now merged. ash's "M. Vance" (@mvance) and brook's
  // "Marisa Vance" (@MVance) must have collapsed to ONE canonical Subject —
  // the case-insensitive handle-uniqueness constraint `graph.ts` declares —
  // alongside the one Halcyon vendor Subject all three reporters share.
  const subjects = await canonical.query().from("Subject", "s").select((c) => ({ id: c.s.id, handle: c.s.handle })).execute();
  if (subjects.length !== 2) {
    throw new Error(
      `runDesk: expected exactly 2 canonical Subjects (Halcyon + the collapsed Vance identity), got ${subjects.length}: ` +
        JSON.stringify(subjects),
    );
  }

  // ---- 3. Fork / what-if side story ----
  const fork = await forkAtDivergence();

  // ---- 4. Retraction cascade ----
  const retraction = await retractBurnedSource(canonical);

  return { mode, newsroom, canonical, forkPoint, cursor, checkpoints, events, fork, retraction };
}

export async function closeDeskRun(run: DeskRun): Promise<void> {
  await Promise.all([
    closeNewsroom(run.newsroom),
    run.canonical.close(),
    run.forkPoint.close(),
    run.cursor.close(),
    run.fork.trunk.close(),
    run.fork.whatIf.close(),
  ]);
}
