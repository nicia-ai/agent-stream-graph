/**
 * desk/materialize.ts — consume each reporter's stream into its OWN
 * history-enabled belief store, checkpointed.
 *
 * This is `examples/agents.ts` part (b) applied to the newsroom: every
 * reporter gets an independent, bitemporal belief graph. Nobody's belief
 * overwrites anybody else's — ash and brook can (and do) hold contradictory
 * values for the same fact, right up until the editor's desk reconciles them.
 * A shared `CheckpointBook` durably tracks each reporter's own cursor, so a
 * materializer that crashed partway through one reporter's stream resumes
 * exactly where it left off without re-processing another reporter's.
 *
 * Reporters are materialized SEQUENTIALLY on purpose: they share one
 * checkpoint book, and `consume`'s checkpoint writes are per-stream but not
 * mutually exclusive across streams — see the same note in
 * `examples/fork-merge.ts`.
 */
import { consume, graphProjector, type CheckpointBook, type ConsumeResult, type ShapeSource } from "@nicia-ai/agent-stream-graph";

import { REPORTERS, type ReporterId } from "../../fixtures/dispatches.js";
import { type DeskHistoryStore, newStore } from "../backend.js";
import { decodeReporterEvent, type ReporterEvent } from "../decode.js";
import { newsroomGraph, type NewsroomGraph } from "../graph.js";

const project = graphProjector(newsroomGraph, decodeReporterEvent);

export type ReporterMaterialization = Readonly<{
  reporterId: ReporterId;
  belief: DeskHistoryStore<NewsroomGraph>;
  result: ConsumeResult;
}>;

/** Every reporter's materialized belief, keyed by reporter id. */
export type Newsroom = ReadonlyMap<ReporterId, ReporterMaterialization>;

/** Look up a reporter's materialization, or throw — every caller expects all three to be present. */
export function requireMaterialization(newsroom: Newsroom, reporterId: ReporterId): ReporterMaterialization {
  const materialization = newsroom.get(reporterId);
  if (materialization === undefined) {
    throw new Error(`requireMaterialization(${reporterId}): no materialization — was materializeDesk() run first?`);
  }
  return materialization;
}

/**
 * Materialize every reporter's stream into its own history-enabled belief
 * store. `sourceFor` is the one seam between "offline replay" and "live
 * agents" — `desk/run.ts` is the only caller that decides which.
 */
export async function materializeDesk(
  sourceFor: (reporterId: ReporterId) => ShapeSource<ReporterEvent>,
  checkpoints: CheckpointBook,
): Promise<Newsroom> {
  const newsroom = new Map<ReporterId, ReporterMaterialization>();
  for (const reporterId of REPORTERS) {
    const belief = await newStore(newsroomGraph, true);
    const result = await consume({ source: sourceFor(reporterId), store: belief, checkpoints, project });
    if (result.lastOffset === undefined) {
      throw new Error(`materializeDesk(${reporterId}): consumed ${result.processed} changes but checkpointed nothing`);
    }
    newsroom.set(reporterId, { reporterId, belief, result });
  }
  return newsroom;
}

/** Close every reporter's belief store. */
export async function closeNewsroom(newsroom: Newsroom): Promise<void> {
  await Promise.all([...newsroom.values()].map((materialization) => materialization.belief.close()));
}
