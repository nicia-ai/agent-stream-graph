/**
 * Replay reporter transcripts — the newsroom's default, offline `ShapeSource`.
 *
 * Wraps the checked-in fixtures in `mockShapeSource`, numbering each
 * reporter's own events as its own append-only stream. `decode.ts`'s decoder
 * never looks at `operation` — every event streams as an "insert". A reporter
 * never retracts its OWN claim through this channel; retraction is the desk's
 * act on a SOURCE (see `desk/retract.ts`), not a reporter event.
 *
 * `reporters/live.ts` builds a `ShapeSource<ReporterEvent>` of the identical
 * shape from real Claude agents — everything downstream (the belief stores,
 * the editor's review, the fork, the retraction cascade) is unchanged either
 * way, because the only thing that differs is which source `desk/run.ts`
 * hands to `consume`.
 */
import { mockShapeSource, type ShapeSource } from "@nicia-ai/agent-stream-graph";

import { type ReporterId, TRANSCRIPTS } from "../../fixtures/dispatches.js";
import type { ReporterEvent } from "../decode.js";
import { toReporterEventChanges } from "./shape.js";

/** The recorded transcript for one reporter, replayed as a durable shape stream. */
export function replaySource(reporterId: ReporterId): ShapeSource<ReporterEvent> {
  return mockShapeSource(reporterId, toReporterEventChanges(TRANSCRIPTS[reporterId]));
}
