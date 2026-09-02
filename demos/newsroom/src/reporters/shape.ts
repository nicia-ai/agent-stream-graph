/**
 * Turn one reporter's ordered events into the `ShapeChange<ReporterEvent>[]`
 * `mockShapeSource` expects. Shared by `replay.ts` (checked-in fixtures) and
 * `live.ts` (real agent output) so both sources number and key events
 * identically — the only thing that differs between them is where the
 * `ReporterEvent[]` itself comes from.
 */
import type { ShapeChange } from "@nicia-ai/agent-stream-graph";

import type { ReporterEvent } from "../decode.js";

/** Zero-padded, 1-based offsets — "001", "002", … — matching the house style. */
function offsetOf(index: number): string {
  return String(index + 1).padStart(3, "0");
}

export function toReporterEventChanges(events: readonly ReporterEvent[]): readonly ShapeChange<ReporterEvent>[] {
  return events.map((event, index) => ({
    offset: offsetOf(index),
    shape: "reporter-event",
    key: event.id,
    operation: "insert",
    value: event,
  }));
}
