/**
 * Turn a newline-delimited JSON tool-call log into the `ShapeChange`s
 * `decodeGenericJsonl` reads. This is the whole adapter a real integration
 * needs for this source: one line of text in, one `ShapeChange` out — the
 * decoder itself never sees a string.
 */
import type { ShapeChange } from "@nicia-ai/agent-stream-graph";

import type { ToolLogLine } from "./generic-jsonl.js";

export function parseToolLog(text: string): readonly ShapeChange<ToolLogLine>[] {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line, i) => {
      const value = JSON.parse(line) as ToolLogLine;
      return {
        offset: String(i + 1).padStart(3, "0"),
        shape: "tool-log",
        key: `${value.run}:${value.call}:${value.phase}`,
        operation: "insert",
        value,
      } satisfies ShapeChange<ToolLogLine>;
    });
}
