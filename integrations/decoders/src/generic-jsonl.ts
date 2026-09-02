/**
 * Decoder — generic newline-delimited JSON tool-call logs → the four-op
 * graph vocabulary.
 *
 * The escape hatch: this is the decoder for a framework that doesn't have
 * one yet, or a hand-rolled agent that just logs what it does. Unlike the
 * other two decoders in this package, `ToolLogLine` needs no run-id envelope
 * — a log line has nowhere else to put its run and call ids, so they are
 * just fields. `parse-jsonl.ts` is the (tiny) other half: turning raw text
 * into `ShapeChange<ToolLogLine>`s this decoder can read.
 */
import type { Decoder } from "@nicia-ai/agent-stream-graph";

import { agentGraph, toolCallEvents } from "./graph.js";

export type ToolLogLine = Readonly<{
  run: string;
  call: string;
  tool: string;
  phase: "start" | "end" | "error";
  args?: unknown;
  result?: unknown;
  error?: string;
}>;

export const decodeGenericJsonl: Decoder<typeof agentGraph, ToolLogLine> = (change, g) => {
  const line = change.value;

  return toolCallEvents(g, {
    runId: line.run,
    runSource: "generic-jsonl",
    toolCallId: `${line.run}:${line.call}`,
    name: line.tool,
    status: line.phase === "start" ? "pending" : line.phase === "end" ? "complete" : "error",
    input: line.args,
    ...(line.phase === "end" ? { output: line.result }
    : line.phase === "error" ? { output: { error: line.error } }
    : {}),
  });
};
