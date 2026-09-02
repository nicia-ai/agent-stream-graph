/**
 * Decoder — Vercel AI SDK (`ai` v7) → the four-op graph vocabulary.
 *
 * Typed against `ai`'s REAL exported `TextStreamPart<ToolSet>` — the union
 * `streamText()`/`generateText()` actually yield on their `fullStream` — so
 * this decoder is proven to compile against the installed package, not a
 * guess at its shape. `TextStreamPart` covers a couple dozen chunk kinds
 * (text deltas, reasoning, step boundaries, …); only the two this decoder
 * cares about are handled, everything else is a no-op.
 */
import type { Decoder } from "@nicia-ai/agent-stream-graph";
import type { TextStreamPart, ToolSet } from "ai";

import { agentGraph, toolCallEvents } from "./graph.js";

/**
 * One `fullStream` chunk, tagged with the run it belongs to. `ai`'s stream is
 * scoped to a single call — it carries a `toolCallId` but no run/session id
 * of its own — so the source wrapping it in a run id is what lets this
 * decoder place many calls under one Run node.
 */
export type VercelStreamEvent = Readonly<{ runId: string; part: TextStreamPart<ToolSet> }>;

export const decodeVercelAiSdk: Decoder<typeof agentGraph, VercelStreamEvent> = (change, g) => {
  const { runId, part } = change.value;
  if (part.type !== "tool-call" && part.type !== "tool-result") return [];

  return toolCallEvents(g, {
    runId,
    runSource: "vercel-ai-sdk",
    // Run-scoped: two runs may reuse the same `toolCallId` under the hood.
    toolCallId: `${runId}:${part.toolCallId}`,
    name: part.toolName,
    status: part.type === "tool-call" ? "pending" : "complete",
    input: part.input,
    ...(part.type === "tool-result" ? { output: part.output } : {}),
  });
};
