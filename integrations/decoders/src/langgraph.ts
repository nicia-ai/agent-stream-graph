/**
 * Decoder — LangGraph → the four-op graph vocabulary.
 *
 * LangGraph is NOT an installed dependency of this package — deliberately:
 * `LangGraphToolEvent` below is typed STRUCTURALLY against LangGraph's own
 * documented `astream_events` v2 event shape (`on_tool_start` / `on_tool_end`,
 * among others) rather than against a real SDK type, which proves the decoder
 * boundary doesn't need the framework installed to be checkable:
 * https://langchain-ai.github.io/langgraph/how-tos/streaming/#stream-multiple-modes
 * If the real package were ever added, only the import above `decodeLangGraph`
 * would change — its body already only touches fields that shape documents.
 */
import type { Decoder } from "@nicia-ai/agent-stream-graph";

import { agentGraph, toolCallEvents } from "./graph.js";

/**
 * The two `astream_events` v2 event names this decoder acts on. LangGraph
 * emits many more (`on_chain_start`, `on_chat_model_stream`, node
 * transitions, …) — out of scope for a decoder about tool calls.
 */
export type LangGraphToolEvent = Readonly<{
  event: "on_tool_start" | "on_tool_end";
  name: string;
  // The id of THIS tool run, per `astream_events` — not the graph invocation.
  run_id: string;
  data: Readonly<{ input?: unknown; output?: unknown }>;
}>;

/**
 * A tool event tagged with the graph-level run id. `astream_events`' own
 * `run_id` is per-runnable (this tool call is its own traced run); the id of
 * the overarching graph invocation lives in `parent_ids`/config, not on the
 * event itself, so the source supplies it the same way `VercelStreamEvent` does.
 */
export type LangGraphEvent = Readonly<{ runId: string; event: LangGraphToolEvent }>;

export const decodeLangGraph: Decoder<typeof agentGraph, LangGraphEvent> = (change, g) => {
  const { runId, event } = change.value;

  return toolCallEvents(g, {
    runId,
    runSource: "langgraph",
    toolCallId: `${runId}:${event.run_id}`,
    name: event.name,
    status: event.event === "on_tool_start" ? "pending" : "complete",
    input: event.data.input,
    ...(event.event === "on_tool_end" ? { output: event.data.output } : {}),
  });
};
