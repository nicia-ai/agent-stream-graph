/**
 * The same scenario as `vercel-ai-sdk.ts` and `generic-jsonl.jsonl` — one
 * run, `searchDocs` then `readFile`, same inputs and outputs — but shaped as
 * LangGraph's `astream_events` v2 stream: an `on_tool_start` followed by an
 * `on_tool_end` per call.
 */
import type { ShapeChange } from "@nicia-ai/agent-stream-graph";

import type { LangGraphEvent } from "../src/langgraph.js";

const RUN_ID = "run-langgraph";

function change(key: string, offset: string, value: LangGraphEvent): ShapeChange<LangGraphEvent> {
  return { offset, shape: "astream-event", key, operation: "insert", value };
}

export const LANGGRAPH_CHANGES: readonly ShapeChange<LangGraphEvent>[] = [
  change("search-start", "001", {
    runId: RUN_ID,
    event: { event: "on_tool_start", name: "searchDocs", run_id: "t1", data: { input: { query: "graph databases" } } },
  }),
  change("search-end", "002", {
    runId: RUN_ID,
    event: {
      event: "on_tool_end",
      name: "searchDocs",
      run_id: "t1",
      data: { input: { query: "graph databases" }, output: { hits: 3 } },
    },
  }),
  change("read-start", "003", {
    runId: RUN_ID,
    event: { event: "on_tool_start", name: "readFile", run_id: "t2", data: { input: { path: "docs/graph.md" } } },
  }),
  change("read-end", "004", {
    runId: RUN_ID,
    event: {
      event: "on_tool_end",
      name: "readFile",
      run_id: "t2",
      data: { input: { path: "docs/graph.md" }, output: { bytes: 812 } },
    },
  }),
];
