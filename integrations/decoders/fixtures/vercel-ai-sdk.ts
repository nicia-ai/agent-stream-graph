/**
 * A recorded-looking `fullStream` from `streamText()`: one run, two tool
 * calls (`searchDocs`, then `readFile`), each as a tool-call chunk followed
 * by its tool-result chunk. `demo.ts` decodes this alongside `langgraph.ts`
 * and `generic-jsonl.jsonl` — same run, same two tools, same inputs and
 * outputs, so the three decoded graphs can be checked for convergence.
 */
import type { ShapeChange } from "@nicia-ai/agent-stream-graph";

import type { VercelStreamEvent } from "../src/vercel-ai-sdk.js";

const RUN_ID = "run-vercel";

function change(key: string, offset: string, value: VercelStreamEvent): ShapeChange<VercelStreamEvent> {
  return { offset, shape: "stream-part", key, operation: "insert", value };
}

export const VERCEL_CHANGES: readonly ShapeChange<VercelStreamEvent>[] = [
  change("search-call", "001", {
    runId: RUN_ID,
    part: { type: "tool-call", toolCallId: "call-1", toolName: "searchDocs", input: { query: "graph databases" } },
  }),
  change("search-result", "002", {
    runId: RUN_ID,
    part: {
      type: "tool-result",
      toolCallId: "call-1",
      toolName: "searchDocs",
      input: { query: "graph databases" },
      output: { hits: 3 },
    },
  }),
  change("read-call", "003", {
    runId: RUN_ID,
    part: { type: "tool-call", toolCallId: "call-2", toolName: "readFile", input: { path: "docs/graph.md" } },
  }),
  change("read-result", "004", {
    runId: RUN_ID,
    part: {
      type: "tool-result",
      toolCallId: "call-2",
      toolName: "readFile",
      input: { path: "docs/graph.md" },
      output: { bytes: 812 },
    },
  }),
];
