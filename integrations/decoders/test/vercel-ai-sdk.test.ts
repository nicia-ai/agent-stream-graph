import { graphEmitter, type GraphEvent, type ShapeChange } from "@nicia-ai/agent-stream-graph";
import type { TextStreamPart, ToolSet } from "ai";
import { describe, expect, it } from "vitest";

import { agentGraph } from "../src/graph.js";
import { decodeVercelAiSdk, type VercelStreamEvent } from "../src/vercel-ai-sdk.js";

const g = graphEmitter(agentGraph);

function change(part: TextStreamPart<ToolSet>, runId = "r1"): ShapeChange<VercelStreamEvent> {
  return { offset: "001", shape: "stream-part", key: "k", operation: "insert", value: { runId, part } };
}

describe("decodeVercelAiSdk", () => {
  it("a tool-call part opens a pending ToolCall under its run, and touches the resource its input names", () => {
    const events = decodeVercelAiSdk(
      change({ type: "tool-call", toolCallId: "c1", toolName: "searchDocs", input: { query: "graph databases" } }),
      g,
    );
    expect(events).toEqual([
      g.nodes.Run.upsert("r1", { source: "vercel-ai-sdk" }),
      g.nodes.ToolCall.upsert("r1:c1", { name: "searchDocs", status: "pending", input: JSON.stringify({ query: "graph databases" }) }),
      g.edges.madeCall.upsert({ kind: "Run", id: "r1" }, { kind: "ToolCall", id: "r1:c1" }),
      g.nodes.Resource.upsert("query:graph databases", { resourceType: "query" }),
      g.edges.touched.upsert({ kind: "ToolCall", id: "r1:c1" }, { kind: "Resource", id: "query:graph databases" }),
    ]);
  });

  it("a tool-result part re-upserts the SAME ToolCall id as complete, carrying output", () => {
    const events = decodeVercelAiSdk(
      change({
        type: "tool-result",
        toolCallId: "c1",
        toolName: "searchDocs",
        input: { query: "graph databases" },
        output: { hits: 3 },
      }),
      g,
    );
    const toolCallEvent = events.find((event) => event.op === "node.upsert" && event.kind === "ToolCall");
    if (toolCallEvent === undefined || toolCallEvent.op !== "node.upsert") throw new Error("no ToolCall upsert found");
    expect(toolCallEvent.id).toBe("r1:c1"); // same id as the tool-call part above — an update, not a new row
    expect(toolCallEvent.props).toEqual({
      name: "searchDocs",
      status: "complete",
      input: JSON.stringify({ query: "graph databases" }),
      output: JSON.stringify({ hits: 3 }),
    });
  });

  it("derives no Resource when the tool input names neither a path nor a query", () => {
    const events = decodeVercelAiSdk(
      change({ type: "tool-call", toolCallId: "c9", toolName: "getClock", input: {} }),
      g,
    );
    expect(events.map((event) => event.op)).toEqual(["node.upsert", "node.upsert", "edge.upsert"]); // Run, ToolCall, madeCall — no Resource, no touched
  });

  it("scopes ToolCall ids by run — the same underlying toolCallId in two runs stays two rows", () => {
    const runA = decodeVercelAiSdk(
      change({ type: "tool-call", toolCallId: "shared-id", toolName: "ping", input: {} }, "run-a"),
      g,
    );
    const runB = decodeVercelAiSdk(
      change({ type: "tool-call", toolCallId: "shared-id", toolName: "ping", input: {} }, "run-b"),
      g,
    );
    function idOf(events: readonly GraphEvent<typeof agentGraph>[]): string | undefined {
      for (const event of events) if (event.op === "node.upsert" && event.kind === "ToolCall") return event.id;
      return undefined;
    }
    expect(idOf(runA)).toBe("run-a:shared-id");
    expect(idOf(runB)).toBe("run-b:shared-id");
    expect(idOf(runA)).not.toBe(idOf(runB));
  });

  it("ignores every stream part that is not a tool call or tool result", () => {
    expect(decodeVercelAiSdk(change({ type: "start" }), g)).toEqual([]);
    expect(decodeVercelAiSdk(change({ type: "abort" }), g)).toEqual([]);
  });
});
