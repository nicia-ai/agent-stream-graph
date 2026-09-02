import { graphEmitter, type ShapeChange } from "@nicia-ai/agent-stream-graph";
import { describe, expect, it } from "vitest";

import { agentGraph } from "../src/graph.js";
import { decodeLangGraph, type LangGraphEvent, type LangGraphToolEvent } from "../src/langgraph.js";

const g = graphEmitter(agentGraph);

function change(event: LangGraphToolEvent, runId = "r1"): ShapeChange<LangGraphEvent> {
  return { offset: "001", shape: "astream-event", key: "k", operation: "insert", value: { runId, event } };
}

describe("decodeLangGraph", () => {
  it("on_tool_start opens a pending ToolCall keyed by the tool's OWN run_id, scoped under the graph run", () => {
    const events = decodeLangGraph(
      change({ event: "on_tool_start", name: "readFile", run_id: "t2", data: { input: { path: "docs/graph.md" } } }),
      g,
    );
    expect(events).toEqual([
      g.nodes.Run.upsert("r1", { source: "langgraph" }),
      g.nodes.ToolCall.upsert("r1:t2", { name: "readFile", status: "pending", input: JSON.stringify({ path: "docs/graph.md" }) }),
      g.edges.madeCall.upsert({ kind: "Run", id: "r1" }, { kind: "ToolCall", id: "r1:t2" }),
      g.nodes.Resource.upsert("file:docs/graph.md", { resourceType: "file" }),
      g.edges.touched.upsert({ kind: "ToolCall", id: "r1:t2" }, { kind: "Resource", id: "file:docs/graph.md" }),
    ]);
  });

  it("on_tool_end re-upserts the SAME ToolCall id as complete, carrying output", () => {
    const events = decodeLangGraph(
      change({
        event: "on_tool_end",
        name: "readFile",
        run_id: "t2",
        data: { input: { path: "docs/graph.md" }, output: { bytes: 812 } },
      }),
      g,
    );
    const toolCallEvent = events.find((event) => event.op === "node.upsert" && event.kind === "ToolCall");
    if (toolCallEvent === undefined || toolCallEvent.op !== "node.upsert") throw new Error("no ToolCall upsert found");
    expect(toolCallEvent.id).toBe("r1:t2"); // same id as on_tool_start above
    expect(toolCallEvent.props).toEqual({
      name: "readFile",
      status: "complete",
      input: JSON.stringify({ path: "docs/graph.md" }),
      output: JSON.stringify({ bytes: 812 }),
    });
  });

  it("derives no Resource when the tool input names neither a path nor a query", () => {
    const events = decodeLangGraph(
      change({ event: "on_tool_start", name: "getClock", run_id: "t9", data: { input: {} } }),
      g,
    );
    expect(events.map((event) => event.op)).toEqual(["node.upsert", "node.upsert", "edge.upsert"]);
  });

  it("two tool runs under one graph run land under the SAME Run node", () => {
    const first = decodeLangGraph(change({ event: "on_tool_start", name: "searchDocs", run_id: "t1", data: {} }), g);
    const second = decodeLangGraph(change({ event: "on_tool_start", name: "readFile", run_id: "t2", data: {} }), g);
    const runIdOf = (events: typeof first): string | undefined => {
      for (const event of events) if (event.op === "node.upsert" && event.kind === "Run") return event.id;
      return undefined;
    };
    expect(runIdOf(first)).toBe("r1");
    expect(runIdOf(first)).toBe(runIdOf(second));
  });
});
