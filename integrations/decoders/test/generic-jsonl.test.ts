import { graphEmitter, type GraphEvent, type ShapeChange } from "@nicia-ai/agent-stream-graph";
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import { decodeGenericJsonl, type ToolLogLine } from "../src/generic-jsonl.js";
import { agentGraph } from "../src/graph.js";
import { parseToolLog } from "../src/parse-jsonl.js";

const g = graphEmitter(agentGraph);
const FIXTURE = new URL("../fixtures/generic-jsonl.jsonl", import.meta.url);

function change(line: ToolLogLine): ShapeChange<ToolLogLine> {
  return { offset: "001", shape: "tool-log", key: "k", operation: "insert", value: line };
}

// Table-driven: one row per phase, since the three phases are the whole
// vocabulary this decoder has to cover.
const PHASES: ReadonlyArray<{
  readonly name: string;
  readonly line: ToolLogLine;
  readonly expectedProps: Readonly<{ name: string; status: string; input: string; output?: string }>;
}> = [
  {
    name: "start — pending, no output yet",
    line: { run: "r1", call: "c1", tool: "searchDocs", phase: "start", args: { query: "graph databases" } },
    expectedProps: { name: "searchDocs", status: "pending", input: JSON.stringify({ query: "graph databases" }) },
  },
  {
    name: "end — complete, carries the result as output",
    line: { run: "r1", call: "c1", tool: "searchDocs", phase: "end", args: { query: "graph databases" }, result: { hits: 3 } },
    expectedProps: {
      name: "searchDocs",
      status: "complete",
      input: JSON.stringify({ query: "graph databases" }),
      output: JSON.stringify({ hits: 3 }),
    },
  },
  {
    name: "error — the failure message becomes the output",
    line: { run: "r1", call: "c1", tool: "searchDocs", phase: "error", args: { query: "graph databases" }, error: "timeout" },
    expectedProps: {
      name: "searchDocs",
      status: "error",
      input: JSON.stringify({ query: "graph databases" }),
      output: JSON.stringify({ error: "timeout" }),
    },
  },
];

describe("decodeGenericJsonl", () => {
  for (const { name, line, expectedProps } of PHASES) {
    it(name, () => {
      const events = decodeGenericJsonl(change(line), g);
      const toolCallEvent = events.find((event) => event.op === "node.upsert" && event.kind === "ToolCall");
      if (toolCallEvent === undefined || toolCallEvent.op !== "node.upsert") throw new Error("no ToolCall upsert found");
      expect(toolCallEvent.id).toBe("r1:c1");
      expect(toolCallEvent.props).toEqual(expectedProps);
    });
  }

  it("needs no envelope — run and call ids come straight from the log line", () => {
    const events = decodeGenericJsonl(change({ run: "r7", call: "cX", tool: "ping", phase: "start" }), g);
    const runEvent = events.find((event) => event.op === "node.upsert" && event.kind === "Run");
    expect(runEvent).toEqual(g.nodes.Run.upsert("r7", { source: "generic-jsonl" }));
  });

  it("derives a Resource from a `path` or `query` field in args, and none otherwise", () => {
    const withPath = decodeGenericJsonl(change({ run: "r1", call: "c1", tool: "readFile", phase: "start", args: { path: "a.txt" } }), g);
    const withQuery = decodeGenericJsonl(change({ run: "r1", call: "c2", tool: "search", phase: "start", args: { query: "q" } }), g);
    const withNeither = decodeGenericJsonl(change({ run: "r1", call: "c3", tool: "getClock", phase: "start", args: {} }), g);

    const resourceIdOf = (events: readonly GraphEvent<typeof agentGraph>[]): string | undefined => {
      for (const event of events) if (event.op === "node.upsert" && event.kind === "Resource") return event.id;
      return undefined;
    };
    expect(resourceIdOf(withPath)).toBe("file:a.txt");
    expect(resourceIdOf(withQuery)).toBe("query:q");
    expect(resourceIdOf(withNeither)).toBeUndefined();
  });

  describe("parseToolLog", () => {
    it("parses the checked-in fixture into one ShapeChange per line, in order", () => {
      const text = readFileSync(FIXTURE, "utf8");
      const changes = parseToolLog(text);
      expect(changes).toHaveLength(4);
      expect(changes.map((c) => c.value.phase)).toEqual(["start", "end", "start", "end"]);
      expect(changes.map((c) => c.offset)).toEqual(["001", "002", "003", "004"]);
      expect(new Set(changes.map((c) => c.operation))).toEqual(new Set(["insert"]));
    });

    it("round-trips through decodeGenericJsonl into a complete run of two tool calls", () => {
      const text = readFileSync(FIXTURE, "utf8");
      const events = parseToolLog(text).flatMap((c) => decodeGenericJsonl(c, g));
      const toolCallIds = new Set<string>();
      for (const event of events) if (event.op === "node.upsert" && event.kind === "ToolCall") toolCallIds.add(event.id);
      expect([...toolCallIds].sort()).toEqual(["run-jsonl:c1", "run-jsonl:c2"]); // start + end both upsert the same id — one row per call, not per event
    });
  });
});
