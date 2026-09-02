/**
 * The shared graph every decoder in this package writes into.
 *
 *   Run --madeCall--> ToolCall --touched--> Resource
 *
 * One tiny schema, three sources (`vercel-ai-sdk.ts`, `langgraph.ts`,
 * `generic-jsonl.ts`). Sharing it is the whole point: if each framework had
 * its own graph shape, "these decoders are interchangeable" would not be a
 * checkable claim — `demo.ts` checks it by decoding three different frameworks'
 * fixtures into events over this one schema and comparing what comes out.
 */
import { defineEdge, defineGraph, defineNode } from "@nicia-ai/typegraph";
import type { GraphEmitter, GraphEvent } from "@nicia-ai/agent-stream-graph";
import { z } from "zod";

const Run = defineNode("Run", {
  schema: z.object({
    // Which framework decoded this run — "vercel-ai-sdk" | "langgraph" | "generic-jsonl".
    // A plain string, not an enum: a fourth decoder should be able to write
    // into this same graph without a schema migration.
    source: z.string(),
  }),
});

const ToolCall = defineNode("ToolCall", {
  schema: z.object({
    name: z.string(),
    status: z.enum(["pending", "complete", "error"]),
    // Input/output are stored JSON-stringified rather than modeled field by
    // field: a shared graph across three frameworks cannot know every tool's
    // argument shape, and re-parsing a stored string is one `JSON.parse` a
    // reader can do, versus a schema this package would have to keep pace
    // with every tool any of the three frameworks might call.
    input: z.string(),
    output: z.string().optional(),
  }),
});

const Resource = defineNode("Resource", {
  schema: z.object({ resourceType: z.string() }),
});

const madeCall = defineEdge("madeCall", { schema: z.object({}) });
const touched = defineEdge("touched", { schema: z.object({}) });

export const agentGraph = defineGraph({
  id: "asg_decoders_demo",
  nodes: { Run: { type: Run }, ToolCall: { type: ToolCall }, Resource: { type: Resource } },
  edges: {
    madeCall: { type: madeCall, from: [Run], to: [ToolCall] },
    touched: { type: touched, from: [ToolCall], to: [Resource] },
  },
});

export type AgentGraph = typeof agentGraph;
export type ToolCallStatus = "pending" | "complete" | "error";

/** A resource a tool call's input named, identified independently of which framework or run saw it. */
export type ResourceRef = Readonly<{ id: string; resourceType: string }>;

/**
 * The one place that knows what counts as "a resource" in a tool call's
 * input. Deliberately shallow — a `path` field means a file, a `query`
 * field means a search — because the point of this package is the decoder
 * boundary, not a resource-classification engine. The id is content-derived
 * and framework-independent on purpose: three decoders that each see a tool
 * call touch `docs/graph.md` all resolve to the SAME Resource node, which is
 * the convergence `demo.ts` shows off.
 */
export function resourceRefFromInput(input: unknown): ResourceRef | undefined {
  if (typeof input !== "object" || input === null) return undefined;
  const record = input as Record<string, unknown>;
  if (typeof record.path === "string") return { id: `file:${record.path}`, resourceType: "file" };
  if (typeof record.query === "string") return { id: `query:${record.query}`, resourceType: "query" };
  return undefined;
}

/** What every decoder in this package learns about one tool call, regardless of the framework it came from. */
export type ToolCallFields = Readonly<{
  runId: string;
  runSource: string;
  toolCallId: string;
  name: string;
  status: ToolCallStatus;
  input: unknown;
  output?: unknown;
}>;

/**
 * The shared write path every decoder in this package calls: the Run node,
 * the ToolCall node, the edge between them, and — when the input names one —
 * the Resource node and the edge to it. Nothing here is framework-specific;
 * each decoder's whole job is reducing its own event shape down to this.
 */
export function toolCallEvents(g: GraphEmitter<AgentGraph>, fields: ToolCallFields): GraphEvent<AgentGraph>[] {
  const { runId, runSource, toolCallId, name, status, input, output } = fields;
  const events: GraphEvent<AgentGraph>[] = [
    g.nodes.Run.upsert(runId, { source: runSource }),
    g.nodes.ToolCall.upsert(toolCallId, {
      name,
      status,
      input: JSON.stringify(input),
      ...(output === undefined ? {} : { output: JSON.stringify(output) }),
    }),
    g.edges.madeCall.upsert({ kind: "Run", id: runId }, { kind: "ToolCall", id: toolCallId }),
  ];

  const resource = resourceRefFromInput(input);
  if (resource !== undefined) {
    events.push(
      g.nodes.Resource.upsert(resource.id, { resourceType: resource.resourceType }),
      g.edges.touched.upsert({ kind: "ToolCall", id: toolCallId }, { kind: "Resource", id: resource.id }),
    );
  }
  return events;
}
