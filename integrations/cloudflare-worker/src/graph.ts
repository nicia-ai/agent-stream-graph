/**
 * The belief graph a per-agent `AgentMaterializer` Durable Object
 * materializes into, plus the wire schema for the events a Worker request
 * carries.
 *
 * Deliberately small — one node kind, one edge kind — so the demo is about
 * the DEPLOYMENT SHAPE (a Durable Object per agent, DO SQLite storage, the D1
 * refusal in `d1-refusal.ts`), not about an elaborate domain model. Swap this
 * graph for your own; nothing else in this package is domain-specific.
 */
import {
  OP_EDGE_REMOVE,
  OP_EDGE_UPSERT,
  OP_NODE_REMOVE,
  OP_NODE_UPSERT,
  type GraphEvent,
} from "@nicia-ai/agent-stream-graph";
import { defineEdge, defineGraph, defineNode } from "@nicia-ai/typegraph";
import { z } from "zod";

export const ENTITY_KIND = "Entity";
export const RELATES_TO_KIND = "relatesTo";

export const Entity = defineNode(ENTITY_KIND, { schema: z.object({ label: z.string() }) });
export const relatesTo = defineEdge(RELATES_TO_KIND, {
  schema: z.object({ label: z.string() }),
  from: [Entity],
  to: [Entity],
});

export const beliefGraph = defineGraph({
  id: "cf_worker_belief",
  nodes: { Entity: { type: Entity } },
  edges: { relatesTo: { type: relatesTo, from: [Entity], to: [Entity] } },
});

export type BeliefGraph = typeof beliefGraph;

// ============================================================
// Wire schema for POST /agents/:agentId/events
// ============================================================
//
// Mirrors `GraphEvent<BeliefGraph>` field for field. Kept graph-specific
// (rather than a generic decoder over `Record<string, unknown>`) so a
// malformed request is refused with a typed 400 before it reaches
// TypeGraph at all, instead of surfacing as an unrelated runtime TypeError
// from `tx.nodes[event.kind]` on an unrecognized kind.

// Each upsert op gets TWO flat variants (open end / `clearValidTo`) rather than
// one schema built by intersecting with a validity union: zod's `.and()`
// produces an intersection-of-a-union type that `exactOptionalPropertyTypes`
// cannot match distributively against `GraphEvent`'s own union — every member
// below is a plain `z.object`, which typechecks against exactly one arm.
const endpointRefSchema = z.object({ kind: z.literal(ENTITY_KIND), id: z.string() });

const nodeUpsertOpenSchema = z.object({
  op: z.literal(OP_NODE_UPSERT),
  kind: z.literal(ENTITY_KIND),
  id: z.string(),
  props: Entity.schema,
  validFrom: z.string().optional(),
  validTo: z.string().optional(),
});
const nodeUpsertReopenSchema = z.object({
  op: z.literal(OP_NODE_UPSERT),
  kind: z.literal(ENTITY_KIND),
  id: z.string(),
  props: Entity.schema,
  validFrom: z.string().optional(),
  clearValidTo: z.literal(true),
});
const nodeRemoveEventSchema = z.object({
  op: z.literal(OP_NODE_REMOVE),
  kind: z.literal(ENTITY_KIND),
  id: z.string(),
});

const edgeUpsertOpenSchema = z.object({
  op: z.literal(OP_EDGE_UPSERT),
  kind: z.literal(RELATES_TO_KIND),
  from: endpointRefSchema,
  to: endpointRefSchema,
  props: relatesTo.schema,
  validFrom: z.string().optional(),
  validTo: z.string().optional(),
});
const edgeUpsertReopenSchema = z.object({
  op: z.literal(OP_EDGE_UPSERT),
  kind: z.literal(RELATES_TO_KIND),
  from: endpointRefSchema,
  to: endpointRefSchema,
  props: relatesTo.schema,
  validFrom: z.string().optional(),
  clearValidTo: z.literal(true),
});
const edgeRemoveEventSchema = z.object({
  op: z.literal(OP_EDGE_REMOVE),
  kind: z.literal(RELATES_TO_KIND),
  from: endpointRefSchema,
  to: endpointRefSchema,
});

/** One event, validated. Structurally compatible with `GraphEvent<BeliefGraph>`. */
export const graphEventSchema = z.union([
  nodeUpsertOpenSchema,
  nodeUpsertReopenSchema,
  nodeRemoveEventSchema,
  edgeUpsertOpenSchema,
  edgeUpsertReopenSchema,
  edgeRemoveEventSchema,
]);

/** The body of `POST /agents/:agentId/events` — one durable batch, pre-reconstruction. */
export const eventBatchSchema = z.object({
  /** Per-agent monotonic sequence number — this batch's resumable offset. */
  seq: z.number().int().nonnegative(),
  events: z.array(graphEventSchema),
});

type ParsedGraphEvent = z.infer<typeof graphEventSchema>;

/**
 * Reconstruct one validated event as a real `GraphEvent<BeliefGraph>`.
 *
 * Not a cast: zod's `.optional()` infers `validFrom?: string | undefined` (the
 * key may be absent, but if present its VALUE type explicitly admits
 * `undefined`), while `GraphEvent`'s own `ValidTime` declares `validFrom?:
 * string` (the key may be absent; no value is ever `undefined`) — a narrower
 * type that `exactOptionalPropertyTypes` enforces literally. The two are
 * different types, not the same type spelled two ways, so the fix is the
 * spread-to-omit idiom this library's own `graph-events.ts` uses everywhere:
 * build the field back in only when the parsed value is not `undefined`.
 */
// The one narrow assertion in this file: a computed key typed by a generic
// `K` cannot be proven, only asserted, to produce exactly `Record<K, string>`
// — TS infers `{ [x: string]: string }` for the literal instead. Contained to
// this one internal helper; every caller gets a concrete, checked shape back.
function optionalField<K extends string>(key: K, value: string | undefined): Readonly<Record<K, string>> | Record<string, never> {
  return value === undefined ? {} : ({ [key]: value } as Record<K, string>);
}

export function toGraphEvent(parsed: ParsedGraphEvent): GraphEvent<BeliefGraph> {
  switch (parsed.op) {
    case OP_NODE_REMOVE:
      return { op: parsed.op, kind: parsed.kind, id: parsed.id };
    case OP_EDGE_REMOVE:
      return { op: parsed.op, kind: parsed.kind, from: parsed.from, to: parsed.to };
    case OP_NODE_UPSERT:
      if ("clearValidTo" in parsed) {
        return { op: parsed.op, kind: parsed.kind, id: parsed.id, props: parsed.props, ...optionalField("validFrom", parsed.validFrom), clearValidTo: parsed.clearValidTo };
      }
      return {
        op: parsed.op,
        kind: parsed.kind,
        id: parsed.id,
        props: parsed.props,
        ...optionalField("validFrom", parsed.validFrom),
        ...optionalField("validTo", parsed.validTo),
      };
    case OP_EDGE_UPSERT:
      if ("clearValidTo" in parsed) {
        return { op: parsed.op, kind: parsed.kind, from: parsed.from, to: parsed.to, props: parsed.props, ...optionalField("validFrom", parsed.validFrom), clearValidTo: parsed.clearValidTo };
      }
      return {
        op: parsed.op,
        kind: parsed.kind,
        from: parsed.from,
        to: parsed.to,
        props: parsed.props,
        ...optionalField("validFrom", parsed.validFrom),
        ...optionalField("validTo", parsed.validTo),
      };
  }
}

export type EventBatch = Readonly<{ seq: number; events: readonly GraphEvent<BeliefGraph>[] }>;
