import {
  asEdgeId,
  asNodeId,
  type EdgeKinds,
  type GetEdgeType,
  type GetNodeType,
  getEdgeKinds,
  getNodeKinds,
  type GraphDef,
  type NodeKinds,
  type TransactionContext,
} from "@nicia-ai/typegraph";
import type { z } from "zod";

import type { Projector } from "./consumer.js";
import type { ShapeChange } from "./types.js";

// --- The canonical graph mutation vocabulary --------------------------------
//
// Sources speak different vocabularies — Electric shapes are row changes, the
// State Protocol is typed CRUD, ActiveGraph emits `object.created` /
// `relation.created`. All of them are ultimately saying one of four things to a
// graph. Naming those four once means a source contributes a DECODER (a pure
// function, no store, no I/O) while the rules that are easy to get wrong —
// idempotence, apply order, valid time, delete semantics — are settled here for
// every source at once.
//
// Events are plain data: no brands, no symbols, no hidden fields. They can be
// logged, snapshot-tested, and round-tripped through JSON unchanged, which is
// why valid time is carried as ISO-8601 strings rather than `Date`.

/** Props a node kind accepts on write. `z.input`, not `z.infer`, so `.default()` fields stay omissible. */
export type NodeInput<G extends GraphDef, K extends NodeKinds<G>> = z.input<GetNodeType<G, K>["schema"]>;

/** Props an edge kind accepts on write. */
export type EdgeInput<G extends GraphDef, K extends EdgeKinds<G>> = z.input<GetEdgeType<G, K>["schema"]>;

/**
 * The node kinds an edge may start from, as a union of kind NAMES.
 *
 * Derived here rather than imported because TypeGraph's equivalents
 * (`EdgeFromTypes` / `EdgeToTypes`) are not part of its public export surface.
 * See the upstream note in `docs/design/graph-events.md`.
 */
export type EdgeFromKinds<G extends GraphDef, K extends EdgeKinds<G>> =
  G["edges"][K]["from"] extends readonly (infer N extends { kind: string })[] ? N["kind"] : never;

/** The node kinds an edge may point to. */
export type EdgeToKinds<G extends GraphDef, K extends EdgeKinds<G>> =
  G["edges"][K]["to"] extends readonly (infer N extends { kind: string })[] ? N["kind"] : never;

/**
 * An endpoint as DATA — the kind name and the id, nothing more.
 *
 * Deliberately not TypeGraph's `NodeRef`, which also admits a live `Node`
 * carrying meta, branded ids, and props that an event has no business
 * transporting.
 */
export type EndpointRef<Kind extends string> = Readonly<{ kind: Kind; id: string }>;

export const OP_NODE_UPSERT = "node.upsert";
export const OP_NODE_REMOVE = "node.remove";
export const OP_EDGE_UPSERT = "edge.upsert";
export const OP_EDGE_REMOVE = "edge.remove";

/**
 * Application-time validity, as ISO-8601 instants.
 *
 * Stream EVENT time belongs here; INGEST time is recorded time and is the
 * store's to assign. Strings rather than `Date` so an event survives a JSON
 * round trip as itself, and because TypeGraph's write options take strings.
 *
 * ONE RULE, NODES AND EDGES ALIKE: `validFrom` opens the window when the row is
 * BORN — created, or resurrected from a tombstone — and the END is whatever the
 * event says about it. A later event never rewinds a live row's start, because
 * that bound is already history.
 *
 * The end has three states, and they are distinct on purpose:
 *
 * - **omitted** — say nothing; the stored end stands.
 * - **`validTo`** — the fact stopped being true at that instant.
 * - **`clearValidTo: true`** — it is true again; reopen the window. Row identity
 *   and the original `validFrom` both survive, so a resumed relationship reads
 *   as one continuous row rather than a second incarnation.
 *
 * The last two are mutually exclusive, in the type and in the store.
 *
 * This rule is what a replayed stream needs. Re-delivery is the normal case
 * under at-least-once, and a stream carrying event time necessarily re-states
 * the start of a row it already created; a store that refused the restatement
 * would abort the batch, leave the cursor unadvanced, and re-deliver the same
 * change forever. `validFrom` is therefore written under
 * `onImmutableLowerBound: "preserve"`, which states it as create/resurrection
 * INPUT rather than as a claim about the row that is there now.
 *
 * A stated bound is still VALIDATED on every write, so a malformed instant or a
 * window that ends before it starts is refused whichever path applies it. Those
 * are data errors, and they are meant to be loud.
 */
export type ValidTime = Readonly<{ validFrom?: string }> & ValidityEnd;

/**
 * What an event says about a row's end of validity. Mirrors TypeGraph's own
 * `ValidityEndMutation` union, so setting an end and clearing one cannot be
 * stated together — a mistake worth catching at the decoder rather than at the
 * write.
 */
export type ValidityEnd =
  | Readonly<{ validTo?: string; clearValidTo?: never }>
  | Readonly<{ validTo?: never; clearValidTo: true }>;

/**
 * The event's valid time as TypeGraph write options, with what the event did
 * not say left out, and the birth-instant rule stated as policy rather than
 * enforced by this module: `onImmutableLowerBound: "preserve"` makes
 * `validFrom` create/resurrection input, so a live update keeps the bound the
 * row already holds while still applying props and the stated end.
 *
 * The policy is passed unconditionally — with no `validFrom` in the event there
 * is no bound for it to decide — and both write surfaces this module uses,
 * `upsertById` and `getOrCreateByEndpoints`, accept it and the end mutation
 * alike.
 */
function validTimeOptions(valid: ValidTime): Readonly<{ validFrom?: string; onImmutableLowerBound: "preserve" }> &
  ValidityEnd {
  const base = {
    ...(valid.validFrom === undefined ? {} : { validFrom: valid.validFrom }),
    onImmutableLowerBound: "preserve" as const,
  };
  if (valid.clearValidTo === true) return { ...base, clearValidTo: true };
  return valid.validTo === undefined ? base : { ...base, validTo: valid.validTo };
}

// Each variant is a DISTRIBUTIVE conditional (`K extends … ? … : never`), not a
// plain generic alias. Distribution is load-bearing: it keeps `kind` and `props`
// correlated even when `K` binds to the whole union, so a hand-written literal
// pairing one kind with another kind's props is rejected. A non-distributive
// alias silently degrades to the cross product and accepts it.

export type NodeUpsertEvent<G extends GraphDef, K extends NodeKinds<G> = NodeKinds<G>> = K extends NodeKinds<G>
  ? Readonly<{ op: typeof OP_NODE_UPSERT; kind: K; id: string; props: NodeInput<G, K> }> & ValidTime
  : never;

export type NodeRemoveEvent<G extends GraphDef, K extends NodeKinds<G> = NodeKinds<G>> = K extends NodeKinds<G>
  ? Readonly<{ op: typeof OP_NODE_REMOVE; kind: K; id: string }>
  : never;

/**
 * Carries {@link ValidTime} under that type's birth-instant rule.
 *
 * The edge is matched by its ENDPOINTS, not by an id, so a replay finds the row
 * it wrote last time — an already-ended one included. That is what keeps a
 * closing event from minting a parallel edge on every redelivery, and what lets
 * `clearValidTo` resume the very relationship that ended.
 */
export type EdgeUpsertEvent<G extends GraphDef, K extends EdgeKinds<G> = EdgeKinds<G>> = K extends EdgeKinds<G>
  ? Readonly<{
      op: typeof OP_EDGE_UPSERT;
      kind: K;
      from: EndpointRef<EdgeFromKinds<G, K>>;
      to: EndpointRef<EdgeToKinds<G, K>>;
      props: EdgeInput<G, K>;
    }> &
      ValidTime
  : never;

export type EdgeRemoveEvent<G extends GraphDef, K extends EdgeKinds<G> = EdgeKinds<G>> = K extends EdgeKinds<G>
  ? Readonly<{
      op: typeof OP_EDGE_REMOVE;
      kind: K;
      from: EndpointRef<EdgeFromKinds<G, K>>;
      to: EndpointRef<EdgeToKinds<G, K>>;
    }>
  : never;

/** One typed graph mutation. */
export type GraphEvent<G extends GraphDef> =
  | NodeUpsertEvent<G>
  | NodeRemoveEvent<G>
  | EdgeUpsertEvent<G>
  | EdgeRemoveEvent<G>;

/** Edges whose schema is empty may omit props entirely — most edges in practice. */
type EdgeUpsertArgs<G extends GraphDef, K extends EdgeKinds<G>> =
  Record<string, never> extends EdgeInput<G, K>
    ? [props?: EdgeInput<G, K>, valid?: ValidTime]
    : [props: EdgeInput<G, K>, valid?: ValidTime];

export type NodeEventEmitter<G extends GraphDef, K extends NodeKinds<G>> = Readonly<{
  upsert: (id: string, props: NodeInput<G, K>, valid?: ValidTime) => NodeUpsertEvent<G, K>;
  remove: (id: string) => NodeRemoveEvent<G, K>;
}>;

export type EdgeEventEmitter<G extends GraphDef, K extends EdgeKinds<G>> = Readonly<{
  upsert: (
    from: EndpointRef<EdgeFromKinds<G, K>>,
    to: EndpointRef<EdgeToKinds<G, K>>,
    ...args: EdgeUpsertArgs<G, K>
  ) => EdgeUpsertEvent<G, K>;
  remove: (from: EndpointRef<EdgeFromKinds<G, K>>, to: EndpointRef<EdgeToKinds<G, K>>) => EdgeRemoveEvent<G, K>;
}>;

/**
 * The typed event constructors a decoder is handed.
 *
 * Kind comes from the OBJECT PATH — `g.nodes.Person.upsert(...)` — never from a
 * kind argument. That is what produces `Did you mean 'Person'?` on a typo
 * instead of an unresolved constraint blob, and it mirrors how the store the
 * events end up in already reads (`tx.nodes.Person.upsertById`).
 */
export type GraphEmitter<G extends GraphDef> = Readonly<{
  nodes: Readonly<{ [K in NodeKinds<G>]: NodeEventEmitter<G, K> }>;
  edges: Readonly<{ [K in EdgeKinds<G>]: EdgeEventEmitter<G, K> }>;
}>;

/** The emitter as the factory builds it, before the mapped-type shape is asserted. */
type ErasedEmitter = Readonly<{
  nodes: Record<string, NodeEventEmitter<GraphDef, string>>;
  edges: Record<string, EdgeEventEmitter<GraphDef, string>>;
}>;

function nodeEventEmitter(kind: string): NodeEventEmitter<GraphDef, string> {
  return {
    upsert: (id, props, valid) => ({ op: OP_NODE_UPSERT, kind, id, props, ...valid }),
    remove: (id) => ({ op: OP_NODE_REMOVE, kind, id }),
  };
}

function edgeEventEmitter(kind: string): EdgeEventEmitter<GraphDef, string> {
  return {
    upsert: (from, to, props, valid) => ({ op: OP_EDGE_UPSERT, kind, from, to, props: props ?? {}, ...valid }),
    remove: (from, to) => ({ op: OP_EDGE_REMOVE, kind, from, to }),
  };
}

/**
 * Build the typed event constructors for `graph`.
 *
 * The one assertion in this module: a loop over runtime kind strings cannot
 * prove to the compiler that it filled every key of a mapped type. It is
 * contained — `ErasedEmitter` keeps the closures shape-checked, and no call site
 * inherits it — but a kind missing here would surface at runtime rather than at
 * compile time, which is what `emitterCoversEveryKind` in the tests guards.
 */
export function graphEmitter<G extends GraphDef>(graph: G): GraphEmitter<G> {
  const emitter: ErasedEmitter = { nodes: {}, edges: {} };
  for (const kind of getNodeKinds(graph)) emitter.nodes[kind] = nodeEventEmitter(kind);
  for (const kind of getEdgeKinds(graph)) emitter.edges[kind] = edgeEventEmitter(kind);
  return emitter as unknown as GraphEmitter<G>;
}

// Each applier is generic in a SINGLE `K`, which is what keeps this module
// cast-free: `event.kind` then has type `K` rather than the whole union, so
// `tx.nodes[event.kind]` stays a deferred indexed access and typechecks directly
// against `event.props`. Collapsing these into one function over `GraphEvent<G>`
// forces `tx.edges[...]` to be reached through an assertion instead.

async function upsertNode<G extends GraphDef, K extends NodeKinds<G>>(
  tx: TransactionContext<G>,
  event: NodeUpsertEvent<G, K>,
): Promise<void> {
  await tx.nodes[event.kind].upsertById(event.id, event.props, validTimeOptions(event));
}

async function removeNode<G extends GraphDef, K extends NodeKinds<G>>(
  tx: TransactionContext<G>,
  event: NodeRemoveEvent<G, K>,
): Promise<void> {
  await tx.nodes[event.kind].delete(asNodeId(event.id));
}

async function upsertEdge<G extends GraphDef, K extends EdgeKinds<G>>(
  tx: TransactionContext<G>,
  event: EdgeUpsertEvent<G, K>,
): Promise<void> {
  await tx.edges[event.kind].getOrCreateByEndpoints(event.from, event.to, event.props, {
    ...validTimeOptions(event),
    // Without this the call is `ensure`, not `upsert`: TypeGraph's default
    // `ifExists: "return"` hands back the existing edge and writes nothing, so
    // an event that changes a relationship's props or CLOSES its window is
    // silently dropped for every edge that already exists — exactly when it
    // matters. Updating keeps re-delivery idempotent: the match is by
    // endpoints, so a replay updates the same row (an ENDED edge included,
    // which is what stops a closing event from minting a duplicate).
    ifExists: "update",
  });
}

async function removeEdge<G extends GraphDef, K extends EdgeKinds<G>>(
  tx: TransactionContext<G>,
  event: EdgeRemoveEvent<G, K>,
): Promise<void> {
  // `EdgeCollection.delete` takes an id, so the edge has to be located first.
  // Returning silently when it is absent keeps a re-delivered removal a no-op
  // rather than an error, matching how `consume` already treats a no-op delete.
  const existing = await tx.edges[event.kind].findByEndpoints(event.from, event.to);
  if (existing === undefined) return;
  await tx.edges[event.kind].delete(asEdgeId(existing.id));
}

async function applyEvent<G extends GraphDef>(tx: TransactionContext<G>, event: GraphEvent<G>): Promise<void> {
  switch (event.op) {
    case OP_NODE_UPSERT:
      return upsertNode(tx, event);
    case OP_EDGE_UPSERT:
      return upsertEdge(tx, event);
    case OP_EDGE_REMOVE:
      return removeEdge(tx, event);
    case OP_NODE_REMOVE:
      return removeNode(tx, event);
  }
}

/**
 * Nodes are created before the edges that reference them, and edges are removed
 * before the nodes they hang off. Emitting in this order is otherwise every
 * decoder's problem to remember, and getting it wrong fails endpoint validation.
 */
const APPLY_ORDER = [OP_NODE_UPSERT, OP_EDGE_UPSERT, OP_EDGE_REMOVE, OP_NODE_REMOVE] as const;

/**
 * Apply one decoded batch.
 *
 * ORDERING IS WITHIN A BATCH ONLY. `Projector` runs per change, so events
 * decoded from DIFFERENT changes still apply in stream order — an edge whose
 * endpoints arrive as a separate later change will fail endpoint validation.
 * Decode both sides from one change, or ensure the source orders them.
 */
export async function applyGraphEvents<G extends GraphDef>(
  tx: TransactionContext<G>,
  events: readonly GraphEvent<G>[],
): Promise<void> {
  for (const op of APPLY_ORDER) {
    for (const event of events) {
      if (event.op === op) await applyEvent(tx, event);
    }
  }
}

/**
 * Turns one source change into graph mutations. PURE — no store, no I/O, not
 * async. That is what makes a source's contribution testable without a database,
 * and it is deliberate: a decoder that could read the store could also make its
 * output depend on when it ran, which would break replay.
 */
export type Decoder<G extends GraphDef, V = Record<string, unknown>> = (
  change: ShapeChange<V>,
  emit: GraphEmitter<G>,
) => readonly GraphEvent<G>[];

/**
 * Adapt a decoder into the {@link Projector} `consume` takes.
 *
 * Takes the graph VALUE, not just its type, because the emitter is built from
 * the declared kinds — and passing the value is what lets an inline decoder
 * lambda infer `G` without anyone writing `Decoder<typeof graph, V>`.
 */
export function graphProjector<G extends GraphDef, V = Record<string, unknown>>(
  graph: G,
  decode: Decoder<G, V>,
): Projector<G, V> {
  const emit = graphEmitter(graph);
  return async (tx, change) => {
    await applyGraphEvents(tx, decode(change, emit));
  };
}
