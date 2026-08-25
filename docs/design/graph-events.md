# Design: graph-shaped events over the durable-stream core

*July 2026. Synthesis of three inputs — Electric Durable Streams' State
Protocol (what we build on), Statefold (what we borrow), ActiveGraph (where we
think the vocabulary ends up).*

**Status.** Staging steps 1 and 2 shipped — `durableStateSource` and the
`GraphEvent` IR are in `src/` and exported. Steps 3 and 4 (`activeGraphSource`,
`statefoldSource`) are **not built**; they appear below as the proposals they
still are. Where this document and the code disagree, the code is current — see
[What shipped](#built-july-2026--and-what-it-cost).

---

## The shape of the question

We were asked whether this becomes one merged unit, or a core plus overlays.
The useful answer came from looking at what each input actually contributes:

| Input | Contributes | Is it a new layer? |
| --- | --- | --- |
| DS **State Protocol** (`@durable-streams/state`) | A transport record with per-message `offset`, `txid`, `old_value`, and control events | **No** — it is a `ShapeSource` |
| **Statefold** | `after=`-cursor tail, gap-free `seq`, fold-as-pure-reducer, `causation_id` | **No** — a `ShapeSource` plus one borrowed discipline |
| **ActiveGraph** | `object.created` / `relation.created` — the graph mutation vocabulary | **Yes** — this is the only new layer |

Two of the three collapse into the seam we already have. That is the finding:
this is **not** three parallel subsystems, and it is **not** one merged unit.
It is the existing core, one new layer, and more sources.

```
   sources (many)                one new layer              core (unchanged)
┌──────────────────────┐   ┌───────────────────────┐   ┌────────────────────┐
│ electricShapeSource  │   │                       │   │                    │
│ durableStreamSource  │──▶│  Decoder: ShapeChange │──▶│ consume()          │
│ durableStateSource   │   │           → GraphEvent│   │  ├ CheckpointBook  │
│ statefoldSource   (?)│   │  applyGraphEvents()   │   │  ├ transactions    │
│ activeGraphSource (?)│   │                       │   │  └ recorded anchor │
└──────────────────────┘   └───────────────────────┘   └────────────────────┘
                (?) = proposed, not built
      ShapeChange                  GraphEvent[]              TypeGraph
```

## Why the new layer earns its place

> **Not the argument: cast removal.** It is tempting to justify this layer by
> the casts it deletes, and that argument does not hold. The `as never` casts
> the demos once carried were stale rather than evidence of a TypeGraph defect —
> deleting them leaves `tsc --noEmit` green, and they were *suppressing* real
> checking, since with them gone reversing an edge's endpoints is a compile
> error. The `as string` casts come from an untyped `change.value`, which
> `ShapeChange<V>` already fixes with no new layer at all.
>
> The case for this layer is narrower than that, and alternative A3 (just export
> helpers) is correspondingly competitive. The four points below are the real
> argument.

Every demo in this repo hand-rolls the same projection —
`examples/agents.ts`, `deep-survey-convergence.ts`, `provenance-retraction.ts`,
`exactly-once.ts`, `fork-merge.ts` all run `switch (change.shape)` into
`upsertById` / `getOrCreateByEndpoints` / `delete`.

The repeated part is not the interesting part. What is genuinely hard, and what
every one of those five gets to re-decide for itself, is:

1. **Idempotence discipline** — `upsertById` / `getOrCreateByEndpoints`, never
   `create`. Today enforced by a doc comment on `Projector`.
2. **Apply order within a batch** — nodes before edges, so endpoint validation
   resolves. `interchange/importGraphStream` already learned this; projectors
   have to rediscover it.
3. **Valid-time mapping** — stream event time belongs in `validFrom`/`validTo`,
   ingest time is recorded time. Stated as doctrine in TypeGraph's
   log-integration handoff; implemented nowhere in our surface.
4. **Delete semantics** — soft vs hard, and what happens to edges whose endpoint
   just went away.
5. **Property typing** — the `as string` casts are the type system being routed
   around, in a library whose entire premise is that the graph is typed.

An IR lets us solve those once. It also makes the per-source part a **pure
function** — that is the bit we take from Statefold: `(state, payload) → state`
is testable without a database, and so is `ShapeChange → GraphEvent[]`.

## The layer

```ts
/** One typed graph mutation. The `G` parameter is the graph definition. */
type GraphEvent<G extends GraphDef> =
  | { op: "node.upsert"; kind: NodeKind<G>; id: string; props: NodeProps<G, K>;
      validFrom?: Date; validTo?: Date }
  | { op: "node.remove"; kind: NodeKind<G>; id: string }
  | { op: "edge.upsert"; kind: EdgeKind<G>; from: EndpointRef<G>; to: EndpointRef<G>;
      props?: EdgeProps<G, K>; validFrom?: Date; validTo?: Date }
  | { op: "edge.remove"; kind: EdgeKind<G>; from: EndpointRef<G>; to: EndpointRef<G> };

/** Pure: no store, no I/O, unit-testable without a database. */
type Decoder<G extends GraphDef, V> = (change: ShapeChange<V>) => readonly GraphEvent<G>[];

/** Applies a decoded batch with the discipline above. */
function applyGraphEvents<G>(tx: TransactionContext<G>, events: readonly GraphEvent<G>[]): Promise<void>;

/** Adapts a decoder into the Projector `consume()` already takes. */
function graphProjector<G, V>(decode: Decoder<G, V>): Projector<G, V>;
```

Nothing in `consume`, `CheckpointBook`, `ShapeSource`, or the offset machinery
changes. `Projector` stays exactly as it is — `graphProjector` is one way to
build one, not a replacement.

Note `op: "node.upsert"`, not `"node.created"`. ActiveGraph says *created*; we
normalize to *upsert* because at-least-once redelivery makes `create` wrong.
That normalization is a deliberate difference from the source vocabulary and
belongs in the IR, not in each decoder.

## The sources

**`durableStateSource`** — a `ShapeSource` over a DS State Protocol stream.
Nearly free on top of `durableStreamSource`, because its record already is ours:

| State Protocol | `ShapeChange` |
| --- | --- |
| `type` | `shape` |
| `key` | `key` |
| `headers.operation` (`insert`/`update`/`delete`/**`upsert`**) | `operation` |
| `value` | `value` |
| `headers.offset` | `offset` |
| `old_value` | *(no home yet — see Alternative C)* |
| `headers.txid` | *(no home yet — see below)* |

Two things fall out of this that we could not get from Electric shapes:

- **`headers.offset` is per-message on the record itself.** Where present, the
  source can use it directly instead of synthesizing a composite offset. The
  composite machinery stays for streams that lack it.
- **`headers.txid` makes `granularity: "transaction"` possible** — group a
  source transaction's changes into one TypeGraph transaction under one
  recorded anchor. That is the mapping I flagged as the most valuable thing in
  Electric Circuits' envelope, and State Protocol hands it to us on the record.
  It slots in beside the existing `"message"` and `"append"` granularities with
  no change to `consume`.

**`statefoldSource`** — `GET /api/tail?stream=…&after=<seq>` is `read(after)`
with a different spelling; `seq` is a gap-free integer, so `compareOffsets`
handles it on the existing numeric path with no composite offsets at all.

**`activeGraphSource`** — its log is in-process (SQLite/Postgres `EventStore`,
or the first-party `JSONLEventSink`), so this is a file/table source rather than
an HTTP one. Its decoder is the shortest of the three, because
`object.created` / `relation.created` / `object.removed` / `relation.removed`
map one-to-one onto the four `GraphEvent` variants.

---

## Design alternatives

### A. Where the graph vocabulary lives

- **A1 — extend `ShapeChange`** with graph operations. Rejected: it conflates
  transport with meaning, forces every source to know about graph ops, and is
  the "special cases on shared infrastructure" smell.
- **A2 — separate `GraphEvent` IR** (sketched above). Decode is pure; apply
  order, valid time, and idempotence are ours to get right once.
- **A3 — no IR, just helpers.** Export `upsertNode(tx, kind, id, props)` and
  friends; projectors call them directly. ~20 lines, no new concept, and it
  fixes the casts. It does *not* get batch ordering, does not make decode pure,
  and leaves each projector deciding valid-time policy.

**Recommendation: A2**, with the honest note that **A3 is two days and captures
maybe half the value.** If we want to de-risk, A3 first is a legitimate path —
the helpers become `applyGraphEvents`'s internals later.

### B. Typed or untyped IR

- **B1 — `kind: string`, `props: Record<string, unknown>`.** Apply via runtime
  lookup on `tx.nodes[kind]`; TypeGraph's Zod validation catches bad props at
  write time. Trivial to build. Throws away compile-time typing, which is the
  reason anyone picks TypeGraph.
- **B2 — mapped-type union** over the graph definition, as sketched. Real type
  machinery, and edge endpoints are the hard part — `provenance-retraction.ts`
  needing `as never` suggests endpoint typing is already awkward upstream.
- **B3 — typed builder instead of literals.** The decoder receives a typed
  emitter rather than returning object literals:
  ```ts
  const decode: Decoder<typeof intelGraph> = (change, g) => [
    g.node("Person", change.key, { name, email, title }),   // props inferred
    g.edge("worksAt", { Person: p }, { Company: c }),
  ];
  ```
  Inference without a giant union, and the emitter can reject an edge whose
  endpoints do not match its declared kinds.

**Recommendation: B3.** It gets B2's safety with less type surface, and it reads
like the rest of the codebase.

### C. Provenance and `old_value`

Statefold has `causation_id`; State Protocol has `old_value`; ActiveGraph has
`llm.requested` / `tool.responded` / `behavior.*` — episodic events that are not
graph mutations at all. Two ways to handle them:

- **C1 — first-class IR field.** Every `GraphEvent` carries `cause?: EndpointRef`,
  and `applyGraphEvents` attaches a provenance edge uniformly. Less boilerplate,
  but bakes a provenance model into the IR and requires the graph to declare
  specific cause/edge kinds.
- **C2 — the decoder emits them as ordinary events.** An `llm.requested` becomes
  a `node.upsert` of an `LlmCall` node plus an `edge.upsert` to what it touched.
  Zero new IR surface.

**Recommendation: C2.** It matches what `provenance-retraction.ts` already does
— justifications are ordinary typed nodes and edges — and keeps the IR to four
variants. Revisit C1 only if the boilerplate actually hurts.

`old_value` has no home in either, and that is fine for now: it becomes useful
when a decoder wants to emit a valid-time close (`validTo`) for the superseded
value rather than an overwrite. Worth prototyping in a decoder before promoting
it to the IR.

### D. Control events — a real gap

State Protocol has `snapshot-start` / `snapshot-end` / `reset`. Our
`ShapeChange` has no control variant, and the Electric adapter turns the
equivalent into an exception (`ElectricMustRefetchError`).

`reset` is fine as an exception. `snapshot-start`/`end` is not — a snapshot
means *this is the complete state, anything unmentioned is gone*, which cannot
be expressed as a run of upserts. Applying it correctly needs a mark-and-sweep
or a diff against current state.

- **D1** — treat snapshot boundaries as ordinary upserts and document that
  deletions implied by a snapshot are missed.
- **D2** — make `ShapeChange` a union with control variants. Every projector
  everywhere then has to handle them. Rejected.
- **D3** — `read()` returns `{ changes, boundaries }`. Breaking change to
  `ShapeSource`, and the reconciliation work still has to be written.

**Recommendation: D1 for v1, stated as a known limitation, not papered over.**
Snapshot reconciliation is its own piece of work and should not ride along with
the IR. Flagging it now because it is the one place where leaning into State
Protocol exposes something our current model genuinely cannot express.

### E. Packaging

Root export (today) versus subpaths (`/graph-events`, `/sources/statefold`),
which is what TypeGraph does.

**Recommendation: root for the IR, subpaths for sources.** The IR is core-adjacent
and small. Sources drag optional dependencies — an ActiveGraph source needs a
SQLite or JSONL reader, a Statefold source needs nothing but shares nothing —
and subpaths keep those lazily importable, the pattern `electricShapeSource` and
`durableStreamSource` already use.

**What shipped: a single root export.** The subpath half of that recommendation
was never needed, because the sources built so far drag no dependency a lazy
`await import()` inside the adapter does not already handle — the transport
clients are optional peers loaded at first `read()`. Revisit subpaths when a
source arrives whose dependency cannot be deferred that way.

### F. Facade or composition

A single `materialize({ source, store, checkpoints, decode })` versus composing
`consume({ ..., project: graphProjector(decode) })`.

**Recommendation: composition, no facade.** One `consume` path stays the only
way changes reach a store, and `graphProjector` is visibly *a way to build a
`Projector`* rather than a second engine. This matches the existing pattern —
`consumeSubscribed` is a driver over exposed primitives, not a parallel one.

---

## What we would not build

- **No agent runtime.** Same posture as TypeGraph's toward logs: agents already
  run on runtimes; we materialize and merge.
- **No new log.** Sources only.
- **No fold/reducer engine.** We borrow Statefold's *discipline* (decode is a
  pure function) without adopting its architecture — our fold target is a typed
  graph, not an opaque state blob, and TypeGraph is already the reducer.
- **No snapshot reconciliation in v1** (Alternative D).

## Staging

1. **SHIPPED — `durableStateSource`** — thin, and it validates the whole thesis
   against the runtime our Deep Survey demo already talks to. Adds
   `granularity: "transaction"` via `headers.txid`.
2. **SHIPPED — `GraphEvent` + `applyGraphEvents` + `graphProjector`** (A2/B3/C2)
   — `examples/agents.ts` is ported onto it; the diff was the argument.
3. **NOT BUILT — `activeGraphSource` + decoder** — shortest decoder, and the
   fork/merge demo against the reference implementation of the opposing bet.
4. **NOT BUILT — `statefoldSource`** — breadth: LangGraph, CrewAI, Agno, MCP
   behind one adapter.

Steps 1 and 2 are independent and could run concurrently. Step 3 is where the
positioning argument gets made; step 4 is where the reach is.

## Built (July 2026) — and what it cost

Shipped as `src/graph-events.ts`: the four-variant IR, a path-based typed
emitter (`g.nodes.Person.upsert(...)`), `applyGraphEvents`, and
`graphProjector`. Chosen by prototyping three designs concurrently and judging
them against a working probe rather than on paper. `examples/agents.ts` is
ported onto it and produces identical output.

Two decisions that fell out of the prototypes:

- **Distributive conditional event variants**, not plain generic aliases.
  Distribution keeps `kind` and `props` correlated when `K` binds to the whole
  union; a non-distributive alias degrades to the cross product and accepts an
  event pairing one kind with another kind's props.
- **Single-`K` generic appliers.** Because `event.kind` stays a type parameter,
  `tx.nodes[event.kind]` remains a deferred indexed access that typechecks
  against `event.props` with no cast — which is what removed the
  `TransactionContext.getEdgeCollection` upstream ask that two of the three
  prototypes had assumed was necessary.

Exactly one assertion remains in the module (`emitter as unknown as
GraphEmitter<G>` in the factory, guarded by a test that every declared kind is
present), down from five and six in the alternatives.

### How valid time lands, and why the two paths differ

Design motivation #3 says stream event time belongs in `validFrom` / `validTo`.
One rule expresses that for a replayed stream: **`validFrom` opens the window
when the row is born, `validTo` closes it on any write, and a later event never
rewinds a live row's start.** Nodes and edges reach it by different routes,
because TypeGraph's two write surfaces are not symmetric.

Edges, per `getOrCreateByEndpoints` result:

| result | `validFrom` | end mutation |
| --- | --- | --- |
| `created` | applied | applied |
| `found` | ignored — nothing is written | ignored |
| `updated` | REFUSED unless it restates the stored bound | applied |
| `resurrected` | applied (asserts the complete window) | applied |

`found` covers two cases: `ifExists: "return"`, which never writes, and a
coalesced replay under `ifExists: "update"` whose props and window matched what
was already stored. `updated` means an update actually ran.

The `found` row is why this layer passes `ifExists: "update"`. That option
defaults to `"return"`, and the default reaches `found` for every edge that
already exists, so the default would drop every `edge.upsert` event revising a
relationship's props or CLOSING its window. Worth recording how the shape hides:
the two obvious tests both pass under the default — a re-delivered identical
event is a no-op either way, and a re-delivered event with a *different*
`validFrom` leaves the window alone either way. Only asserting that a CHANGED
value converges catches it.

The `updated` row is what a replayed stream keeps colliding with, since a stream
carrying event time re-states the start of a row it already created on every
redelivery. Both surfaces settle it declaratively, with the same option:
`onImmutableLowerBound: "preserve"` makes `validFrom` create/resurrection input
and leaves a live update to apply props and `validTo` over the stored bound. One
policy, stated once in `validTimeOptions`, passed to `upsertById` and
`getOrCreateByEndpoints` alike — so this module holds the birth-instant rule
without catching an error to do it.

Neither path weakens validation: a malformed instant and a window of negative
width (`INVERTED_VALIDITY_WINDOW`) are refused on every write, and the consumer
surfaces them as a failed batch. That is deliberate — the fix for an inverted
window is to emit `validFrom` on the creating event, not to let the row through.

### Known limits, stated rather than papered over

1. **Ordering is within a decoded batch only.** `Projector` runs per change, so
   an edge whose endpoints arrive as a *separate* change still applies in stream
   order and fails endpoint validation. Motivation #2 is therefore half
   delivered. Closing it means giving `consume` a batch-level projector hook —
   a `consume` change, not a graph-events change, and it should not ride along
   silently.
2. **Union size is `2 × (nodes + edges)` and unmeasured above ~14 members.**
   Benchmark before the first large graph, because the fix if compile time
   degrades is to collapse the union, which destroys the per-kind inference that
   is the whole point.
3. **Purity forecloses snapshot mark-and-sweep.** Reconciling a
   `snapshot-start`/`end` pair needs to know what the belief already holds —
   i.e. store reads, which the decoder contract forbids. Holding the purity line
   is a deliberate architectural commitment that closes the obvious
   implementation of alternative D, not merely deferred work.

## Upstream: what TypeGraph should add

Nothing outstanding on the valid-time path — the create-only lower bound, the
endpoint-matched coalescing, window reopening, and the born-already-ended lower
bound all landed upstream and are used above. Interchange import now targets an
ingestion branch directly, so the merge examples stage untrusted belief data
through the importer rather than a hand-rolled copy loop.

- **FIXED in 0.51.1 — the stored schema document failed TypeGraph's own parser
  when a unique constraint left `scope` / `collation` to default.** The writer
  omitted both (`{"name":"byEmail","fields":["email"]}` is what landed in
  `typegraph_schema_versions.schema_doc`), while `parseSerializedSchema` required
  each to be one of its enum members. Every read path through that parser —
  `getActiveSchema()`, `store.requiresMigration()`, `store.schemaChanges()` and
  `store.verifyConstraintFences()` — threw `DatabaseOperationError: Stored schema
  document is malformed` on a database TypeGraph itself had just written. Present
  at least as far back as 0.40. Reported as
  [#525](https://github.com/nicia-ai/typegraph/issues/525) and fixed on the
  READER, which is what matters here: applying the documented defaults on read
  repairs databases already written that way, where fixing only the writer would
  have left them unreadable. Verified against the same on-disk database — it
  throws on 0.51.0 and reads cleanly on 0.51.1, with uniqueness still enforced
  rather than silently dropped.

- **SMALL — export the endpoint-kind derivation.** `EdgeFromTypes` /
  `EdgeToTypes` already exist and are used by the exported
  `TypedEdgeCollection`, but are absent from the public export list. All three
  prototypes independently hand-wrote the same `infer` incantation, which is the
  signal. Anything modelling an endpoint as *data* rather than as a live `Node`
  needs the kind names.
- **NOT asked for — `TransactionContext.getEdgeCollection`.** Two prototypes led
  with this as their top gap; the single-`K` applier removes the need entirely.
  (It remains a genuine `Store` / `TransactionCollections` asymmetry worth
  filing on its own merits — just not on this layer's account.)
- **Docs note — `NodeProps` / `EdgeProps` are `z.infer` (output) while every
  write surface takes `z.input`,** so a `.default()` field reads as required.
  A `NodeInputProps<N>` alias or a doc line would save the next consumer the
  discovery.

## Open questions

1. ~~Does `applyGraphEvents` own valid-time policy, or take it per call?~~
   **Settled: it owns it.** `validTimeOptions` states
   `onImmutableLowerBound: "preserve"` unconditionally, so the birth-instant
   rule holds for every source without a decoder opting in. Event time is on the
   record for State Protocol (`headers.timestamp`) and Statefold but absent from
   ActiveGraph's object events, and a per-call policy would have made that
   difference every decoder's problem.
2. ~~Edge removal by endpoints or by edge id?~~ **Settled: by endpoints.**
   `removeEdge` locates the row with `findByEndpoints` and returns silently when
   it is absent, which keeps a re-delivered removal a no-op. Endpoints are also
   what `edge.upsert` matches on, so both edge operations address a row the same
   way.
3. **Does a decoder ever need to read the store? No** — purity is held as an
   invariant. If a use case emerges that genuinely needs store reads
   during decode, it gets a separate abstraction rather than a relaxation here —
   a decoder that could read the store could make its output depend on when it
   ran, which breaks replay. See known limit 3 for what this forecloses.
