# @nicia-ai/agent-stream-graph

Durably materialize agent event streams ([Electric](https://electric-sql.com)
shapes) into **entity-resolved, bitemporal** knowledge graphs on
[TypeGraph](https://github.com/nicia-ai/typegraph) — resumable, idempotent, and
replayable by offset.

## The problem

Agents emit durable streams of what they did and saw. A stream tells you _what
happened_, in order — but not that two agents were talking about the **same
entity**, and not what an agent _believed at a given moment_. This library
brings those together:

- **Durable consumption.** A crash-safe consumer reads each agent's shape log
  from its last checkpoint, applies changes idempotently, and records the
  offset alongside a recorded-time anchor.
- **Per-agent bitemporal belief.** Each agent materializes its own
  history-enabled graph, so you can ask _what did this agent believe at offset
  N?_ — `store.asOfRecorded(book.anchorFor(stream, offset))`.
- **Entity resolution.** Merge the per-agent beliefs into one canonical graph
  with TypeGraph's `mergeIncremental` — `J. Doe` and `Jane Doe` at the same
  email collapse to one entity, with conflicts flagged and provenance retained.

## Install

```bash
pnpm add @nicia-ai/agent-stream-graph @nicia-ai/typegraph zod
# plus the driver your backend needs, e.g. for the local SQLite quick start:
pnpm add drizzle-orm better-sqlite3
```

> **TypeGraph and Zod are peer dependencies, not bundled ones**, and installing
> them yourself is not a formality. TypeGraph brands `NodeId`, `EdgeId`, and
> `RecordedInstant` with `unique symbol`s, which are nominal: two copies of the
> library in one dependency tree produce two mutually incompatible sets of those
> types. Since you build the `Store` and this package consumes it, a second copy
> makes `consume({ store })` fail to typecheck with the notoriously unhelpful
> `Type 'Store<G>' is not assignable to type 'Store<G>'`. Declaring them as peers
> is what guarantees one copy. The supported range is TypeGraph
> `>=0.54.0 <0.55.0` and Zod `^4` — the same range TypeGraph itself declares.
>
> **The transport clients are OPTIONAL peers**: install `@electric-sql/client`
> or `@durable-streams/client` only for the transport you actually use. Both are
> lazily imported at first `read()`, so the package loads with neither present,
> and nothing is installed on your behalf.
>
> **This package is ESM-only** (`"type": "module"`, a single `.` export
> condition). It requires Node >= 22 and has no CommonJS build; `require()` of
> it will not resolve.
>
> This package depends only on TypeGraph's **portable entrypoints**, so it pulls
> in no database driver of its own. Since TypeGraph 0.51 `drizzle-orm` is an
> optional peer rather than a hard dependency: install it — with the driver
> underneath it — only for the backend you actually run. A managed SQLite or
> PGlite Store, or any explicit `/adapters/drizzle/...` entrypoint, needs it; a
> Store built on a custom backend does not. A managed Store factory whose peer is
> missing reports a typed `MISSING_PEER_DEPENDENCY` naming the install command.
>
> The declared peer range is a hard floor, so the per-feature minimums below it
> are historical rather than something you can select: the graph-event layer
> needs >= 0.48 for `onImmutableLowerBound: "preserve"` and `clearValidTo` (see
> [Valid time on events](#valid-time-on-events)), the claim fences need >= 0.50,
> and `verifyConstraintFences()` needs >= 0.51.1.
>
> The consumer reads each change's write count and recorded commit instant off
> the transaction receipt (`store.transactionWithReceipt()`), and belief stores
> use `coalesceUnchangedUpserts` so replays don't churn history.
>
> The default checkpoint book is portable: build its store with
> `createStoreWithSchema`. Exactly-once callers opt into
> `typeGraphAdoptingCheckpoints` and `createAdapterStoreWithSchema`, which keep
> the adopted native transaction handle precisely typed. Drizzle-backed
> constructors come from `@nicia-ai/typegraph/adapters/drizzle/sqlite/local`.
>
> This package is developed against TypeGraph 0.54.0 and better-sqlite3 13,
> which TypeGraph's optional peer range covers. better-sqlite3 13 ships N-API
> prebuilds, so it needs no source build — `pnpm-workspace.yaml` declares it
> under `allowBuilds` as deliberately not built. pnpm itself is pinned by the
> `packageManager` field, which both corepack and CI honour.
>
> **Upgrading an existing belief store to TypeGraph >= 0.50** — 0.50 added
> `typegraph_edge_claims`, the relation that fences declared edge cardinality.
> A store opened through `createStoreWithSchema` emits it on its idempotent boot
> path, so a fresh database and a normal reopen both get it for free; a database
> whose schema you manage with `generateSqliteMigrationSQL` /
> `generatePostgresMigrationSQL` needs that migration re-run. A store that
> reaches the relation and does not find it refuses its first constrained edge
> write with a typed `ConfigurationError` (`EDGE_CLAIM_RELATION_MISSING`) naming
> the migration, rather than failing opaquely at the driver.

## Quick start

```ts
import {
  consume,
  mockShapeSource,
  typeGraphCheckpoints,
  checkpointGraph,
  type Projector,
} from "@nicia-ai/agent-stream-graph";
import { createStoreWithSchema } from "@nicia-ai/typegraph";
import { createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";

// 1. A history-enabled belief graph for the agent, and a durable checkpoint store.
//    `coalesceUnchangedUpserts` makes a re-delivered identical row a true no-op,
//    so at-least-once re-delivery and replays don't rewrite rows or churn history.
const [belief] = await createStoreWithSchema(myGraph, createLocalSqliteBackend().backend, {
  history: true,
  coalesceUnchangedUpserts: true,
});
// At-least-once checkpointing needs only the portable Store surface.
const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
const book = typeGraphCheckpoints(cursor);

// 2. An idempotent projector: shape change → belief graph. The first type
//    parameter is the GRAPH definition (`myGraph` above), not the store.
const project: Projector<typeof myGraph> = async (store, change) => {
  await store.nodes.Person.upsertById(change.key, { name: change.value.name as string });
};

// 3. Consume — resumes from the checkpoint, idempotent, replayable by offset.
const source = mockShapeSource("crm-agent", changes);
await consume({ source, store: belief, checkpoints: book, project });

// 4. Time-travel the belief by offset.
const anchor = await book.anchorFor("crm-agent", "0042");
const past = belief.asOfRecorded(anchor!); // a read-only view as of that offset
```

## Demos

Every demo below is self-contained: no network, no service, no API key. They are
deterministic, they assert their own invariants rather than narrating them — a
demo that would still print success if the library broke is worthless — and
`pnpm demo:all` runs the whole set in CI so the showcase cannot rot.

Start here. Each of these carries exactly one idea:

| | What it proves |
| --- | --- |
| `pnpm demo:emit` | An agent's memory is an append-only log; the graph is a fold over it. Events are plain JSON — the demo round-trips them through `JSON.stringify` before applying any. |
| `pnpm demo:time-travel` | "Why did the agent do that?" A decision is reproduced by reconstructing the belief it was made against, after the evidence is gone from the current graph. |
| `pnpm demo:valid-time` | The fact that came back. A window closes, then reopens **in place** — one row, original `validFrom` intact — and the two time axes answer different questions. |
| `pnpm demo:contradiction` | Two agents, one entity, no silent winner. `J. Doe` and `Jane Doe` collapse; the disagreement they carry is *flagged*, not averaged. |
| `pnpm demo:crash-resume` | A real `SIGKILL` mid-stream. The resumed graph is byte-identical to an uninterrupted run, with the re-delivered change absorbed rather than duplicated. |
| `pnpm demo:swarm` | One worker, a fleet of agent streams, pull-wake subscriptions. A failed batch hands the lease back instead of sitting on it. |

Then the longer ones, which combine those ideas:

| | What it proves |
| --- | --- |
| `pnpm demo` | The flagship: Deep Survey shared state converging into canonical concepts. |
| `pnpm demo:mechanics` | consume → resume → per-agent time travel → merge, end to end. |
| `pnpm demo:exactly-once` | Projection and cursor advance in one transaction, so a crash leaves nothing to re-deliver. |
| `pnpm demo:fork-merge` | Fork the log at a checkpoint, let the branches diverge, reconcile them. |
| `pnpm demo:provenance` | Retract a source and watch belief revision cascade through justifications. |

## Integrations

Beyond `examples/`, this repo carries a workspace of ecosystem integrations —
`demos/newsroom` (the flagship app) and `integrations/*`. They are private and
unpublished, and they exist for two reasons.

The first is obvious: showing what this library looks like wired to the things
people actually run. The second is not. Every demo in `examples/` imports
`../src`, so nothing there exercises what a consumer *installs*. Each
integration depends on `"@nicia-ai/agent-stream-graph": "workspace:*"` — the
built `dist/`, through the real `exports` map, under Node's ESM resolver, with a
single copy of TypeGraph. They are a standing test of the packaging claims, and
CI runs each one's `verify` on every push.

| Package | What it demonstrates |
| --- | --- |
| `integrations/mcp-memory` | An MCP server exposing the belief graph as `recall` / `believedAt` / `whySoFar`, so any agent gets entity-resolved, time-travelable memory. |
| `integrations/claude-agent` | A Claude Agent SDK session — tool calls, results, subagents — projected into a bitemporal graph. |
| `integrations/decoders` | The decoder seam, in ~20 lines per framework: Vercel AI SDK, LangGraph, and plain JSONL converging on one graph. |
| `integrations/otel-service-graph` | OpenTelemetry spans as the stream. Answers "what did the topology look like *during* the incident". |
| `integrations/electric-postgres` | The real thing: Electric over Postgres logical replication, live. |
| `integrations/react-timeline` | The read side — TanStack DB plus a recorded-time scrubber that re-renders the belief at any anchor. |
| `integrations/cloudflare-worker` | Per-agent Durable Objects on DO SQLite storage, and a reproduced account of why D1 is refused. |
| `demos/newsroom` | The flagship: reporters disagree, the desk reviews a merge plan before it lands, a burned source cascades. |

**What they do and do not prove.** Each package's README carries its own
limitations; the ones worth knowing up front:

- `electric-postgres` **has** been run against real Postgres + Electric
  containers, which is how the `inf` offset bug below was found. CI runs its
  offline fallback; the live path is a local runbook (`pnpm docker:up`).
- `cloudflare-worker` runs against real local `workerd` and passes
  `wrangler deploy --dry-run`, but no Cloudflare account was available: no real
  deploy, no real D1 database, no multi-isolate concurrency.
- `claude-agent`'s transcript fixture is hand-authored against the shipped
  `sdk.d.ts`, not captured from a live session. Live mode is wired and typechecks
  but has not run against a real model. `demos/newsroom`'s live reporters are in
  the same position.
- `react-timeline` was verified rendering in a real headless browser in its
  offline mode; its Electric mode typechecks against the installed client but has
  never been pointed at a live shape stream.
- `integrations/decoders`' LangGraph decoder is typed structurally against that
  framework's documented event shape — LangGraph is not installed — and says so.

Two bugs in this library were found by building these, and both are fixed:
`compareOffsets` could not order Electric's `"<lsn>_inf"` offset (which broke the
recommended tail-in-a-loop pattern on essentially every fresh start), and the
in-process Durable Streams stand-in ignored `pattern`, so glob subscriptions were
untested end to end. That is the argument for integrations that actually run.

Run the flagship Deep Survey convergence demo:

```bash
pnpm install
pnpm demo
```

That demo starts with Electric Deep Survey-shaped shared state (`wiki` rows and
`xrefs`), then projects the rows into TypeGraph so repeated semantic mentions
collapse to canonical concepts with source attribution. Concept **extraction**
uses substring alias matching; entity **resolution** (merging entry-scoped
concepts into canonical ones) uses TypeGraph's fulltext similarity.

Run the lower-level stream mechanics demo (consume → resume → per-agent time
travel → merge):

```bash
pnpm demo:mechanics
```

Run the exactly-once variant — projection and cursor advance in one transaction,
so a crash leaves nothing to re-deliver:

```bash
pnpm demo:exactly-once
```

Run the same convergence projection against a live Electric Deep Survey swarm:

```bash
DARIX_URL=http://localhost:4437 DEEP_SURVEY_SWARM_ID=<swarm-id> pnpm demo:deep-survey-live
```

The live adapter dynamically loads `@electric-ax/agents-runtime/client`, observes
`wiki-swarm-${DEEP_SURVEY_SWARM_ID}`, reads the real `wiki` and `xrefs`
collections, and feeds them through the same TypeGraph projection.

To create the upstream swarm, run Electric's Deep Survey example in another
checkout:

```bash
git clone https://github.com/electric-sql/electric.git
cd electric
pnpm install && pnpm --filter @electric-ax/agents-runtime build
npx electric-ax agents quickstart
cd examples/deep-survey
pnpm run dev:server
pnpm run dev:ui
```

Start a survey from the UI, then pass the orchestrator entity id as
`DEEP_SURVEY_SWARM_ID`. If you already know the shared-state stream id, pass it
directly with `DEEP_SURVEY_SHARED_STATE_ID=wiki-swarm-...`.

## Wiring real Electric

`mockShapeSource` and `electricShapeSource` implement the same `ShapeSource`
seam, so the consumer is unchanged between tests and production:

```ts
import { electricShapeSource } from "@nicia-ai/agent-stream-graph";

const source = electricShapeSource({
  name: "crm-agent",
  url: "http://localhost:3000/v1/shape",
  params: { table: "agent_events", where: "agent = 'crm-agent'" },
  timeoutMs: 30_000, // bound the wait for the batch's up-to-date message
  // Map one change message to a `ShapeChange` minus `offset` — the adapter
  // stamps the authoritative Electric resume offset for you.
  toChange: (message) => ({
    shape: "person",
    key: message.key,
    operation: message.headers.operation,
    value: message.value,
  }),
});
```

`electricShapeSource` lazily imports `@electric-sql/client` (an optional
dependency) and drains one catch-up batch per `read()` (subscribe → buffer change
messages → tag them with `ShapeStream.lastOffset` on the `up-to-date` control
message → resolve). Call it in a loop to keep tailing a live shape.

**Offset semantics.** Electric does not provide per-message resume offsets —
every change in a catch-up batch shares the batch's `ShapeStream.lastOffset`.
The adapter tags all changes in the batch with that one offset, so
`anchorFor(stream, offset)` reconstructs the belief as of the **end** of the
batch, not after each individual change. Intermediate within-batch states are
still in the belief store's history (each projector write recorded its own
anchor) but are reachable by recorded time, not by stream offset.

**Resuming and the shape handle.** Electric resumes a shape from `(offset,
handle)` — a real offset without its handle is rejected. Within one process the
adapter captures the handle after each batch and threads it into the next
`read()` automatically, so tailing in a loop just works. To resume mid-shape
across a process restart, persist the handle from the `onHandle` callback (next
to your checkpoint) and pass it back as `handle`. Without a persisted handle a
cold start re-fetches the shape from the beginning — safe and idempotent (the
projector dedups), but it re-streams the shape once.

**Control messages.** The adapter handles three Electric control messages:
`up-to-date` (resolve the batch), `must-refetch` (rejects with
`ElectricMustRefetchError` — drop any persisted handle and call
`read(undefined)` to re-fetch from the start), and `error` (rejects with
`ElectricControlError`). Unknown control messages are ignored for forward
compatibility.

**Timeout.** `read()` rejects if the batch does not reach `up-to-date` within
`timeoutMs` (default 30s; pass `Infinity` to disable). Without a bound a stalled
Electric server would hang the consumer forever.

The adapter unit test covers the client message shape, but a live Electric
service is still required to verify your deployment's URL, shape params, and
proxy/cache configuration.

## Wiring Durable Streams

`durableStreamSource` is the third implementation of the same `ShapeSource` seam,
over an [Electric Durable Stream](https://electric.ax/primitives/durable-streams)
rather than a Postgres-backed shape. It lazily imports `@durable-streams/client`
(an optional dependency).

```ts
import { durableStreamSource } from "@nicia-ai/agent-stream-graph";

const source = durableStreamSource({
  name: "agents/crm-agent",
  url: "http://localhost:8791/agents/crm-agent",
  headers: { Authorization: `Bearer ${token}` },
  toChange: (note) => ({
    shape: "person",
    key: note.id,
    operation: "insert",
    value: note,
  }),
});
```

**Per-message offsets.** This is the reason to prefer it. The protocol addresses
whole appends, so message-level positions use its own two-part form — a base
offset plus a count of messages past it, the same pair `Stream-Fork-Offset` /
`Stream-Fork-Sub-Offset` use — rendered as one `"<base>,<n>"` cursor string. Each
change therefore gets its own resumable offset, and
`store.asOfRecorded(anchorFor(stream, offset))` reconstructs the belief as of
**that single change** rather than as of the end of a batch.

The count is measured from the offset the read was *issued* at, never from a
batch boundary, so it does not depend on how the server chunks its responses:
resuming from `<base>,<n>` re-reads from `base` and drops `n` messages, and any
chunking of those messages leaves the same survivors. The last message of each
batch is relabelled onto that batch's end offset, so the count resets at every
boundary instead of growing without bound.

**Granularity.** `granularity: "append"` gives every message in an append that
append's end offset instead, so `consume` applies the whole append in one
transaction under one anchor — the same trade an Electric batch makes, for when
bulk catch-up matters more than per-message replay. The default is `"message"`.

**Retention.** A stream may drop data below its earliest retained position; a read
below it rejects with `DurableStreamRetentionError` (HTTP `410 Gone`). Recover by
discarding the checkpoint and calling `read(undefined)` — the start sentinel
means "everything still retained", and an idempotent projector converges on the
replay.

### State Protocol streams

`durableStateSource` reads a [State Protocol](https://electric.ax/primitives/durable-streams)
stream — the typed-CRUD layer Electric's agents runtime writes — whose record is
already nearly a `ShapeChange`:

```ts
import { durableStateSource } from "@nicia-ai/agent-stream-graph";

const source = durableStateSource({
  name: "agents/crm-agent",
  url: "http://localhost:8791/agents/crm-agent",
  types: ["person", "company"],      // omit to take every record type
  granularity: "transaction",
});
```

**`granularity: "transaction"`** is the reason to prefer it. State Protocol puts
the writer's own `headers.txid` on every record, so every change from one source
transaction takes a single offset — and `consume` then applies that whole
transaction in one TypeGraph transaction under one recorded anchor. Recorded
time lines up with the writer's commits instead of with transport chunking, and
`anchorFor` can never reconstruct half a source transaction. Grouping runs after
the full catch-up is collected, so a transaction split across transport chunks
still lands in one group; records without a `txid` fall back to per-message.

`upsert` — State Protocol's fourth operation — arrives as `update`. `Operation`
describes what the projector must *do*, and an idempotent projector cannot
distinguish insert from update anyway.

**Control frames.** `reset` rejects with `StateResetError` (drop the checkpoint,
re-materialize). `snapshot-start` / `snapshot-end` are **dropped**, which is
correct for a snapshot that only adds or supersedes and wrong for one that
implies deletions — reconciling "this is the complete state" needs a diff against
the belief, not a run of upserts. Until that exists, don't point this at a stream
whose snapshots prune.

### Forking a stream at a checkpoint

Because a cursor *is* the protocol's `(offset, sub-offset)` pair, the stream can
be branched at exactly the position a belief graph was last anchored to:

```ts
import { forkPointFor, forkStream } from "@nicia-ai/agent-stream-graph";

const cursor = await checkpoints.lastOffset("agents/crm-agent");
await forkStream({
  url: "http://localhost:8791/agents/crm-agent-what-if",
  sourcePath: "agents/crm-agent", // stream-root-relative, per the protocol
  at: forkPointFor(cursor!),
});
```

Reads on the fork return the source's prefix followed by the fork's own appends,
and a fork inherits its source's offsets — so the same cursor string addresses
the same message on both. Forking is idempotent (`created: false` when an
identical fork exists) and does **not** inherit producer state.

`@durable-streams/client@0.2.6` exposes no fork API, so `forkStream` speaks the
protocol directly over `fetch`; it works whether or not the optional client is
installed.

`pnpm demo:fork-merge` runs the whole loop: fork at a checkpoint → let the
branches diverge → materialize each into its own belief graph → reconcile them
with TypeGraph's `mergeIncremental`, conflicts flagged rather than silently
resolved.

The demo merges in one call. If a human or a policy check should see the write
set before it lands, `planMergeIncremental` takes the same arguments and returns
a JSON-serializable `MergePlanArtifact` — a digest, a `proposed` summary, and
the bound write set — without touching the target; `applyMergePlan(target, plan)`
then commits it exactly once, refusing a plan whose target moved since it was
built. That is the natural shape for reviewing an agent's belief before it
reaches canonical.

Four things about the reconciliation itself are worth knowing before you copy
the demo's `exportGraphStream` → `importGraphStream` → `mergeIncremental` shape:

- **A branch that ends a row's validity merges that ending.** Several branches
  ending the same row at different instants is *not* a conflict — an ending is a
  monotone claim, like a deletion, so the earliest end wins silently. Read
  `MergeReport.validityEnds` if you need to see that the arbitration happened.
  It reports rows INHERITED from the fork point, so a merge against an empty
  fork point — every row branch-created, its window travelling with its
  create — cannot populate it.
- **A base-vs-branch conflict reports only the INCOMING value.** When a branch's
  value conflicts with a row already committed to the target (rather than with
  another branch in the same merge), `PropertyConflict.values` carries just the
  incoming branch's contribution; the value that was KEPT is in `.resolution`,
  not duplicated into `.values` under a synthetic base entry. Same-wave
  branch-vs-branch conflicts do list both sides, so code written against that
  shape reads a base conflict as one-sided and silently prints half of it. Two
  independent demos in this repo made exactly that assumption before checking.
  Relatedly, `readProvenance` rows DO carry a synthetic `__committed_base__`
  branch id meaning "this row already existed" — filter it out when you are
  listing contributors, or a pre-existing row acquires a phantom author.
- **Stage an agent's belief into an `ingestionBranch`, not a plain `branch`.**
  A branch inherits the fork point's rows *and* its node uniqueness constraints,
  so staging a belief that holds an ALIAS of a canonical row — same unique value,
  different id, which is the case entity resolution exists to reconcile — fails
  on the uniqueness constraint before merge planning ever sees it. The demos
  don't hit this only because they fork from an empty graph; a real incremental
  pipeline forks from a populated canonical. `ingestionBranch(forkPoint,
  makeBackend)` defers node uniqueness to the resolved write set, so aliases
  reach candidate generation and the constraints are enforced once the merge has
  decided what the rows mean. `importGraphStream` and `importGraph` take the
  handle directly, so staging is the same one-liner as before and the importer
  keeps owning node-first ordering, validity windows, and edge endpoint order:

  ```ts
  const staged = unwrap(await ingestionBranch(forkPoint, makeBackend, { id: branchId }));
  await importGraphStream(staged, exportGraphStream(belief, { includeTemporal: true }), {
    onConflict: "update",
  });
  const result = await mergeIncremental({ forkPoint, target: canonical, branches: [staged], options });
  await staged.close();
  ```

  The handle withholds the branch's `Store` — no schema evolution, no
  transactions — so a projector that stages through `store.transaction(...)`
  rather than through interchange still wants an ordinary `branch()`. On an
  identity-enabled graph the handle also carries an assertion-only `identity`
  facade, so explicit `same` / `different` evidence can be staged alongside the
  aliases it explains.
- **Give the export and the import their own backend handles.** An export stream
  holds one snapshot transaction and, on a single-connection backend, an
  exclusive lease on that connection. That covers any better-sqlite3 handle, a
  PGlite connection, and a Postgres pool capped at one — including the spellings
  that arrive as strings (`max: "1"` from an env var, `?max=1` in the URL,
  `PGMAX=1`). A cap TypeGraph cannot see, or a detection that is wrong for your
  topology, is declarable with `serializedResource` on the backend constructor.
  Piping an export into an import that shares the handle is refused with a typed
  `ConfigurationError` rather than hanging both, and branch cloning materializes
  the whole export in memory rather than streaming it — which matters for a
  large fork point. The demos open a fresh backend per store, which is why they
  are unaffected. If your consumer might walk away mid-stream — anything that
  races `next()` against a timeout — pass `idleTimeoutMs` so an abandoned export
  rolls its snapshot back and frees the lease instead of holding both for the
  life of the process.
- **An `includeTemporal: true` export replayed over rows that already exist
  reports them rather than quietly updating their props.** The document's
  `validFrom` cannot apply to a live row (see
  [Valid time on events](#valid-time-on-events)), so an import onto a *non-empty*
  branch fails those rows. Import into a branch off an empty fork point, export
  with `includeTemporal: false`, or drop `validFrom` from the update document.

### Pull-wake subscriptions

Instead of polling every stream on a timer, let the server say which ones have
work. `consumeSubscribed` claims the subscription's lease, drains each pending
stream through `consume`, then acks and ends the claim:

```ts
import { consumeSubscribed, ensureSubscription } from "@nicia-ai/agent-stream-graph";

await ensureSubscription({
  rootUrl: "http://localhost:8791",
  id: "materializer",
  pattern: "agents/*",
});

const { streams, nextWake } = await consumeSubscribed({
  subscription: { rootUrl: "http://localhost:8791", id: "materializer" },
  worker: "materializer-1",
  sourceFor: (stream) => durableStreamSource({ /* … */ }),
  store: belief,
  checkpoints: book,
  project,
});
```

The server's `acked_offset` is **not** the resume cursor. It addresses whole
appends, and it is written after processing rather than atomically with it, so it
can only be a wake-scheduling hint — the `CheckpointBook` stays authoritative.
Acks therefore carry the whole-append base of the last cursor, which understates
progress by at most one append: that costs a redundant wake and never a lost one.

If consuming throws, the lease is released rather than held, so another worker
can pick the work up immediately instead of waiting out `lease_ttl_ms`. A claim
the server has moved past rejects with `SubscriptionFencedError`; a claim held by
another worker rejects with `SubscriptionClaimedError`.

## Idempotency and no-op changes

At-least-once re-delivery is safe **when the projector is idempotent** — use
`upsertById` / `getOrCreateByEndpoints`, not `create`. The consumer does not
enforce this; it is the projector's contract.

Changes sharing one resumable offset are applied in **bounded transactions and
checkpointed once**: a run of N changes uses `ceil(N / maxBatchSize)`
`store.transactionWithReceipt()` calls — and that many recorded instants — but
advances the cursor a single time, at the run's offset boundary. The consumer
reads two independent signals off the receipts, at two different scopes:

| Receipt field | Scope | Question it answers |
| --- | --- | --- |
| `writes.total` | per change (`tx.measure`) | Did the projector actually write? |
| `recorded` | per batch | Which recorded instant did *this* transaction commit at? |

Batching matters because each **commit** consumes one revision of a strictly
monotonic per-graph counter. Only an offset boundary is ever addressable by
`anchorFor`, so committing per change inside a shared-offset batch buys no replay
granularity while burning a revision each. Sources with a distinct offset per
change are unaffected: each offset is its own run and still commits separately.

An anchor is `r1:<logical revision>:<wall-clock timestamp>`. The revision orders
commits; the timestamp is a non-decreasing high-water mark for the valid-time
axis of a diagonal `asOfRecorded` read. Treat the whole string as opaque —
`recordedInstantRevision`, `recordedInstantWallTime`, and
`compareRecordedInstants` are the supported accessors.

`maxBatchSize` (default `DEFAULT_MAX_BATCH_SIZE`, 1000) bounds the write-lock
hold and history capture buffer, since an Electric initial sync tags an entire
shape with one offset. It is a transaction-size bound only, never a checkpoint
boundary — a split run still checkpoints once, at its true offset boundary. It
must be a positive integer; anything else throws `InvalidMaxBatchSizeError`
before any change is read.

Per-change write counts come from a `tx.measure` scope rather than the batch
receipt, which aggregates: one dropped change among writing neighbours would
otherwise still total non-zero and pass unnoticed. A drop throws from *inside*
the transaction, so the whole batch rolls back rather than leaving a half-applied
run behind an unadvanced cursor.

An insert/update change is expected to write, so `writes.total === 0` means the
change was dropped: the consumer throws `ProjectorRecordedNothingError` rather
than silently advancing the cursor past it. The common causes are a projector
that wrote to a different store than the one it was handed, swallowed a write
error, or issued only an empty bulk write (`bulkCreate([])` counts 0, since bulk
methods count by input length).

**The trap this sets for decoder authors: filter unmodeled messages at the
SOURCE, not in the decoder.** A stream usually carries message types your graph
does not model — control frames, status pings, progress events, lifecycle
records. If a source turns each of those into a `ShapeChange` and the decoder
returns no events for them, every one is a dropped insert and throws. The fix is
not to invent a placeholder write; it is to never mint a `ShapeChange` for a
message the decoder does not model, which is exactly what `electricShapeSource`
does with `up-to-date` and `must-refetch`. Decide what is in the stream at the
adapter boundary, and let the decoder assume everything it receives is
meaningful. Belt and braces: make at least one write unconditional for every
message type you DO model, so a modeled message can never decode to nothing
because of the particular shape of its payload.

The offset's anchor is `receipt.recorded` — the instant that transaction
allocated — **not** `store.recordedNow()`. The recorded clock is graph-global:
any concurrent writer advances it, so reading it after the commit can hand back
someone else's instant and anchor the offset to a belief this stream never
produced.

A `delete` is allowed to record nothing (deleting an already-absent key is a
legitimate no-op). It completes a write intent (`writes.total === 1`) but
captures nothing (`recorded === undefined`), so its offset carries the prior
anchor forward: `anchorFor(offset)` reconstructs the same belief as the previous
offset and re-delivery stays a no-op. When that no-op `delete` is the very first
change (empty graph, no prior anchor), the checkpoint is skipped so re-delivery
re-processes.

Create belief stores with `coalesceUnchangedUpserts: true`: a re-delivered
byte-identical row then coalesces to a true no-op (same `writes.total === 1`,
`recorded === undefined` shape as a no-op delete, handled unchanged), so
at-least-once re-delivery and replays stop rewriting rows and churning history.
It removes *re-delivery* churn, not the cost of a full replay-from-zero that
re-walks superseded intermediate values (replaying an old value over the current
one is a real change).

## Valid time on events

`node.upsert` and `edge.upsert` carry an optional valid-time window. Stream
**event** time belongs there; **ingest** time is recorded time and stays the
store's to assign. One rule covers nodes and edges:

> `validFrom` opens the window on the write that **creates** the row (or
> resurrects a tombstoned one). A later event never rewinds a live row's start.

The end has three states, and the difference matters to a projector:

| the event says | meaning |
| --- | --- |
| nothing | the stored end stands |
| `validTo` | the fact stopped being true at that instant |
| `clearValidTo: true` | it is true again — reopen the window |

The last two are mutually exclusive, in this library's types and in the store.

By default TypeGraph refuses a `validFrom` naming an instant other than the one
a live row holds — right for a caller stating a bound once, wrong for a replayed
stream, which re-states the start of a row it already created on every
redelivery and would otherwise roll back the batch, leave the cursor unadvanced,
and re-deliver the same change forever. Two things make the rule above hold
instead:

- **Both** node and edge writes carry `onImmutableLowerBound: "preserve"`, so a
  live update keeps the bound the row already holds while applying props and
  `validTo` — one statement, no error to catch.
- **Edges** additionally pass `ifExists: "update"`. The default `"return"` hands
  back the existing edge and writes *nothing*, which would drop every event
  revising a relationship's props or closing its window. Re-delivery stays
  idempotent because the match is by endpoints: a replay finds the row it wrote
  last time, an already-ended one included, rather than minting a parallel
  edge.

Preserving a lower bound is not the same as ignoring valid time. A stated bound
is validated on every write, so a malformed instant, or a stated pair that ends
before it starts (`INVERTED_VALIDITY_WINDOW`), is refused and surfaces as a
failed batch.

**A resumed fact is one row, not two.** `clearValidTo: true` reopens a closed
window in place: the row keeps its id *and* its original `validFrom`, so a
resumption is not a duplicate entity.

Be precise about what that means for valid time, because it is easy to read too
much into. A row carries ONE window, so reopening **extends** that window across
the interruption rather than recording two disjoint spans. A plain valid-time
read at a coordinate inside the interruption therefore returns the row as valid
once the reopening has been applied: `asOf(mid-gap)` answers "not employed"
before the reopening lands and "employed" after it does. `pnpm demo:valid-time`
asserts exactly that.

The interruption is not lost, though — it is recoverable by asking both axes at
once. Views compose in the order valid-time-then-recorded:

```ts
// "was she employed mid-gap, as far as we knew before the rehire was filed?"
const view = store.asOf(midGap).asOfRecorded(anchorBeforeRehire);
await view.edges.worksAt.getById(edgeId); // undefined — the gap, reconstructed
```

Note the `getById`: a `RecordedStoreView` is a reconstructing read and exposes
only `query`, `subgraph`, graph algorithms, and `getById` / `getByIds` / `scan`
— no `findByEndpoints`. So capture the row's id from a live read first, then
address it on the composed view.

If the interruption is a fact you need to query in valid time ALONE, without
pinning recorded time, model the two spans as two rows: `clearValidTo` is for a
fact that RESUMED, not for one with a hole in it.

Reopening an already-open row is a no-op, and a replayed reopening coalesces like
any other unchanged write.

```ts
emit.edges.worksAt.upsert(person, company, undefined, { validTo: leftAt });
// …later in the stream, they come back:
emit.edges.worksAt.upsert(person, company, undefined, { clearValidTo: true });
```

**A fact that arrives already historical needs no `validFrom`.** One event that
both creates a row and ends it in the past stores no lower bound at all —
"ended at T, start unknown" — and reads back at every coordinate before its end.
`meta.validFrom` is `undefined` for such a row.

**Building events from Zod-validated input? Reconstruct, don't pass through.**
`exactOptionalPropertyTypes` and Zod's `.optional()` disagree in a way that bites
exactly here. Zod infers `validTo?: string | undefined`; `ValidTime` declares
`validTo?: string`. Under `exactOptionalPropertyTypes` those are *different
types*, so a Zod-validated object is not assignable to a `GraphEvent` — and
because the event types are distributive unions, TypeScript tends to report the
mismatch against the wrong union arm, which makes it hard to read. Rebuild the
event with the same spread-to-omit idiom this library uses internally rather than
widening the type:

```ts
const valid = {
  validFrom: input.validFrom,
  ...(input.validTo === undefined ? {} : { validTo: input.validTo }),
};
```

**But an event that ends a row an EARLIER event created still needs one.** A row
created without a stated start gets the *ingest* instant as its lower bound, so
a later change closing the window at a past instant describes a row that stopped
being true before it started, and is refused with `INVERTED_VALIDITY_WINDOW`.
Replaying a historical log into a fresh graph hits this whenever a fact's
creation and its ending arrive as two changes — so if your stream carries event
time, emit `validFrom` on every event, not just the closing one.

## Exactly-once

`consume()` is at-least-once: projection and cursor advance commit separately, so
a crash re-delivers and an idempotent projector converges. When the source gives
a resumable offset **per change** (a Postgres LSN changefeed, not an Electric
batch), you can go exactly-once by committing the projection and the cursor
advance in **one** transaction — a crash then leaves nothing to re-deliver.
`typeGraphAdoptingCheckpoints(adapterStore)` returns an
`AdoptingCheckpointBook` whose `recordIn(externalTx, …)` advances the cursor
inside a caller-owned transaction that the belief projection (via
`store.withRecordedTransaction`) also enlists in. See
[`examples/exactly-once.ts`](examples/exactly-once.ts).

## API

| Export | Purpose |
| --- | --- |
| `consume(args)` | Resumable, idempotent, checkpointing consumer (at-least-once). |
| `ShapeSource<V>`, `mockShapeSource<V>`, `electricShapeSource<V>`, `durableStreamSource<V>`, `durableStateSource<V>` | The transport seam (value type `V` defaults to `Record<string, unknown>`). |
| `Projector<G, V>` | `(store, change) => Promise<void>` over graph `G` — must be idempotent. |
| `ProjectorRecordedNothingError` | Thrown when an insert/update change records no write. |
| `CheckpointBook`, `typeGraphCheckpoints`, `checkpointGraph` | Portable durable offset ↔ anchor bookkeeping. `record` opens its own transaction; `lastOffset` is an O(1) per-stream high-water read. |
| `AdoptingCheckpointBook`, `typeGraphAdoptingCheckpoints` | Additive exactly-once API. `recordIn(externalTx, …)` enlists the checkpoint write in a caller-owned adapter transaction. |
| `compareOffsets`, `composeOffset`, `parseCompositeOffset` | Offset ordering across numeric-tuple, opaque, and composite `<base>,<n>` forms. |
| `forkStream`, `forkPointFor` | Branch a durable stream at the position a checkpoint names. |
| `ensureSubscription`, `claimSubscription`, `ackSubscription`, `releaseSubscription`, `deleteSubscription`, `consumeSubscribed` | Pull-wake subscriptions: let the server say which streams have work. |
| `ElectricMustRefetchError`, `ElectricControlError` | Typed errors from the Electric adapter. |
| `DurableStreamRetentionError`, `StreamForkError`, `SubscriptionClaimedError`, `SubscriptionFencedError`, `SubscriptionRequestError` | Typed errors from the Durable Streams adapters. |
| `ShapeChange<V>`, `Operation` | The change-message contract. |

## Limitations

- **One consumer per belief store.** A change's replay anchor is its
  transaction's own `receipt.recorded`, so concurrent writers are otherwise
  fine — except the carry-forward anchor for a leading no-op change (a no-op
  delete, or a coalesced re-delivery after a crash) is seeded from
  `store.recordedNow()`, which is graph-global. A second writer advancing that
  clock between the crash and resume would mis-anchor such an offset. Point one
  consumer at a belief store.
- **Recorded time is ingest order.** Anchors reflect when a change was
  _consumed_, which is faithful for a live forward stream. Replaying a
  _historical_ backfill collapses its recorded instants to "now" — recorded
  time is not source/event time.
- **The graph-event layer needs a backend with transactions.** Enforcing a
  declared constraint means committing a reservation row together with the row
  it gates, which a backend with no transactions cannot roll back. On one that
  reports none — Cloudflare D1, `drizzle-orm/neon-http`, a SQLite backend built
  with `transactionMode: "none"` — the affected write is refused with
  `CONSTRAINT_WRITE_FENCE_UNSUPPORTED` rather than run unfenced. Since TypeGraph
  0.50 that covers more than edges:
  - **`edge.upsert`**, always. It is endpoint-matched, and both the match-key
    fence and the unchanged-replay coalescing that fence enables are
    transaction-scoped.
  - **`node.upsert` onto a kind that declares a unique constraint**, but only on
    the two legs that have to MOVE the reservation: an update that changes the
    constraint's key, and a resurrect. Both claim before the row they gate, so
    there is a reservation with nothing to undo it. Measured on 0.51, reason
    `nodeUniquenessClaim`. Not refused: the create, a delete, an update that
    leaves the key alone — which is what a re-delivered identical row is — and
    any kind declaring no unique constraint at all.
  - **A `disjointWith` partner or a non-`many` edge cardinality anywhere in the
    graph**, which also refuses `importGraph` / `importGraphStream` up front —
    relevant to the fork/merge and provenance demos' interchange steps, not to
    `consume()` itself.

  In practice you reach none of this through the documented setup, because
  `createStoreWithSchema` is itself refused on a transactionless backend:
  committing a schema version needs an atomic transaction. Such a deployment has
  to apply `generateSqliteMigrationSQL` output out of band and open the graph
  with the unmanaged `createStore`. Every backend the demos use reports
  transactions, so none of this fires there.
- **Under Electric, checkpoint granularity is per batch, not per change.**
  Electric does not provide per-message resume offsets; changes in one catch-up
  batch share the batch's `lastOffset`. `anchorFor(stream, offset)` reconstructs
  the belief as of the end of that batch, not after an individual change within
  it — and since such a batch also commits as one transaction, no intermediate
  state is observable in recorded time either. `durableStreamSource` does not
  have this limitation: at the default `"message"` granularity every change is
  independently addressable, at one transaction per change.
- **Checkpoint rows are retained for replay.** Every `(stream, offset)` gets a
  `Checkpoint` row so any past offset can be replayed; a `Stream` high-water row
  backs the O(1) `lastOffset`. There is no compaction pass yet — long-running
  streams accumulate rows. Prune anchors older than the high-water minus N if
  you do not need deep replay.
- **The Electric adapter is not live-service tested IN CI.** Unit tests cover the
  `@electric-sql/client` message shape, control messages, and timeout, and CI
  does not run an Electric service. It has, however, been exercised against real
  Postgres + Electric containers via `integrations/electric-postgres` — which is
  how the `"<lsn>_inf"` offset bug was found, and where `ElectricMustRefetchError`
  was first seen firing on a genuine `ALTER TABLE` shape invalidation, with the
  documented `read(undefined)` recovery confirmed. Run it yourself with
  `pnpm --filter @nicia-ai/asg-electric-postgres docker:up` and then its `demo`.
- **The Durable Streams adapters run against an in-process server, not a real
  one.** The tests drive the real `@durable-streams/client` against a faithful
  in-memory implementation of the slice this package speaks (offset reads,
  forking, retention, subscriptions), which is enough to pin the offset and fork
  semantics but not your deployment's auth, retention policy, or CDN behaviour.
  Subscription support in particular is written to the protocol spec and has not
  been exercised against a hosted server.

## Operating a long-running belief store

A belief store whose entity resolution leans on TypeGraph's fulltext or vector
strategies (like the Deep Survey demo's concept matching) accrues strategy-owned
index state alongside the graph. TypeGraph's maintenance operations form one
escalation ladder, and **nothing in `consume()` climbs it for you**:

| Call | Writes | Use it for |
| --- | --- | --- |
| `store.probeContributions()` | nothing | "is search coherent with the graph right now" — safe on a read path, a replica, or a least-privilege role. Returns one `ready` / `degraded` entry per projection plus the `graphRevision` the assessment was taken at (on a history/revision-tracked store). |
| `store.verifyContributions()` | — | the detailed audit: `missing-marker`, `orphaned-marker`, `failed-materialization`, `stale`. Shares the probe's detection logic, so the two can never disagree. |
| `store.repairContributions()` | non-destructively | re-audits and retries what it safely can. Reports a `stale` projection as `requires-rebuild` and never rebuilds one itself. |
| `store.verifyConstraintFences()` | nothing | the constraint-violation audit for a store that predates the 0.50 claim relations — duplicate scoped uniques, disjoint namesakes, extra `cardinality: "one"` edges. Read-only by design; see the note below. |
| `store.rebuildContribution("fulltext")` | **destructively** | the one repair for `stale` — storage at a shape the current DDL no longer produces. Drops, recreates, and reconstructs the content from the node rows in one transaction. |

Probe periodically against a store that has been consuming for a long time, or
after restoring one from a backup, and clear any finding before trusting
fulltext-backed entity resolution on it.

A belief store that predates TypeGraph 0.50 may also carry constraint
violations the claim relations were introduced to prevent. The claims refuse the
second live claimant of an axis from the first write after the upgrade onward,
but they repair nothing already committed — so two live siblings sharing a
`scope: "kindWithSubClasses"` key, an id live under both kinds of a
`disjointWith` pair, or two live `cardinality: "one"` edges out of one source
all survive the upgrade in place. `store.verifyConstraintFences()` is the
read-only audit that finds them: it reads the relation each constraint is
_declared over_, never a claim key, because a pre-0.50 database holds no claim
rows at all and a claim scan would report zero violations on exactly the data
worth finding. It writes nothing — choosing which claimant keeps an axis is a
data-loss decision that belongs to you. Until you make it, the next write
touching such an axis is refused with the ordinary typed `UniquenessError` /
`DisjointError` / `CardinalityError` naming the incumbent, which for a projector
means a batch that aborts and re-delivers forever.

> Needs TypeGraph **>= 0.51.1** if any node kind declares a unique constraint
> that leaves `scope` or `collation` to default. Before that, the schema
> document's reader required both to be present while the writer omitted them,
> so this audit — along with `getActiveSchema()`, `store.requiresMigration()`
> and `store.schemaChanges()` — threw `DatabaseOperationError: Stored schema
> document is malformed` on a database TypeGraph itself had written
> ([nicia-ai/typegraph#525](https://github.com/nicia-ai/typegraph/issues/525)).
> 0.51.1 fixes it on the reader by applying the documented defaults, so a
> database written by an older version is repaired by upgrading alone — no
> migration and no schema rewrite.

A belief store that ingested valid-time windows under an older TypeGraph may
also hold rows written with a backwards window (`valid_from > valid_to`), which
are readable at no coordinate at all. `store.repairInvertedValidityWindows({
mode: "report", relations: "live-and-recorded" })` counts them without writing;
`mode: "apply"` normalizes them to "ended at T, start unknown", the shape
today's writes store. Repair with writers stopped, pass
`relations: "live-and-recorded"` so the recorded twin cannot re-materialize the
invisible row at an `asOfRecorded` coordinate, and re-baseline any outstanding
merge branch afterwards — `valid_from` is part of the `base@V` fingerprint.

Two limits on the destructive rung. Vector contributions are **not** rebuildable
— TypeGraph stores the vectors you supply and never the inputs that produced
them, so `reembedVectorField(kind, fieldPath, { embed })` is the sanctioned path
and `rebuildContribution` refuses with `ContributionRebuildUnsupportedError`.
And the fulltext projection is one physical table shared by every graph on the
database: a `stale` rebuild whose storage still holds another graph's rows
refuses with the reason `shared-storage-in-use` rather than destroying content it
cannot reconstruct. If you run several belief graphs on one database, that
refusal names the other graph ids and the maintenance-window sequence to follow.

## Releasing

Published to npm by `.github/workflows/release.yml` on a `v*` tag (or a manual
dispatch), using **npm trusted publishing**: the workflow requests an OIDC token
via `permissions: id-token: write` and npm exchanges it for a short-lived
credential, so there is no `NPM_TOKEN` in the repository or the organization.
`NPM_CONFIG_PROVENANCE` attaches a provenance attestation, which npm accepts
only for a public source repository.

Two things must be true before the first publish, and neither is in this repo:

1. The package is registered on npmjs.com with this repository **and this
   workflow filename** as a trusted publisher. Renaming `release.yml` breaks
   publishing until the registration is updated to match.
2. `publishConfig.access` is `public` in `package.json` — a scoped package is
   private by default and the first publish fails without it.

Both CI and the release workflow pack the tarball, install it into a sandbox
**outside the checkout**, and import it under plain `node`. The sandbox location
is load-bearing: Node resolves by walking parent directories, so a sandbox
inside the repo finds the repo's own `node_modules` and the smoke would pass on
a package missing a dependency it only appeared to have. Only required peers are
installed, so the run also proves the transport clients are imported lazily
rather than at module load.

That gate exists because the gap it covers is not hypothetical. `tsc` emits
relative specifiers verbatim, so extensionless imports in `src/` shipped as
extensionless imports in `dist/`, which Node's ESM resolver refuses — the
package was unimportable while `pnpm typecheck`, `pnpm test`, and every demo
stayed green, because those reach the code through vitest and tsx, which resolve
specifiers Node does not. Relative imports in `src/` therefore carry explicit
`.js` extensions.

## License

MIT
