# @nicia-ai/asg-cloudflare-worker

A Cloudflare Worker that accepts agent events over HTTP and materializes each
agent's belief graph into a **Durable Object's own SQLite storage**, using
[`@nicia-ai/agent-stream-graph`](../..)'s published entrypoint
(`import ... from "@nicia-ai/agent-stream-graph"`, never `../src`) and
[`@nicia-ai/typegraph`](https://www.npmjs.com/package/@nicia-ai/typegraph)
0.54's `"do-sqlite"` transaction mode.

The root README's "Limitations" section says the graph-event layer needs a
backend with transactions, and names Cloudflare D1 as one that reports none —
refused with `CONSTRAINT_WRITE_FENCE_UNSUPPORTED` rather than run unfenced.
That's true, and `src/d1-refusal.ts` reproduces it end to end. But **Durable
Object SQLite storage is not D1** — it is a real transactional SQLite backend,
and this package's main job is to show the working shape, not just the
refused one.

**This is a documented TypeGraph feature (since 0.52), not something found by
accident.** `node_modules/@nicia-ai/typegraph/dist/backend/sqlite/index.d.ts`
states it directly on `SqliteBackendOptions.executionProfile`: *"Durable
Objects (`drizzle(ctx.storage)`) auto-detect `transactionMode: "do-sqlite"`
and do not need a hint."* `"none"` — no transactions, the D1 case — is a
separate, distinct profile the same detection assigns to a
`drizzle-orm/d1` connection. `src/backend.ts` relies on exactly this
auto-detection and passes no hint of its own.

## Why a Durable Object per agent

This library's own documented limitation is **"one consumer per belief
store"**: a change's replay anchor is seeded from `store.recordedNow()`,
which is graph-global, so a second concurrent writer can mis-anchor a
carry-forward checkpoint after a crash. Every worked example in this repo
gets that for free because it's a single Node process talking to one store.

A Cloudflare Worker is not single-process — it's many isolates, anywhere,
handling requests for every agent at once. Naively, that's exactly the
"concurrent writer" scenario the library warns about. **Routing every
request for one agent to that agent's own Durable Object instance
(`idFromName(agentId)`) makes "one consumer per belief store" structural
instead of a rule an operator has to remember**: Cloudflare's runtime
guarantees a given Durable Object id has at most one active instance
processing requests at a time, anywhere in the network. The DO's input
gate serializes concurrent requests to the *same* agent; different agents
land on different objects with independent storage and run fully in
parallel. That is also why this shape fits the library's per-agent belief
model specifically well, not accidentally — TypeGraph's node/edge tables are
already multi-tenant by `graph_id` internally, but here each agent doesn't
even share a physical database with any other agent, so there is no
per-graph interleaving to reason about at all.

The tradeoff this buys: an agent's belief graph can never outgrow one
Durable Object's storage, and cross-agent queries ("which agents believe X")
need a fan-out over DO stubs rather than one SQL join — a real constraint,
not a free lunch. See "Limitations" below.

## What's proven here, and what isn't

| Claim | How it's verified |
| --- | --- |
| A TypeGraph store runs, with real transactions, on Durable Object SQLite storage | `wrangler dev` against local `workerd`, driven with real HTTP requests (transcript below) |
| Retried/duplicate requests are true no-ops | Same live transcript: POSTing the same `seq` twice yields `processed: 0` the second time |
| Per-agent isolation is physical, not just logical | Same transcript: two agent ids produce two separate `.sqlite` files under `.wrangler/state/v3/do/` |
| The Worker + DO bundle and validate for a real deploy | `npx wrangler deploy --dry-run` (transcript below) |
| D1 refuses the same constrained write TypeGraph's docs describe | `src/d1-refusal.ts`, run and captured (transcript below) — against a real transactionless SQLite backend, not a mock |
| This works against a **real Cloudflare account** — quotas, billing, edge routing, real D1 | **Not verified.** No account is available in this environment, and a live deploy was out of scope. Nothing here should be read as proof beyond what the transcripts show. |

## The wire protocol

```
POST /agents/:agentId/events
  { "seq": number, "events": GraphEvent[] }
  -> 200 { processed, fromOffset, lastOffset }   (ConsumeResult)
  -> 400 on a malformed body

GET  /agents/:agentId
  -> 200 { agentId, entityCount }
```

`seq` is the agent's own per-stream monotonic sequence number — it becomes
the change's resumable `offset` (`consumer.ts`'s `compareOffsets` handles
plain numeric-tuple offsets like `"1"`, `"2"`, `"10"` natively). A retried
POST with a `seq` at or behind the durable checkpoint is filtered out by
`mockShapeSource`'s own `read(after)` before it ever reaches the belief
store — the retry costs a request, not a write.

`events` is a `GraphEvent<BeliefGraph>[]` — the same four-op vocabulary
(`node.upsert` / `node.remove` / `edge.upsert` / `edge.remove`) `graphEmitter`
produces in `examples/emit.ts`, sent as plain JSON. The belief graph itself
(`src/graph.ts`) is deliberately small — one `Entity` node kind, one
`relatesTo` edge kind — so the demo is about the deployment shape, not an
elaborate domain model; swap it for your own.

## Files

- [`wrangler.jsonc`](./wrangler.jsonc) — the Worker + Durable Object config,
  using the current declarative `exports` form (`{"type": "durable-object",
  "storage": "sqlite"}`), confirmed against the installed wrangler's own
  `node_modules/wrangler/config-schema.json` rather than assumed from docs.
- [`src/worker.ts`](./src/worker.ts) — the Worker entry point: routes,
  request validation, and `idFromName(agentId)` dispatch to the DO.
- [`src/materializer.ts`](./src/materializer.ts) — `AgentMaterializer`, the
  Durable Object. One per agent; ingests batches, exposes a read snapshot.
- [`src/backend.ts`](./src/backend.ts) — wires a TypeGraph SQLite backend
  onto `ctx.storage` via `drizzle-orm/durable-sqlite`.
- [`src/graph.ts`](./src/graph.ts) — the belief graph, plus the zod wire
  schema and the reconstruction step that turns validated JSON back into a
  real `GraphEvent<BeliefGraph>` (see "Design notes" for why that step
  exists).
- [`src/d1-refusal.ts`](./src/d1-refusal.ts) — the honest counterpart,
  runnable standalone under Node.

## Running it

All commands from this directory (`integrations/cloudflare-worker`).

```bash
pnpm typecheck            # tsc --noEmit over src/ (Worker + DO code)
pnpm check                # wrangler deploy --dry-run — validates config, bundles, no account needed
pnpm dev                  # wrangler dev — real local workerd, real HTTP
pnpm typecheck:verify     # tsc --noEmit over src/d1-refusal.ts (Node types, not workerd)
pnpm verify:d1-refusal    # tsx src/d1-refusal.ts
```

### `pnpm check` — verified

```
 ⛅️ wrangler 4.127.1
────────────────────
Total Upload: 2864.56 KiB / gzip: 533.81 KiB
Your Worker has access to the following bindings:
Binding                                   Resource
env.MATERIALIZER (AgentMaterializer)      Durable Object

--dry-run: exiting now.
```

### `pnpm dev` — verified against real local `workerd`

```
$ curl -X POST localhost:8787/agents/agent-1/events -d '{"seq":1,"events":[
    {"op":"node.upsert","kind":"Entity","id":"alice","props":{"label":"Alice"}},
    {"op":"node.upsert","kind":"Entity","id":"bob","props":{"label":"Bob"}},
    {"op":"edge.upsert","kind":"relatesTo","from":{"kind":"Entity","id":"alice"},
     "to":{"kind":"Entity","id":"bob"},"props":{"label":"knows"}}]}'
{"processed":1,"lastOffset":"1"}

$ curl localhost:8787/agents/agent-1
{"agentId":"agent-1","entityCount":2}

$ curl -X POST localhost:8787/agents/agent-1/events \
    -d '{"seq":1,"events":[{"op":"node.upsert","kind":"Entity","id":"alice","props":{"label":"Alice"}}]}'
{"processed":0,"fromOffset":"1","lastOffset":"1"}      # same seq -> no-op, exactly once

$ curl -X POST localhost:8787/agents/agent-1/events \
    -d '{"seq":2,"events":[{"op":"node.upsert","kind":"Entity","id":"carol","props":{"label":"Carol"}}]}'
{"processed":1,"fromOffset":"1","lastOffset":"2"}

$ curl localhost:8787/agents/agent-1
{"agentId":"agent-1","entityCount":3}

$ curl localhost:8787/agents/agent-2                   # a different agent id
{"agentId":"agent-2","entityCount":0}                  # -> its own, empty, DO

$ curl -o /dev/null -w '%{http_code}\n' -X POST localhost:8787/agents/agent-1/events \
    -d '{"seq":"nope"}'
400
```

`.wrangler/state/v3/do/asg-cloudflare-worker-AgentMaterializer/` held two
distinct `.sqlite` files after this run, one per agent id — physical, not
just logical, isolation. (`.wrangler/` is dev-only local state; it's
git-ignored by this package's own `.gitignore` and not part of the deploy.)

### `pnpm verify:d1-refusal` — verified

```
=== 1. createStoreWithSchema refuses a transactionless backend outright ===
  refused: Schema writes and removal cleanup require atomic transactions, but
  this SQLite backend has transactions disabled. Configure a driver that
  supports transactions (better-sqlite3, libsql, bun:sqlite) to use schema
  commits.

=== 2. The documented workaround: unmanaged createStore over pre-applied DDL ===
  createStore succeeded — no schema commit, so no transaction was needed for
  THIS call.

=== 3. Unconstrained writes need no fence and keep working ===
  node.upsertById OK: alice, bob

=== 4. edge.getOrCreateByEndpoints IS fenced — refused with CONSTRAINT_WRITE_FENCE_UNSUPPORTED ===
  refused: This backend cannot fence a constrained write: enforcing a
  declared constraint requires a transaction — to scope the per-graph write
  lock to, and to commit a reservation row together with the row it gates —
  and this backend has no transactions.
  error.details = {"code":"CONSTRAINT_WRITE_FENCE_UNSUPPORTED","graphId":"cf_worker_belief","constraint":"edgeMatchKeyConvergence"}
```

That's a **real** transactionless SQLite backend (`executionProfile:
{transactionMode: "none"}` on `createSqliteBackend`, over a real, in-memory
better-sqlite3 database) refusing the same write the same way a real D1
binding would — `createSqliteBackend` auto-detects `drizzle-orm/d1` and
assigns it that exact profile, per its own doc comment ("Set
`transactionMode: 'none'` for drivers without transactions (e.g. Cloudflare
D1)"). No D1 database was opened — none is available without a Cloudflare
account — but nothing about the refusal path is simulated; it's the same
code TypeGraph runs for a real D1 connection.

## Deploying for real (not attempted here)

```bash
npx wrangler login                # once, interactively
npx wrangler deploy               # ships src/worker.ts + the AgentMaterializer DO
curl -X POST https://<your-worker>.<subdomain>.workers.dev/agents/my-agent/events \
  -d '{"seq":1,"events":[...]}'
```

No code change is needed to go from `wrangler dev` to a real deploy — the
`exports`-declared `AgentMaterializer` provisions its own SQLite-backed
storage on first use in production exactly as it did locally. What a real
deploy would additionally exercise, and this package does not: multiple
isolates actually running concurrently across Cloudflare's edge (`wrangler
dev` is one local process), the DO's real network-hop input-gate behavior
under contention, and anything involving quotas, billing, or auth — none of
that is available to verify here.

## Design notes

- **One Durable Object, two TypeGraph graphs, one physical SQLite database.**
  The root README's quick start opens the belief store and the checkpoint
  store on two separate `createLocalSqliteBackend()` calls — two files. A
  Durable Object has exactly one SQLite database, so `materializer.ts` opens
  `beliefGraph` and the library's own `checkpointGraph` on the **same**
  backend instance. This is not a hack: TypeGraph's node/edge/schema tables
  already carry a `graph_id` column and are multi-tenant by design — verified
  directly (not just inferred from the schema) by running both
  `createAdapterStoreWithSchema` calls against one `createLocalSqliteBackend()`
  locally before ever touching a Durable Object.
- **`transactionMode: "do-sqlite"` is real, and it is new.** TypeGraph (since
  0.52) auto-detects `drizzle(ctx.storage)` (`drizzle-orm/durable-sqlite`) and
  routes managed writes through `ctx.storage.transaction(async ...)` — the DO
  async storage-transaction runner — rather than SQL `BEGIN`/`COMMIT` (which
  `ctx.storage.sql.exec` cannot issue at all) or `ctx.storage.transactionSync`
  (which cannot span an `await`, and TypeGraph's write path does). This is
  the actual reason the Durable Object path avoids every refusal
  `d1-refusal.ts` demonstrates: this backend genuinely reports transactions.
- **`drizzle-orm` had to be added as a direct dependency.** The given
  `package.json` didn't declare it, only `@nicia-ai/typegraph` (which
  resolves its own nested copy for internal use). But `createSqliteBackend`
  takes a caller-constructed Drizzle database — any consumer of TypeGraph's
  drizzle adapters is necessarily a direct `drizzle-orm` consumer too, not
  just transitively. `better-sqlite3` and `@types/better-sqlite3` are added
  as **devDependencies**, needed only by `d1-refusal.ts` (Node-only, never
  bundled into the Worker). All three resolve today via this pnpm
  workspace's root `devDependencies` regardless of this package's own
  manifest (verified: `wrangler deploy --dry-run` and `tsc` both succeeded
  before this edit too) — the edit was not run through `pnpm install` per
  this task's constraints, so it documents the true dependency rather than
  changing what resolves in this checkout today.
- **`exactOptionalPropertyTypes` vs. zod's `.optional()` — a real trap.**
  `z.string().optional()` infers `field?: string | undefined` (the value
  itself may be `undefined`); this library's `GraphEvent` types declare
  `field?: string` (the key may be absent; no value is ever `undefined`).
  Under `exactOptionalPropertyTypes: true` these are genuinely different
  types, and a zod-validated object is **not** assignable to a `GraphEvent`
  directly — not even through a union of otherwise-correct variants; TS
  reported the mismatch against an unrelated arm of the union, not the one
  actually being compared, which cost real time to track down (see the
  minimal repro in this package's git history if you need to see it
  reasoned through). `graph.ts`'s `toGraphEvent` reconstructs each event
  field-by-field with the same spread-to-omit idiom `graph-events.ts` itself
  uses (`onImmutableLowerBound`/`validTimeOptions`) — this is not a cast past
  the type system, it's the same idiom applied at a second boundary.
- **The wire schema is written as flat, non-intersected zod objects on
  purpose.** An earlier version built each upsert variant as `z.object({...
  base}).and(endOfValiditySchema)` — a single schema covering both the
  "open end" and `clearValidTo` shapes via intersection with a union. That
  version failed to typecheck against `GraphEvent`'s union for the same
  `exactOptionalPropertyTypes` reason above, compounded by the intersection:
  TS would not distribute the intersection over the validity union before
  attempting assignability. Two fully separate flat variants per upsert op
  (six schemas total) typecheck cleanly with no such ambiguity.

## Limitations — read before trusting this further than it goes

- **No real Cloudflare account was available to verify this against.**
  Everything above is either a local `wrangler dev`/`workerd` run or a
  config-only `--dry-run`. Multi-isolate concurrency, real edge routing,
  billing/quota behavior, and a genuine D1 database are all unverified here.
- **The D1 refusal is reproduced, not observed against real D1.** The
  refusal path is real TypeGraph code running against a real (if in-memory)
  transactionless SQLite backend with the exact profile `createSqliteBackend`
  assigns to a `drizzle-orm/d1` connection — but no `drizzle-orm/d1` driver
  was ever instantiated, because doing so needs a live D1 binding this
  environment does not have.
- **No auth.** `src/worker.ts` accepts events from anyone who can reach the
  Worker's URL. A real deployment needs its own authentication in front of
  `POST /agents/:agentId/events` — nothing here provides it.
- **An agent's belief graph is bounded by one Durable Object's storage**
  (currently a few GB), and there is no cross-agent query surface — "which
  agents believe X" needs a fan-out over DO stubs this package doesn't
  implement. See "Why a Durable Object per agent" above for why that
  tradeoff is the point, not an oversight.
- **The belief graph here is intentionally tiny** (one node kind, one edge
  kind) so the deployment shape is what's on display. `src/d1-refusal.ts`
  reuses it for the same reason `otel-service-graph`'s decoder tests reuse
  its own small graph — not because it is a realistic agent-memory schema.
- **No retention or compaction story.** Every event ever ingested stays in
  the belief store's bitemporal history (`history: true,
  coalesceUnchangedUpserts: true` absorbs exact re-deliveries, but not
  genuine updates) — a long-lived agent will grow its DO's SQLite file
  without bound. Out of scope here, same as the rest of this repo's demos.
