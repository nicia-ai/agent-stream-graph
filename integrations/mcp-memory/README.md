# @nicia-ai/asg-mcp-memory

An MCP server that exposes an entity-resolved, time-travelable, source-justified
belief graph as three tools — `recall`, `believedAt`, `whySoFar` — built on
[`@nicia-ai/agent-stream-graph`](../..) and [`@nicia-ai/typegraph`](https://github.com/nicia-ai/typegraph).
It gives any MCP client (Claude Code included) durable memory it did not have to
build: a person can be observed under two different spellings by two different
sources and still resolve to one entity, a past belief can be reconstructed even
after it has been corrected, and a fact can be traced back to the sources that
support it — including whether one of them has since been retracted.

## The tools

- **`recall(entity)`** — resolved lookup. Give it a name, alias, email, domain,
  or id; get back the canonical entity and what is currently believed about it,
  including every alias that has collapsed into it. `recall("J. Doe")` and
  `recall("Jane Doe")` answer with the same person.
- **`believedAt(agent, offset)`** — time travel. What did a given observation
  stream (`agent`, e.g. `"linkedin-scrape"`) cause this memory to believe once
  it had reached `offset`? Reconstructs belief as of that point even though the
  current graph has since moved on — no other memory server does this, because
  it needs a bitemporal store underneath.
- **`whySoFar(entity, predicate)`** — provenance. Walks
  `Source --premiseOf--> Justification --derives--> Fact` backward from a fact
  (an entity plus a predicate, e.g. `{ entity: "jane.doe@acme.example",
  predicate: "verified" }`) and reports every source that supports it and
  whether that source has been retracted. A fact with no live support left is
  no longer held.

All three are read-only. Retraction itself (marking a source withdrawn, which
cascades to whatever it alone justified) is demonstrated in `demo.ts` and the
tests via `@nicia-ai/typegraph/provenance`'s `createRetractionCapability`
directly — it is not exposed as an MCP tool in this package. See
[Limitations](#limitations).

## How memory works

`src/graph.ts` defines the belief graph: `Person` and `Org` nodes keyed by a
normalized identity key (email / domain), so two observations of the same key
under different spellings land on the same row — the projector in `src/store.ts`
reads the existing row before writing and folds the new spelling into an
`aliases` array instead of creating a second entity. `Fact` nodes are never
written directly; they are derived from a `Justification`, which is premised on
one or more `Source` nodes, exactly the shape
[`examples/provenance-retraction.ts`](../../examples/provenance-retraction.ts)
in the parent package uses.

`src/store.ts` opens a file-backed, history-enabled TypeGraph store
(`createLocalSqliteBackend({ path })`) plus the durable checkpoint book from
`@nicia-ai/agent-stream-graph` that tracks each observation stream's offset
within it. On first open it seeds the store by `consume()`-ing the fixture
streams in `src/fixtures.ts`; on every later open it finds the checkpoints
already there and does nothing. Memory persists across server restarts because
the SQLite files do — this is asserted, not just claimed, by `demo.ts` step
(d) and by the `pnpm test` suite, both of which close the store and reopen it
from the same files mid-run.

## Run it

```bash
pnpm install   # from the repo root, if you haven't already
cd integrations/mcp-memory

pnpm typecheck   # strict, clean
pnpm test        # vitest: tool functions + a real stdio JSON-RPC smoke test
pnpm demo        # the story below, with assertions, exit 0
pnpm start       # the actual MCP server, over stdio
```

`pnpm demo` narrates, and asserts at every step:

1. two sources observe the same person as "Jane Doe" and "J. Doe" —
   `recall()` resolves both (and the email) to one entity, aliases and all;
2. a title correction lands mid-stream — `believedAt()` reconstructs the
   pre-correction belief even though current belief has moved on;
3. two independent sources verify that person's identity — retracting one
   leaves the fact standing (`whySoFar` shows it still held, via the
   remaining source); retracting the second kills it (`whySoFar` shows it
   no longer held, both sources on record and retracted);
4. the store is closed and reopened from the same files — the entity, its
   aliases, and the retraction from step 3 are all still there, read back
   from SQLite rather than re-derived.

Everything above runs offline, with no network and no external service — the
whole point of a local memory server. There is no "live mode" to opt into.

## Attaching this server to an MCP client

From a checkout of this repo:

```bash
claude mcp add asg-memory -- pnpm --dir /absolute/path/to/agent-stream-graph/integrations/mcp-memory start
```

Or, for a client that reads raw MCP server config (e.g. `.mcp.json`):

```json
{
  "mcpServers": {
    "asg-memory": {
      "command": "pnpm",
      "args": ["--dir", "/absolute/path/to/agent-stream-graph/integrations/mcp-memory", "start"]
    }
  }
}
```

Replace the path with your own checkout location. The server persists its
belief graph under `integrations/mcp-memory/.data/server/` by default (two
SQLite files: `memory.db`, `checkpoints.db`); override the directory with the
`ASG_MCP_MEMORY_DATA_DIR` environment variable — the stdio smoke test in
`test/server-smoke.test.ts` does exactly that to run against a disposable
directory.

## Limitations

This package documents its gaps rather than hiding them:

- **Naive entity resolution.** `recall()` resolves a handle by scanning all
  `Person`/`Org` rows and matching id / email / name / alias in memory. That
  is fine at demo scale; a real deployment with many entities would want the
  parent library's fulltext/similarity resolution
  (`examples/agents.ts`'s `mergeIncremental` + `resolve: { kind: "fulltext",
  ... }`) instead of a linear scan.
- **One identity key, not fuzzy dedup.** Aliases collapse under one *exact*
  key (a normalized email or domain). A person observed under two different
  emails is not merged — that is a genuinely different problem
  (`examples/agents.ts` demonstrates the fulltext-similarity merge that
  solves it, at the cost of a branch-and-merge pipeline this package
  deliberately keeps out of scope).
- **One seeded predicate.** `whySoFar`'s machinery generalizes to any
  `{entity, predicate}` pair (`factId` is `"${subject}#${predicate}"`), but
  only the `"verified"` predicate has justification data in the shipped
  fixtures.
- **Per-change offsets, by construction of the fixtures.** `believedAt`'s
  granularity is exactly what the underlying stream's offsets allow. This
  package's seed data uses `mockShapeSource`, where every change carries its
  own offset; a real Electric shape source batches a catch-up window under
  one offset, so live deployments would scrub by batch, not by row — the
  same documented limitation as `examples/time-travel.ts`.
- **No retraction tool.** `recall`/`believedAt`/`whySoFar` are read-only; an
  MCP client cannot itself retract a source through this server today. The
  retraction path is real and tested (`src/graph.ts`'s `retractionConfig` +
  `createRetractionCapability`), just not wired to a fourth tool.
- **Single shared graph, no multi-tenancy.** One belief graph per server
  process/data directory. Fine for a personal local memory server; not an
  isolation boundary for multiple untrusted users.
