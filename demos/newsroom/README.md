# The newsroom desk

The flagship demo for `@nicia-ai/agent-stream-graph`: a swarm of reporters
that disagree, get reviewed by an editor, fork into a hypothetical, and get
retracted — with a recorded-time audit trail throughout. Everything here
imports the PUBLISHED package (`@nicia-ai/agent-stream-graph`, `workspace:*`)
and `@nicia-ai/typegraph`, never a relative `../src` — this package doubles
as a standing test of what a real consumer installs.

## What it demonstrates

Three reporters — ash, brook, cass — cover one story (a transit contract
award) from partially overlapping sources, and each materializes their OWN
bitemporal belief graph (`desk/materialize.ts`, `examples/agents.ts`'s
per-agent pattern). There is deliberately no `Reporter` node in the schema
(`src/graph.ts`): each reporter's belief is staged into the merge as its own
branch, so "who said this" is answered by TypeGraph's own merge provenance
rather than an edge this package would have to keep honest — see
`desk/editor.ts`'s `provenanceBylines`.

1. **Entity resolution collapses a spelling difference.** ash writes "M.
   Vance" `@mvance`; brook writes "Marisa Vance" `@MVance`. The handle differs
   only in case, and `graph.ts` declares it a case-insensitive unique key, so
   the merge collapses them to ONE canonical `Subject` — and flags the name/role
   spelling as a property conflict in the process, exactly as advertised.
2. **A real disagreement gets FLAGGED, not resolved.** ash and brook both file
   a claim literally id'd `claim-value`, for $41M and $38M respectively.
   Whichever commits first is kept (`onBasePropertyConflict: "flag"`); the
   other is recorded as a flagged conflict, not silently discarded — see
   `desk/run.ts`'s assertion on this exact conflict, and section "Canonical" in
   `pnpm demo`'s output.
3. **The editor reviews before anything commits, and the library enforces it.**
   `desk/editor.ts` splits TypeGraph's one-call merge into `buildReviewPlan`
   (`planMergeIncremental` — a durable, JSON-serializable `MergePlanArtifact`,
   nothing written) and `commitReviewedPlan` (`applyMergePlan`). `desk/run.ts`
   builds ash's plan FIRST, lets brook's commit land, and only then applies
   ash's now-stale plan — and the library refuses it
   (`StaleMergePlanError`), asserted, before a freshly-rebuilt plan commits for
   real. This human-in-the-loop review beat — see it before it lands, and have
   the library catch you if the ground moved — is the point of the package.
4. **A retraction cascades asymmetrically.** ash's award claim rests on TWO
   independent sources (the filing AND the anonymous tip); cass's kickback
   claim rests on the tip ALONE. Burning the tip
   (`desk/retract.ts`, `@nicia-ai/typegraph/provenance`) kills cass's claim and
   its draft story, and leaves ash's claim standing — because `decode.ts`
   grounds every premise in its OWN `Justification` node rather than one
   AND-justification per claim. `retract.ts` doesn't just narrate this: it
   THROWS if either half of the asymmetry doesn't hold.
5. **A hypothetical forks off the shared belief.** `desk/fork.ts` freezes the
   belief right after ash's and brook's shared "the award happened" beat,
   before their coverage diverges on the value, and runs a THIRD account —
   one no reporter filed — forward on an isolated `branch()`. **This forks the
   GRAPH at the point the log would have diverged, not the log itself** —
   `examples/fork-merge.ts` is the one that forks a Durable Streams LOG
   against a running server; see "What this does NOT prove" below and that
   file's own "honesty note" for exactly why.

## Running it

Both modes run the exact same pipeline (`desk/run.ts`); only the reporters'
`ShapeSource` changes.

```bash
pnpm install    # from the repo root — already done in this checkout
pnpm typecheck
pnpm test
pnpm demo       # the whole story, narrated, to stdout
pnpm serve      # the same run, exposed as HTTP + SSE (see below)
```

**Replay mode (default, offline).** No `ANTHROPIC_API_KEY` needed. Reporters'
events come from `fixtures/dispatches.ts` — recorded transcripts checked into
the repo — via `reporters/replay.ts`. Deterministic: the same run produces
the same canonical graph, the same conflicts, the same retraction report,
every time. This is what CI runs.

**Live mode.** Set `ANTHROPIC_API_KEY` (and optionally `ANTHROPIC_MODEL`,
default `claude-sonnet-5`) and run `pnpm demo` / `pnpm serve` again.
`reporters/live.ts` gives each reporter persona the same underlying source
documents as the fixtures, as prose (not as pre-structured events), and asks
each for the SAME `ReporterEvent` JSON shape via a single tool-free
`@anthropic-ai/claude-agent-sdk` `query()` call (`tools: []` — a reporter
writes, it doesn't touch a filesystem or shell). The response is validated
against a zod schema before anything downstream sees it. Everything from
`desk/materialize.ts` on is identical code, unaware which mode produced its
input — that's the whole point of the `ShapeSource` seam.

Live mode is inherently non-deterministic: a real model may phrase a claim
differently, omit one, or occasionally return JSON the schema rejects (in
which case the error names which reporter and what failed). **No API key was
available while building this package** — the live path is real integration
code, not a stub, but it is unverified end-to-end against an actual model.
Try it and, if something breaks, the error will say what.

## The HTTP contract (`pnpm serve`)

`server.ts` runs the desk pipeline ONCE at startup and serves the completed
result. **This is a replay server, not a live one** — `/api/events` streams
the run's own finished timeline to whoever connects; it does not push new
events as they happen, because nothing happens after startup. Default port
`8879` (override with `PORT`). Every endpoint is JSON; `Access-Control-Allow-Origin: *`
throughout.

| Method & path | Returns |
|---|---|
| `GET /api/health` | `{ status: "ok", mode: "replay" \| "live" }` |
| `GET /api/reporters` | `Array<{ id, processed, lastOffset, claims: ClaimRow[], stories: StoryRow[] }>` — one entry per reporter, in `fixtures/dispatches.ts`'s `REPORTERS` order |
| `GET /api/reporters/:id` | One reporter's entry from the array above; `404 { error }` for an unknown id |
| `GET /api/canonical` | `{ subjects: SubjectRow[], claims: ClaimRow[], stories: StoryRow[], bylines: Record<subjectId, string[]> }` — `bylines[id]` is the list of reporter ids whose branch contributed to that canonical `Subject` (`desk/editor.ts`'s `provenanceBylines`) |
| `GET /api/review` | The editor's review-queue timeline: `Array<DeskEventJson>` — see below |
| `GET /api/fork` | `{ forkOffset: string, trunkValue: string \| null, whatIfValue: string \| null }` |
| `GET /api/retraction` | `{ before: CurrentState, after: CurrentState, report: RetractionReport }` — `CurrentState = { survivingClaim, dyingClaim, dyingStory }`, each a value/headline string or `null` when non-current |
| `GET /api/timeline` | `{ reporters: Array<{ reporterId, lastOffset }>, forkOffset, canonicalRecordedRevision: { beforeRetraction, afterRetraction } }` — raw material for a recorded-time scrubber |
| `GET /api/events` | Server-Sent Events: replays `/api/review`'s entries as `event: <type>` frames, then one `event: fork`, one `event: retracted`, then `event: done`, then closes the connection |

`ClaimRow = { id, text, predicate, value, confidence }`. `StoryRow = { id,
headline, status }`. `SubjectRow = { id, name, handle, role }` (see
`desk/views.ts`). `DeskEventJson` is one of:

```ts
{ type: "materialized", reporterId, processed, lastOffset }
{ type: "review-queue", reporterId, plan: MergePlanArtifact }   // see @nicia-ai/typegraph/graph-merge
{ type: "stale-refusal", reporterId, errorMessage }
{ type: "committed", reporterId, merged, conflicts, resolutions }
```

This table is the interface contract — if you change an endpoint's shape,
update this table in the same commit (`server.ts`'s file header says the
same thing from the other side).

## What this does NOT prove

- **The fork is a graph fork, not a log fork.** `examples/fork-merge.ts`
  forks a Durable Streams LOG against a running server
  (`forkStream`/`forkPointFor`). This package's reporters read from
  checked-in fixtures or a one-shot agent call — neither is a durable log
  with a server to fork — so `desk/fork.ts` forks the BELIEF instead, via
  TypeGraph's `branch()` (the same primitive `ingestionBranch` builds on).
  See that file's header for the full reasoning. If you need the log-level
  primitive demonstrated, `examples/fork-merge.ts` is the worked example.
- **Live mode is unverified against a real model.** See above — the
  integration is real, but it never ran against live credentials while this
  package was built.
- **No crash/resume story of its own.** `examples/crash-resume.ts` and
  `examples/exactly-once.ts` cover the durable-consumer crash semantics this
  package's `consume()` calls rely on; this package doesn't re-demonstrate
  them, it just uses them.
- **Single-process, in-memory-adjacent SQLite only.** Every store here is a
  fresh local SQLite backend (`src/backend.ts`, mirroring `examples/_support.ts`).
  Nothing here exercises Postgres, D1, or a multi-process deployment.
- **The UI is not this package.** `server.ts` is the contract another
  package's UI is built against; this package ships no UI of its own.
- **`pnpm serve`'s SSE is a replay, not a push.** Said above, worth repeating:
  connecting twice gets you the same finished timeline twice, not two views
  of one live process.

## Layout

```
fixtures/dispatches.ts   recorded reporter transcripts (the offline default)
src/graph.ts              schema: Wire, Tipster, Subject, Claim, Story, Justification
src/decode.ts              pure reporter-event → graph-event decoder
src/backend.ts             SQLite backend + store helpers for this package
src/reporters/replay.ts    ShapeSource from the fixtures
src/reporters/live.ts      ShapeSource from real Claude agents (ANTHROPIC_API_KEY)
src/desk/materialize.ts    per-reporter belief materialization
src/desk/editor.ts         review-plan / commit — the human-in-the-loop beat
src/desk/fork.ts           the graph-level what-if fork
src/desk/retract.ts        the retraction cascade + asymmetry assertion
src/desk/views.ts          shared read queries (main.ts and server.ts both use these)
src/desk/run.ts            orchestrates the whole story once, as data
src/main.ts                pnpm demo — narrated console output
src/server.ts              pnpm serve — the same run, as HTTP + SSE
test/                      vitest over decode.ts (pure) and the retraction asymmetry
```
