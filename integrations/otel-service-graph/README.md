# @nicia-ai/asg-otel-service-graph

OpenTelemetry spans, as a durable stream, materialized into a bitemporal
service-dependency graph — using [`@nicia-ai/agent-stream-graph`](../..)'s
own published entrypoint (`import ... from "@nicia-ai/agent-stream-graph"`,
never `../src`).

The question this answers — **"what did the service topology look like
DURING the incident?"** — is one an OTel backend genuinely answers badly:
dashboards show *current* topology, and trace search finds individual
requests, not "the shape of the dependency graph an hour ago." Here it comes
almost for free, because the graph this library materializes is bitemporal:
`asOfRecorded` reconstructs the belief as of any past recorded instant, and a
service topology is just a belief like any other.

## What it demonstrates

Run `pnpm demo` and it:

1. **Generates real spans** with `@opentelemetry/sdk-trace-node` — a handful
   of services calling each other, offline, driven by the traffic script in
   [`fixtures/incident-scenario.ts`](./fixtures/incident-scenario.ts). Nothing
   is hand-written span JSON; the shapes are genuinely the SDK's own.
2. **Feeds those spans through this library's durable consumer** via a custom
   `SpanExporter` ([`src/exporter.ts`](./src/exporter.ts)) that projects each
   `ReadableSpan` into a plain-JSON `SpanRecord`, and a pure `Decoder`
   ([`src/decode.ts`](./src/decode.ts)) that turns each record into graph
   events — `Service`, `ServiceAlias`, and `Operation` nodes; `aliasOf`,
   `performs`, and `calls` edges ([`src/graph.ts`](./src/graph.ts)).
3. **Asserts entity resolution actually resolves something.** The fixture
   deploys `checkout` under three spellings over its life — `checkout-svc`,
   `checkout` (a rename), `checkout.prod` (an environment-scoped name) — the
   same mess real telemetry accumulates across redeploys. `decode.ts`
   normalizes all three to one canonical `Service` node id while retaining
   every spelling as its own `ServiceAlias` row. The demo throws if the three
   spellings don't collapse to exactly one node.
4. **Asserts the incident timeline.** Partway through the stream, the
   `checkout.prod` deployment starts calling a `fraud-detection` service it
   never called before. `book.anchorFor(stream, offset)` +
   `belief.asOfRecorded(anchor)` reconstruct the topology at an anchor before
   that call and an anchor after — the demo throws unless the edge is
   genuinely absent before and present after, even though the *current* graph
   has moved on past both moments.

## Running it

```
pnpm demo         # generates spans, materializes the graph, prints the timeline
pnpm test         # vitest over decode.ts — pure unit tests, no store, no SDK
pnpm typecheck     # tsc --noEmit
```

Fully offline: no collector, no network, no external service — an in-memory
SQLite belief store via `@nicia-ai/typegraph`'s `sqlite/local` entrypoint.
There is no "live" mode for this package (unlike some of this repo's other
integrations) — there is no meaningful notion of a paid API here, only "spans
from a real collector" vs. "spans from the SDK in-process," and the demo
already generates spans with the real SDK either way.

## Design notes

- **Entity resolution is a fixed suffix strip, not fuzzy matching**
  (`normalizeServiceIdentity` in `src/graph.ts`) — a pure, deterministic
  function of the raw name alone, safe to call from a `Decoder` that only
  ever sees one span at a time. Every observed spelling is kept as its own
  `ServiceAlias` row rather than merged into an array field on `Service`, so
  no read-modify-write is ever needed: each span's decode only writes the one
  alias it saw.
- **Span timing is valid time; ingest is recorded time.** `decode.ts` stamps
  `validFrom` from the span's own `startTime` on *every* event, not just
  closing ones — the API's rule is that a stated `validFrom` on an already-
  live row never rewinds its start, so this is safe to do uniformly.
- **Multiple `NodeTracerProvider`s, one exporter.** To simulate several
  independent services in one process, `src/demo.ts` gives each raw service
  name its own provider/resource/tracer, and threads span parent context
  explicitly through `startSpan`'s third argument rather than relying on a
  registered global context manager — which is also what lets several
  providers coexist without one clobbering another's global registration.

## Limitations — read before trusting this further than it goes

- **Naive entity resolution over-merges.** `normalizeServiceIdentity` strips
  a fixed list of environment/deployment suffixes. It cannot tell a
  `payments-svc` and an unrelated `payments.internal` dashboard apart — they
  would collapse into one `payments` node. A real system would want the
  fuzzy-matching, provenance-tracked entity resolution this repo's
  `examples/deep-survey-convergence.ts` demonstrates (fulltext similarity +
  `graph-merge`), not a suffix list. This package deliberately keeps
  resolution simple and pure so it can run inside a stateless per-span
  decoder; it does not attempt that harder problem.
- **`peer.service` vs. `service.peer.name` is genuinely unsettled upstream.**
  OpenTelemetry's own docs contradict each other on this pairing as of this
  writing (2026-08-28): one page marks `peer.service` deprecated in favour of
  `service.peer.name`; another still lists `service.peer.name` itself as
  Development status. `src/exporter.ts` prefers the newer name and falls back
  to the older one, and says so in a comment — this is a documented judgment
  call, not a settled fact. Re-check against whatever
  `@opentelemetry/semantic-conventions` version you pin.
- **No span drops, no batching, no retries, no OTLP.** `SimpleSpanProcessor`
  exports synchronously and the whole scenario runs in one process — this
  says nothing about how the exporter behaves under a `BatchSpanProcessor`'s
  batching/retry semantics or against a real collector.
- **Offsets, not wall-clock time, drive the "before"/"after" anchors.** The
  demo's incident-timeline assertion depends on export ORDER (which the
  `ShapeChange` offsets encode), not on the spans' own `startTime` values
  being far apart — recorded time (what `asOfRecorded` reconstructs) and
  valid time (what `startTime` feeds into `validFrom`) are genuinely
  different axes, and this demo only exercises the recorded-time one for its
  timeline assertion.
- Like every demo in this repo, `Operation` nodes and the `performs` edge add
  structural realism (a service topology usually has operations under it)
  but the demo's assertions don't exercise them beyond decoding — see
  `test/decode.test.ts` for what's actually covered.
