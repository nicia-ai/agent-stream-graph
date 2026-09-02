# @nicia-ai/asg-decoders

Proof that adopting `@nicia-ai/agent-stream-graph` from an existing agent framework costs about
twenty lines. A source contributes a pure `Decoder` — no store, no I/O — while the hard parts
(idempotence, apply order, valid time, delete semantics) are settled once, in the library, for
every source. This package makes that claim checkable.

## The line counts

| Decoder | Framework | Code lines | Typed against |
|---|---|---:|---|
| [`src/vercel-ai-sdk.ts`](src/vercel-ai-sdk.ts) | Vercel AI SDK (`ai` v7) | 17 | the real, installed `ai` package |
| [`src/langgraph.ts`](src/langgraph.ts) | LangGraph | 21 | LangGraph's documented event shape (structural — see below) |
| [`src/generic-jsonl.ts`](src/generic-jsonl.ts) | any tool-call log | 25 | nothing but its own log-line type |

"Code lines" excludes comments and blank lines — each file fits on one screen, comments included.
All three share one 21-line helper, [`toolCallEvents`](src/graph.ts), which is the part of the
mapping that has nothing to do with any particular framework.

## Side by side: the Vercel AI SDK decoder, in full

```ts
import type { Decoder } from "@nicia-ai/agent-stream-graph";
import type { TextStreamPart, ToolSet } from "ai";

import { agentGraph, toolCallEvents } from "./graph.js";

export type VercelStreamEvent = Readonly<{ runId: string; part: TextStreamPart<ToolSet> }>;

export const decodeVercelAiSdk: Decoder<typeof agentGraph, VercelStreamEvent> = (change, g) => {
  const { runId, part } = change.value;
  if (part.type !== "tool-call" && part.type !== "tool-result") return [];

  return toolCallEvents(g, {
    runId,
    runSource: "vercel-ai-sdk",
    toolCallId: `${runId}:${part.toolCallId}`,
    name: part.toolName,
    status: part.type === "tool-call" ? "pending" : "complete",
    input: part.input,
    ...(part.type === "tool-result" ? { output: part.output } : {}),
  });
};
```

That is the whole adapter. `part` is `ai`'s own `TextStreamPart<ToolSet>` — the exact union
`streamText()`'s `fullStream` yields — imported straight from `node_modules/ai`, not guessed at.
`decodeLangGraph` and `decodeGenericJsonl` follow the identical shape: narrow the framework's event
to a tool call, hand its fields to `toolCallEvents`, done. Read all three in
[`src/`](src/) — none of them is longer than this one.

## What `toolCallEvents` does once, for all three

[`src/graph.ts`](src/graph.ts) defines the shared graph every decoder writes into —

```
Run --madeCall--> ToolCall --touched--> Resource
```

— and the one function (`toolCallEvents`) that turns `{ runId, toolCallId, name, status, input,
output? }` into that shape: a `Run` upsert, a `ToolCall` upsert, the edge between them, and — when
the tool's input names a `path` or `query` — a `Resource` upsert and the edge to it. A decoder's
entire job is reducing its framework's event to those six fields.

## Run it

```
pnpm typecheck   # strict, clean, no `any` / `as never` / `!` / @ts-expect-error anywhere in this package
pnpm test        # vitest — every decoder tested as a pure function, no store involved
pnpm demo        # tsx src/demo.ts — decodes all three fixtures and checks convergence
```

All three fixtures encode the identical scenario — one run that calls `searchDocs({ query:
"graph databases" })` then `readFile({ path: "docs/graph.md" })`, with the identical outputs — so
the decoded graphs can be checked against each other rather than just eyeballed. `pnpm demo` makes
two separate checks, weakest claim last:

1. **The `Resource` nodes, compared with ZERO normalization.** A `Resource`'s id is derived from
   the tool's input alone (see `resourceRefFromInput` in `graph.ts`) — it has no run id, no
   framework name, nothing source-specific baked in by construction. So the `Resource.upsert` event
   each decoder emits for `docs/graph.md` is asserted **byte-identical, untouched**, across all
   three sources. This is the strongest form of the claim: three frameworks independently minting
   the exact same event for the same real-world thing, with nothing normalized away to make it so.
2. **The `ToolCall` events, after normalizing exactly two things.** Unlike a `Resource`, a
   `ToolCall`'s id is deliberately run-scoped (`` `${runId}:${toolCallId}` ``) so that two runs
   reusing the same underlying call id don't collide — see the id-scoping test in
   `test/vercel-ai-sdk.test.ts`. Comparing raw `ToolCall` ids across sources would therefore always
   fail, for a reason that has nothing to do with whether the decoders agree. Before comparing,
   `demo.ts` does exactly two things, and only these two:
   - **drops** every `Run` node and every `madeCall` edge (the part whose whole job is naming
     *which* source produced this run — asserting those are identical would be asserting away the
     one thing that is supposed to differ);
   - **renames** each `ToolCall`'s run-scoped id to the tool name it calls (stable across sources
     in this fixture, where each tool is called once per run), so `run-vercel:call-1` and
     `run-langgraph:t1` both become `searchDocs` before comparing.

   Everything else — `name`, `status`, `input`, `output`, and every `touched` edge — is compared
   as decoded, unchanged. If step 2 is not fully convincing on its own, that is by design: step 1
   is the check that carries no asterisk.

Both checks are real assertions, not printed claims: edit any fixture's output value and `pnpm
demo` fails loudly with both sides shown. (Verified by temporarily changing a fixture's output and
re-running — `pnpm demo` threw with both sides printed, then reverted.)

## Write your own decoder

1. Pick (or extend) a shared graph — reuse `agentGraph` from `graph.ts`, or define your own with
   `defineGraph` and export a `toolCallEvents`-shaped helper for the fields your sources share.
2. Type your source's event as precisely as you can. If the framework is installed, import its
   real types (`vercel-ai-sdk.ts`). If it isn't, or can't be, transcribe its documented shape as a
   structural type and say so in a comment (`langgraph.ts`). If there's no SDK to speak of, define
   the plainest type your log actually has (`generic-jsonl.ts`).
3. Write a `Decoder<G, V>`: `(change, emit) => GraphEvent<G>[]`. It is a pure function — no store,
   no `await`, nothing but the two arguments in and an array of events out. That purity is what
   makes it testable the way `test/` tests these three: construct a `ShapeChange`, call the decoder,
   assert on the exact events back — no database, no fixture server, nothing running.
4. Wire it up with `graphProjector(graph, decode)` and `consume()` — that part is the same for every
   source and is demonstrated in `examples/agents.ts` and `examples/emit.ts` at the repo root, not
   repeated here.

## What this does NOT prove

- **No live store.** This package intentionally carries no database dependency (compare
  `integrations/claude-agent` or `integrations/mcp-memory`, which do). A `Decoder` has nothing to do
  with storage, so the convergence check above compares decoded `GraphEvent` arrays directly rather
  than materializing them into a running `TypeGraph` store. Applying decoded events through
  `graphProjector` + `consume` into an actual store — idempotent upserts, resumable checkpoints,
  the `RESTRICTED_DELETE` ordering `applyGraphEvents` handles — is exercised by the root `examples/`
  and by the other `integrations/` packages, not by this one.
- **LangGraph is not actually installed or run against.** `langgraph.ts` is typed structurally
  against its documented `astream_events` v2 shape. It has never compiled against a real LangGraph
  type, and if that shape changes upstream, nothing here would notice.
- **Real streams are messier than the fixtures.** Each fixture is a clean, ordered, two-call run.
  Nothing here demonstrates out-of-order delivery, a tool call whose result never arrives, or
  provider-specific chunk shapes (approvals, dynamic tools, provider-executed tools) that
  `TextStreamPart` also includes but this decoder doesn't act on.
- **Tool errors are handled shallowly.** `generic-jsonl.ts`'s `error` phase folds the error message
  into `ToolCall.output` as JSON; the Vercel and LangGraph decoders don't decode a tool-error event
  at all (out of scope for "tool-call/tool-result", per this package's brief).
- **Resource classification is deliberately naive.** `resourceRefFromInput` recognizes exactly two
  field names (`path`, `query`). It is meant to make convergence checkable across three toy
  fixtures, not to be a real resource-extraction strategy.
