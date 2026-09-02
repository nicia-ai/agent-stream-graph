# @nicia-ai/asg-claude-agent

Project a live [Claude Agent SDK](https://github.com/anthropics/claude-agent-sdk-typescript)
session — the tool calls it made, what came back, what its subagents did —
into a bitemporal belief graph, using [`@nicia-ai/agent-stream-graph`](../../README.md).

It turns a Claude agent's activity from "a transcript you'd grep" into "a
graph you'd query": *which files did this session touch, and which subagent
touched them* becomes a two-hop traversal instead of a text search, and
*what had the agent discovered as of tool call N* becomes a recorded-time
read instead of a manual scroll back through the log.

## What this demonstrates

1. **A real message stream, decoded purely.** `src/decode.ts` is a
   `Decoder<AgentSessionGraph, SDKMessage>` — one `SDKMessage` in, zero or
   more `GraphEvent`s out, no store and no SDK subprocess anywhere in it.
   `test/decode.test.ts` proves it correct against hand-built `SDKMessage`
   literals, with vitest as the only thing running.
2. **The graph a session means.** `src/graph.ts`: `Session`, `Subagent`,
   `Turn` (narrated text), `ToolCall`, `ToolResult`, and `Artifact` (a file
   path or URL a tool call touched), linked cause → effect —
   `(Session | Subagent) -[ran]-> ToolCall -[touched]-> Artifact` is the
   query this package is built around.
3. **The same decoder, two sources.** `src/source.ts` is one
   `ShapeSource<SDKMessage>` adapter over any `AsyncIterable<SDKMessage>`;
   `src/replay.ts` feeds it a recorded fixture, `src/live.ts` feeds it a real
   `query()` session. `src/demo.ts` picks one at runtime and prints which.
4. **Bitemporal time travel over a live agent's own beliefs.** The demo
   reconstructs the graph as it stood right after the 4th tool call's result
   landed (`belief.asOfRecorded(anchor)`) and asserts it is a strict prefix
   of the current graph — not narrated, asserted: the demo throws if the
   past view isn't actually narrower.
5. **A subagent's contribution stays attributable.** Every `Turn`/`ToolCall`
   a subagent produced hangs off its own `Subagent` node, not the parent
   `Session` — asserted, not just printed, in both the demo and the tests.

## Run it

```sh
pnpm typecheck   # tsc --noEmit
pnpm test        # vitest run — the pure decoder, no SDK involved
pnpm demo        # tsx src/demo.ts
```

`pnpm demo` runs **offline** against `fixtures/session.json` by default —
no network, no API key, no running service — and prints `mode: OFFLINE` at
the top so you can tell which path ran. Set `ANTHROPIC_API_KEY` and it runs
**live** instead: a real `query()` call, asked to read two files and spawn a
subagent to investigate the dependency tree (`src/live.ts`'s
`DEFAULT_PROMPT`), streamed through the exact same decoder.

## The `forwardSubagentText` trap

By default the Claude Agent SDK forwards only `tool_use`/`tool_result`
content from a subagent — enough for a heartbeat counter, not enough to
narrate what it said. `src/live.ts` sets `options.forwardSubagentText: true`
explicitly; without it, a live run's `Turn` nodes for subagent narration
would be silently missing (the `ToolCall`/`Artifact` graph would still be
complete — that half doesn't depend on the flag — but "what did the subagent
say about it" would not). This is the single most consequential option in
the whole integration and it is not the default.

## The fixture is hand-authored, not recorded

`fixtures/session.json` was **not** captured from a real Claude Agent SDK
run — no `ANTHROPIC_API_KEY` was available while building this package. It
is instead written field-for-field against the shipped `sdk.d.ts` inside
`@anthropic-ai/claude-agent-sdk@0.3.250` (every required field of
`SDKAssistantMessage`/`SDKUserMessage`/`SDKSystemMessage` and the Anthropic
SDK's `BetaMessage`/`BetaUsage` present, checked by the TypeScript compiler
via `fixtures/`'s generator script, kept outside this package's published
`src/`). That is a **weaker claim than a recorded transcript** would be: it
proves the shapes this decoder reads are the real shapes, not that a real
session actually produces messages in this order, with this content. If you
have an `ANTHROPIC_API_KEY`, running `pnpm demo` live is the way to see the
real thing; nothing here should be read as "this is what Claude actually
said."

## Scope — what the decoder does NOT model

`decode.ts` and `graph.ts` are deliberately narrow. Left out, on purpose:

- **Only `Read`/`Write`/`Edit`/`NotebookEdit` (→ a `file` `Artifact`) and
  `WebFetch` (→ a `url` `Artifact`) are recognized.** `Bash`, `Grep`, `Glob`,
  and every other tool still get a `ToolCall` node — just no `Artifact`.
  Extending `artifactFor` in `decode.ts` is a small, local change if you need
  more.
- **A `touched` edge is recorded at `tool_use` time, not `tool_result`
  time.** A file a tool call *tried* to touch is recorded as touched even if
  the call subsequently errored — `ToolResult.status` on the same
  `ToolCall` (via `hasResult`) is where you'd check that.
- **Only `system`/`init` is read from `SDKSystemMessage`; every other
  message type is not modeled at all** — `SDKResultMessage` (cost, turn
  count, stop reason), `SDKToolProgressMessage`, hook/compact/rate-limit
  events, and the rest of the (large, and growing) `SDKMessage` union.
  `isModeledSdkMessage` in `decode.ts` is the exact boundary; `source.ts`
  filters the raw stream through it before assigning offsets at all, the
  same way this library's own `electricShapeSource` drops Electric's control
  messages rather than surfacing them as empty changes — see that file's
  comment for why an unfiltered stream would otherwise trip `consume()`'s
  `ProjectorRecordedNothingError` on every `result`/`status`/... message.
- **Content blocks this decoder does not recognize** (`redacted_thinking`,
  `server_tool_use`, MCP tool blocks, ...) **contribute no facts beyond an
  empty-text `Turn`.** The `Turn` write itself is unconditional exactly so
  that a modeled message never decodes to zero events regardless of which
  content blocks it carries — see the comment above `decodeAssistant` in
  `decode.ts`, and `test/decode.test.ts`'s "never returns zero events" case.

## What a live session's "resume" does NOT mean

`source.ts` has the full argument, but the short version: a durable stream
(Electric, `@durable-streams/client`) lives on a server this process can
reconnect to and ask "everything after offset N". A live Claude Agent SDK
session is a subprocess with one `AsyncGenerator` and no server behind it —
once that generator is exhausted or the process dies, there is nothing left
to resume *from*, at any offset. What survives a crash is the **consumer**
side of this library (the belief store and checkpoint book are durable, so a
restarted process resumes its *projection* correctly) — not the ability to
pull new messages out of a dead session. Do not build a "reconnect to a live
agent and keep tailing it" feature on top of this adapter; it cannot do
that, and `source.ts` says so rather than implying otherwise.

## Other things not verified here

- **Live mode is not exercised in CI and is not deterministic.** The demo's
  assertions (a session touches files, a subagent exists, artifacts are
  disjoint between them) hold for the fixture and are *likely* to hold for a
  real run against `DEFAULT_PROMPT`, but a live model is free to solve the
  task differently — e.g. without spawning a subagent at all — in which case
  the demo's assertions will legitimately throw. That is the demo behaving
  correctly, not a bug in the decoder.
- **No entity resolution, no merge, no multi-session canonical graph.**
  Unlike `examples/agents.ts` in the parent package, this integration stops
  at one session's own belief graph. Merging several sessions' graphs into a
  canonical one is exactly what `@nicia-ai/typegraph/graph-merge` (used
  elsewhere in this repo) is for, but this package does not wire it up.
- **`Session`/`Subagent` id collisions across independent processes are not
  guarded against.** Two real sessions never share a `session_id`, so this
  has not come up in practice, but nothing here defends against it either.
