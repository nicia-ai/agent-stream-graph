/**
 * What a Claude Agent SDK session MEANS as a graph.
 *
 * A session is a tree, not a list: a main agent runs `Turn`s (its own text
 * and thinking) and issues `ToolCall`s, some of which (`Task`) spawn a
 * `Subagent` that runs its own turns and tool calls, whose results eventually
 * answer back into the parent. `ToolCall`s that read/write/fetch something
 * addressable are linked to the `Artifact` (a file path or a URL) they
 * touched — that link is the whole point of materializing this into a graph
 * rather than just printing the transcript: "which files did this session
 * touch, and which subagent touched them" becomes a two-hop traversal —
 * `Subagent -[ran]-> ToolCall -[touched]-> Artifact` — instead of a
 * transcript search.
 *
 * Every edge below reads CAUSE -> EFFECT, agent -> action:
 *
 *   (Session | Subagent) -[[ran]]->        ToolCall    the agent that issued it
 *   (Session | Subagent) -[[narrated]]->   Turn        the agent that said it
 *    ToolCall            -[[touched]]->    Artifact    what the call read/wrote/fetched
 *    ToolCall            -[[hasResult]]->  ToolResult  how the call came back
 *    ToolCall            -[[spawned]]->    Subagent    a Task call starting a subagent
 *
 * `Subagent`'s id IS the id of the `ToolCall` (the `Task` invocation) that
 * spawned it — that is how the SDK itself names a subagent's origin
 * (`parent_tool_use_id`), so reusing it as the id keeps `spawned`'s two
 * endpoints trivially derivable from one field on every message, with no
 * separate id-minting scheme to keep in sync. See `decode.ts`.
 */
import { defineEdge, defineGraph, defineNode } from "@nicia-ai/typegraph";
import { z } from "zod";

/** One `query()` call — the root of the session tree. */
const Session = defineNode("Session", {
  schema: z.object({
    model: z.string(),
    cwd: z.string(),
    apiKeySource: z.string(),
  }),
});

/**
 * One subagent run, spawned by a `Task` tool call. `sessionId` names the
 * ROOT session it ultimately reports to, even when a subagent spawns another
 * subagent — useful for "everything this session touched, at any depth"
 * without walking the `spawned` chain first.
 */
const Subagent = defineNode("Subagent", {
  schema: z.object({
    subagentType: z.string(),
    taskDescription: z.string(),
    sessionId: z.string(),
  }),
});

/**
 * One narrated message: an assistant's text/thinking, or a user-role text
 * prompt. Deliberately holds no tool_use/tool_result content — those are
 * `ToolCall`/`ToolResult` — so a `Turn` is exactly "what did the agent say",
 * not "what did the agent do".
 */
const Turn = defineNode("Turn", {
  schema: z.object({
    role: z.enum(["assistant", "user"]),
    text: z.string(),
  }),
});

/** One `tool_use` content block. `input` is the tool's raw JSON input, stringified. */
const ToolCall = defineNode("ToolCall", {
  schema: z.object({
    name: z.string(),
    input: z.string(),
  }),
});

/** The `tool_result` content block answering one `ToolCall`. Same id as the call it answers. */
const ToolResult = defineNode("ToolResult", {
  schema: z.object({
    status: z.enum(["ok", "error"]),
    resultText: z.string(),
  }),
});

/**
 * A file or URL a `ToolCall` read, wrote, or fetched. `id` is
 * `` `${artifactKind}:${path}` ``, so the same path touched by two different
 * tool calls (or two different agents) resolves to one row — the graph, not
 * the transcript, is where "who touched this file" gets answered.
 *
 * The field is `artifactKind`, not `kind`: `id`, `kind`, and `meta` are
 * reserved — TypeGraph adds them to every node automatically, and
 * `defineNode` REFUSES a schema that declares any of the three, loudly and
 * at definition time (a `ConfigurationError` naming the conflict), not as a
 * type-level surprise discovered later at a query call site. See `decode.ts`.
 */
const Artifact = defineNode("Artifact", {
  schema: z.object({
    artifactKind: z.enum(["file", "url"]),
    path: z.string(),
  }),
});

const ran = defineEdge("ran", { schema: z.object({}) });
const narrated = defineEdge("narrated", { schema: z.object({}) });
const touched = defineEdge("touched", { schema: z.object({}) });
const hasResult = defineEdge("hasResult", { schema: z.object({}) });
const spawned = defineEdge("spawned", { schema: z.object({}) });

/** The belief graph one Claude Agent SDK session materializes into. See the module doc comment for the shape. */
export const agentSessionGraph = defineGraph({
  id: "claude_agent_session",
  nodes: {
    Session: { type: Session },
    Subagent: { type: Subagent },
    Turn: { type: Turn },
    ToolCall: { type: ToolCall },
    ToolResult: { type: ToolResult },
    Artifact: { type: Artifact },
  },
  edges: {
    ran: { type: ran, from: [Session, Subagent], to: [ToolCall] },
    narrated: { type: narrated, from: [Session, Subagent], to: [Turn] },
    touched: { type: touched, from: [ToolCall], to: [Artifact] },
    hasResult: { type: hasResult, from: [ToolCall], to: [ToolResult] },
    spawned: { type: spawned, from: [ToolCall], to: [Subagent] },
  },
});

export type AgentSessionGraph = typeof agentSessionGraph;
