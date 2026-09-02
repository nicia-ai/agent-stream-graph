/**
 * The pure decoder: one `SDKMessage` in, zero or more `GraphEvent`s out,
 * shaped as this library's own `Decoder<G, V>` and built from `emit` — the
 * typed `GraphEmitter` a `Decoder` is always handed, never constructed here.
 * No store, no SDK subprocess, no I/O — every case below is testable by
 * handing it a message literal and an emitter, which is the whole point of
 * the decoder seam (`test/decode.test.ts` does exactly that, with no
 * `@anthropic-ai/claude-agent-sdk` process ever spawned).
 *
 * `tool_use` and `tool_result` are NOT top-level `SDKMessage` variants — they
 * are ordinary Anthropic content blocks nested inside `message.content` of
 * `SDKAssistantMessage` (tool_use) and `SDKUserMessage` (tool_result). This
 * module's job is exactly that walk: message -> content blocks -> events.
 *
 * `parent_tool_use_id` is the one field this whole decoder pivots on: `null`
 * means "the main session said/did this", non-null means "the subagent
 * spawned by that tool call said/did this" — see graph.ts's module comment
 * for why that value doubles as the `Subagent` node's id.
 */
import type { Decoder, GraphEmitter, GraphEvent } from "@nicia-ai/agent-stream-graph";
import { z } from "zod";

import type { SDKAssistantMessage, SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";

import type { AgentSessionGraph } from "./graph.js";

type Event = GraphEvent<AgentSessionGraph>;
type Emit = GraphEmitter<AgentSessionGraph>;

/** Who said or did something: the main session, or the subagent spawned at `id`. */
type Subject = Readonly<{ kind: "Session" | "Subagent"; id: string }>;

/**
 * `undefined` only in the one case that cannot be recovered without state: a
 * MAIN-SESSION (`parent_tool_use_id === null`) `SDKUserMessage`, whose
 * `session_id` — unlike `SDKAssistantMessage`'s — is optional in the SDK's
 * own types (hence the two overloads: an assistant message's subject is
 * always determinable, a user message's is not quite). A subagent message
 * never hits this either way: its subject is the `parent_tool_use_id` itself,
 * which is never optional. Callers skip attribution (the `narrated` edge)
 * rather than inventing an id no `Session` row was ever created for.
 */
function subjectOf(msg: SDKAssistantMessage): Subject;
function subjectOf(msg: SDKUserMessage): Subject | undefined;
function subjectOf(msg: SDKAssistantMessage | SDKUserMessage): Subject | undefined {
  if (msg.parent_tool_use_id !== null) return { kind: "Subagent", id: msg.parent_tool_use_id };
  return msg.session_id === undefined ? undefined : { kind: "Session", id: msg.session_id };
}

// --- Content-block narrowing --------------------------------------------
//
// Derived by indexing into the ALREADY-IMPORTED SDK types rather than adding
// a direct import of `@anthropic-ai/sdk` (a transitive dependency this
// package never declares) — `sdk.d.ts` resolves that import relative to
// ITS OWN location, so indexing through it works without us naming the
// package at all.

type AssistantContentBlock = SDKAssistantMessage["message"]["content"][number];
type UserContent = SDKUserMessage["message"]["content"];
type UserContentBlock = Extract<UserContent, readonly unknown[]>[number];

function assistantNarrative(content: readonly AssistantContentBlock[]): string {
  const parts: string[] = [];
  for (const block of content) {
    if (block.type === "text") parts.push(block.text);
    else if (block.type === "thinking") parts.push(block.thinking);
  }
  return parts.join("\n\n");
}

function userNarrative(content: UserContent): string {
  if (typeof content === "string") return content;
  const parts: string[] = [];
  for (const block of content) {
    if (block.type === "text") parts.push(block.text);
  }
  return parts.join("\n\n");
}

function userContentBlocks(content: UserContent): readonly UserContentBlock[] {
  return typeof content === "string" ? [] : content;
}

function toolResultText(content: Extract<UserContentBlock, { type: "tool_result" }>["content"]): string {
  if (content === undefined) return "";
  if (typeof content === "string") return content;
  const parts: string[] = [];
  for (const block of content) {
    if (block.type === "text") parts.push(block.text);
  }
  return parts.join("\n");
}

function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value) ?? "null";
  } catch {
    return `<unserializable: ${String(value)}>`;
  }
}

// --- Artifact extraction --------------------------------------------------
//
// Validated with zod (already a dependency, and the idiom this codebase uses
// everywhere else to narrow `unknown`) rather than a hand-rolled `unknown` ->
// `Record<string, unknown>` cast. A tool whose input does not match is simply
// not an artifact touch — Bash, Grep, Glob, and anything unrecognized fall
// through with no Artifact node, which is a real scope cut: see the README.

const FILE_TOOLS = new Set(["Read", "Write", "Edit", "NotebookEdit"]);
const URL_TOOLS = new Set(["WebFetch"]);
const FileToolInput = z.object({ file_path: z.string() });
const UrlToolInput = z.object({ url: z.string() });

// Field is `artifactKind`, not `kind`: `id`, `kind`, and `meta` are reserved
// — TypeGraph adds them to every node automatically — so `defineNode`
// refuses a schema declaring any of the three with a `ConfigurationError` at
// definition time, not at some later query call site. See `graph.ts` and
// `test/decode.test.ts`'s naming-collision note.
type ArtifactRef = Readonly<{ artifactKind: "file" | "url"; path: string }>;

function artifactFor(toolName: string, input: unknown): ArtifactRef | undefined {
  if (FILE_TOOLS.has(toolName)) {
    const parsed = FileToolInput.safeParse(input);
    return parsed.success ? { artifactKind: "file", path: parsed.data.file_path } : undefined;
  }
  if (URL_TOOLS.has(toolName)) {
    const parsed = UrlToolInput.safeParse(input);
    return parsed.success ? { artifactKind: "url", path: parsed.data.url } : undefined;
  }
  return undefined;
}

const artifactId = (artifact: ArtifactRef): string => `${artifact.artifactKind}:${artifact.path}`;

// --- Per-message decoding --------------------------------------------------

function decodeSubagentBirth(
  msg: Readonly<{ parent_tool_use_id: string | null; subagent_type?: string; task_description?: string; session_id?: string | undefined }>,
  emit: Emit,
): readonly Event[] {
  if (msg.parent_tool_use_id === null) return [];
  const subagentId = msg.parent_tool_use_id;
  return [
    emit.nodes.Subagent.upsert(subagentId, {
      subagentType: msg.subagent_type ?? "unknown",
      taskDescription: msg.task_description ?? "",
      sessionId: msg.session_id ?? "",
    }),
    emit.edges.spawned.upsert({ kind: "ToolCall", id: subagentId }, { kind: "Subagent", id: subagentId }),
  ];
}

function decodeToolUse(subject: Subject, block: Extract<AssistantContentBlock, { type: "tool_use" }>, emit: Emit): readonly Event[] {
  const events: Event[] = [
    emit.nodes.ToolCall.upsert(block.id, { name: block.name, input: safeStringify(block.input) }),
    emit.edges.ran.upsert(subject, { kind: "ToolCall", id: block.id }),
  ];
  const artifact = artifactFor(block.name, block.input);
  if (artifact !== undefined) {
    events.push(
      emit.nodes.Artifact.upsert(artifactId(artifact), artifact),
      emit.edges.touched.upsert({ kind: "ToolCall", id: block.id }, { kind: "Artifact", id: artifactId(artifact) }),
    );
  }
  return events;
}

function decodeToolResult(block: Extract<UserContentBlock, { type: "tool_result" }>, emit: Emit): readonly Event[] {
  return [
    emit.nodes.ToolResult.upsert(block.tool_use_id, {
      status: block.is_error === true ? "error" : "ok",
      resultText: toolResultText(block.content),
    }),
    emit.edges.hasResult.upsert({ kind: "ToolCall", id: block.tool_use_id }, { kind: "ToolResult", id: block.tool_use_id }),
  ];
}

// A `Turn` is created UNCONDITIONALLY for every assistant/user message this
// decoder is handed, even when its narrated text is "" (a tool_use-only or
// tool_result-only frame, or a content block kind this decoder does not
// model — redacted_thinking, server_tool_use, and the rest of the Beta
// surface). That is not padding: `consume()` treats a non-`delete` change
// that writes NOTHING as a dropped change and throws
// `ProjectorRecordedNothingError` — see `test/decode.test.ts`'s
// "never returns zero events for a modeled message" case. Guaranteeing at
// least the Turn write, for every message `isModeledSdkMessage` admits, is
// what makes that guarantee hold by construction instead of by the shape of
// today's fixture. `source.ts`'s filter is the other half: everything
// `isModeledSdkMessage` rejects never becomes a `ShapeChange` at all.

function decodeAssistant(msg: SDKAssistantMessage, offset: string, emit: Emit): readonly Event[] {
  const subject = subjectOf(msg);
  const events: Event[] = [
    ...decodeSubagentBirth(msg, emit),
    emit.nodes.Turn.upsert(offset, { role: "assistant", text: assistantNarrative(msg.message.content) }),
    emit.edges.narrated.upsert(subject, { kind: "Turn", id: offset }),
  ];

  for (const block of msg.message.content) {
    if (block.type === "tool_use") events.push(...decodeToolUse(subject, block, emit));
  }
  return events;
}

function decodeUser(msg: SDKUserMessage, offset: string, emit: Emit): readonly Event[] {
  const subject = subjectOf(msg);
  const events: Event[] = [
    ...decodeSubagentBirth(msg, emit),
    emit.nodes.Turn.upsert(offset, { role: "user", text: userNarrative(msg.message.content) }),
  ];
  if (subject !== undefined) events.push(emit.edges.narrated.upsert(subject, { kind: "Turn", id: offset }));

  for (const block of userContentBlocks(msg.message.content)) {
    if (block.type === "tool_result") events.push(...decodeToolResult(block, emit));
  }
  return events;
}

/**
 * The three message shapes `decodeSdkMessage` models: a session's `init`
 * (the ONLY `system` subtype it acts on), and every `assistant`/`user`
 * message (guaranteed non-empty by construction — see the comment above
 * `decodeAssistant`).
 *
 * `source.ts` filters the raw SDK stream through this predicate BEFORE
 * assigning offsets, so no other message type — `result`, `status`,
 * `tool_progress`, hook/compact/rate-limit events, and everything else in
 * the (large, and growing) `SDKMessage` union — ever becomes a
 * `ShapeChange` at all. That mirrors how `electricShapeSource` (this
 * library's Electric adapter) never turns an `up-to-date`/`must-refetch`
 * control message into a `ShapeChange` either: a message this decoder has
 * nothing to say about is not a dropped fact, it is a message outside this
 * package's stated scope (see the README), and `consume()` has no way to
 * tell those two apart from an empty decode — so the filter is what keeps
 * that distinction real instead of silently tripping
 * `ProjectorRecordedNothingError` on every `result`/`status`/... message.
 */
export function isModeledSdkMessage(message: SDKMessage): boolean {
  if (message.type === "system") return message.subtype === "init";
  return message.type === "assistant" || message.type === "user";
}

/**
 * Decode one `SDKMessage` — carried as a `ShapeChange<SDKMessage>`'s `value`,
 * always `operation: "insert"` since the message stream is append-only — into
 * graph events. `graphProjector(agentSessionGraph, decodeSdkMessage)` adapts
 * this into the `Projector` `consume()` takes; see `source.ts`/`replay.ts`/
 * `live.ts` for where the `ShapeChange`s themselves come from, already
 * filtered through {@link isModeledSdkMessage}.
 *
 * `change.offset` doubles as the `Turn` node's id: one `ShapeChange` is one
 * `SDKMessage` (`source.ts` assigns a fresh offset per message off the
 * stream), so it is already a stable, unique, arrival-ordered key — no
 * dependence on `message.uuid`, which `SDKUserMessage` does not always carry.
 *
 * Two invariants this decoder trusts rather than defends against (documented
 * here instead of defensively coded around, matching this library's own
 * `RESTRICTED_DELETE` / apply-order notes): a `system`/`init` message is
 * always the first message of a session, so by the time any `assistant`/
 * `user` message's `narrated`/`ran` edge targets a `Session` row, that row
 * already exists; and a `Task` tool_use always precedes the first message
 * from the subagent it spawns, so `spawned`'s `ToolCall` endpoint always
 * exists by the time a subagent message writes the edge. Both hold for the
 * real SDK's protocol and for every fixture in `fixtures/`.
 */
export const decodeSdkMessage: Decoder<AgentSessionGraph, SDKMessage> = (change, emit) => {
  const message = change.value;
  const offset = change.offset;
  switch (message.type) {
    case "system":
      if (message.subtype !== "init") return [];
      return [emit.nodes.Session.upsert(message.session_id, { model: message.model, cwd: message.cwd, apiKeySource: message.apiKeySource })];
    case "assistant":
      return decodeAssistant(message, offset, emit);
    case "user":
      return decodeUser(message, offset, emit);
    default:
      // Reachable only if a caller feeds this decoder a message
      // `isModeledSdkMessage` would reject (`source.ts` never does) — an
      // intentional, honest empty decode for a message this package does
      // not model, not a dropped fact.
      return [];
  }
};
