/**
 * `decodeSdkMessage` is a pure function of one `SDKMessage` — no store, no
 * `@anthropic-ai/claude-agent-sdk` subprocess, no I/O anywhere in this file.
 * Every case below constructs a plain `SDKMessage` literal directly (never a
 * real `query()` result) and inspects the `GraphEvent[]` it produces. That
 * is the whole claim this package makes about the decoder seam being cheap
 * — this file is the proof.
 */
import type { UUID } from "node:crypto";

import { graphEmitter, OP_EDGE_UPSERT, OP_NODE_UPSERT, type ShapeChange } from "@nicia-ai/agent-stream-graph";
import { describe, expect, it } from "vitest";

import type { SDKAssistantMessage, SDKMessage, SDKSystemMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";

import { decodeSdkMessage, isModeledSdkMessage } from "../src/decode.js";
import { agentSessionGraph } from "../src/graph.js";

const emit = graphEmitter(agentSessionGraph);

const SESSION_ID = "sess-test";
const MODEL = "claude-opus-5";

let uuidCounter = 0;
function nextUuid(): UUID {
  uuidCounter += 1;
  const hex = String(uuidCounter).padStart(12, "0");
  return `00000000-0000-4000-8000-${hex}`;
}

function changeAt(offset: string, message: SDKMessage): ShapeChange<SDKMessage> {
  return { offset, shape: "sdk-message", key: offset, operation: "insert", value: message };
}

// --- Minimal, faithfully-typed SDKMessage builders --------------------------
//
// Every required field of `BetaMessage`/`BetaUsage` (checked against
// `sdk.d.ts` and `@anthropic-ai/sdk`'s own types — see `fixtures/`'s
// generator for the same exercise at fixture scale) gets a plausible
// constant; `decodeSdkMessage` never reads most of them, but the compiler
// checking every one of them present is exactly the point: these literals
// prove they are real `SDKMessage`s, not a hand-waved stand-in.

const USAGE = {
  cache_creation: null,
  cache_creation_input_tokens: null,
  cache_read_input_tokens: null,
  fallback_credit: null,
  inference_geo: null,
  input_tokens: 10,
  iterations: null,
  output_tokens: 10,
  output_tokens_details: null,
  server_tool_use: null,
  service_tier: "standard",
  speed: "standard",
} as const;

function betaMessage(content: SDKAssistantMessage["message"]["content"]): SDKAssistantMessage["message"] {
  return {
    id: "msg_test",
    container: null,
    content,
    context_management: null,
    diagnostics: null,
    model: MODEL,
    role: "assistant",
    stop_details: null,
    stop_reason: null,
    stop_sequence: null,
    type: "message",
    usage: USAGE,
  };
}

function textBlock(text: string): Extract<SDKAssistantMessage["message"]["content"][number], { type: "text" }> {
  return { citations: null, text, type: "text" };
}

function redactedThinkingBlock(): Extract<SDKAssistantMessage["message"]["content"][number], { type: "redacted_thinking" }> {
  return { data: "opaque", type: "redacted_thinking" };
}

function toolUseBlock(id: string, name: string, input: unknown): Extract<SDKAssistantMessage["message"]["content"][number], { type: "tool_use" }> {
  return { id, input, name, type: "tool_use" };
}

function userTextBlock(text: string): Extract<Exclude<SDKUserMessage["message"]["content"], string>[number], { type: "text" }> {
  return { text, type: "text" };
}

function toolResultBlock(toolUseId: string, text: string, isError = false): Extract<Exclude<SDKUserMessage["message"]["content"], string>[number], { type: "tool_result" }> {
  return { tool_use_id: toolUseId, type: "tool_result", content: [{ text, type: "text" }], is_error: isError };
}

function assistantMessage(
  content: SDKAssistantMessage["message"]["content"],
  options: Readonly<{ parentToolUseId?: string; subagentType?: string; taskDescription?: string }> = {},
): SDKAssistantMessage {
  const { parentToolUseId = null, subagentType, taskDescription } = options;
  return {
    type: "assistant",
    message: betaMessage(content),
    parent_tool_use_id: parentToolUseId,
    uuid: nextUuid(),
    session_id: SESSION_ID,
    ...(subagentType === undefined ? {} : { subagent_type: subagentType }),
    ...(taskDescription === undefined ? {} : { task_description: taskDescription }),
  };
}

function userMessage(
  content: SDKUserMessage["message"]["content"],
  options: Readonly<{ parentToolUseId?: string; subagentType?: string; taskDescription?: string }> = {},
): SDKUserMessage {
  const { parentToolUseId = null, subagentType, taskDescription } = options;
  return {
    type: "user",
    message: { role: "user", content },
    parent_tool_use_id: parentToolUseId,
    uuid: nextUuid(),
    session_id: SESSION_ID,
    ...(subagentType === undefined ? {} : { subagent_type: subagentType }),
    ...(taskDescription === undefined ? {} : { task_description: taskDescription }),
  };
}

/** The one case `SDKUserMessage`'s own types admit and `SDKAssistantMessage`'s do not: no `session_id` at all. */
function userMessageWithNoSessionId(content: SDKUserMessage["message"]["content"]): SDKUserMessage {
  return { type: "user", message: { role: "user", content }, parent_tool_use_id: null, uuid: nextUuid() };
}

function systemInit(): SDKSystemMessage {
  return {
    type: "system",
    subtype: "init",
    agents: [],
    apiKeySource: "ANTHROPIC_API_KEY",
    claude_code_version: "2.1.240",
    cwd: "/repo",
    tools: ["Read", "Task", "WebFetch"],
    mcp_servers: [],
    model: MODEL,
    permissionMode: "default",
    slash_commands: [],
    output_style: "default",
    skills: [],
    plugins: [],
    uuid: nextUuid(),
    session_id: SESSION_ID,
  };
}

// --- Tests -------------------------------------------------------------------

describe("decodeSdkMessage", () => {
  it("system/init upserts exactly one Session, keyed by session_id", () => {
    const events = decodeSdkMessage(changeAt("1", systemInit()), emit);
    expect(events).toEqual([emit.nodes.Session.upsert(SESSION_ID, { model: MODEL, cwd: "/repo", apiKeySource: "ANTHROPIC_API_KEY" })]);
  });

  it("a non-init system message decodes to nothing (isModeledSdkMessage would reject it upstream)", () => {
    const stopped: SDKMessage = { type: "system", subtype: "worker_shutting_down", reason: "host_exit", uuid: nextUuid(), session_id: SESSION_ID };
    expect(decodeSdkMessage(changeAt("1", stopped), emit)).toEqual([]);
  });

  it("an assistant text message narrates a Turn attributed to the Session", () => {
    const events = decodeSdkMessage(changeAt("2", assistantMessage([textBlock("hello")])), emit);
    expect(events).toContainEqual(emit.nodes.Turn.upsert("2", { role: "assistant", text: "hello" }));
    expect(events).toContainEqual(emit.edges.narrated.upsert({ kind: "Session", id: SESSION_ID }, { kind: "Turn", id: "2" }));
    expect(events.some((event) => event.op === OP_NODE_UPSERT && event.kind === "ToolCall")).toBe(false);
  });

  it("a Read tool_use produces a ToolCall, a ran edge, an Artifact, and a touched edge", () => {
    const events = decodeSdkMessage(changeAt("3", assistantMessage([toolUseBlock("tu1", "Read", { file_path: "/repo/a.ts" })])), emit);
    expect(events).toContainEqual(emit.nodes.ToolCall.upsert("tu1", { name: "Read", input: JSON.stringify({ file_path: "/repo/a.ts" }) }));
    expect(events).toContainEqual(emit.edges.ran.upsert({ kind: "Session", id: SESSION_ID }, { kind: "ToolCall", id: "tu1" }));
    expect(events).toContainEqual(emit.nodes.Artifact.upsert("file:/repo/a.ts", { artifactKind: "file", path: "/repo/a.ts" }));
    expect(events).toContainEqual(emit.edges.touched.upsert({ kind: "ToolCall", id: "tu1" }, { kind: "Artifact", id: "file:/repo/a.ts" }));
  });

  it("a WebFetch tool_use produces a url Artifact, not a file one", () => {
    const events = decodeSdkMessage(changeAt("3", assistantMessage([toolUseBlock("tu1", "WebFetch", { url: "https://example.com/x", prompt: "read it" })])), emit);
    expect(events).toContainEqual(emit.nodes.Artifact.upsert("url:https://example.com/x", { artifactKind: "url", path: "https://example.com/x" }));
  });

  it("a Bash tool_use produces a ToolCall but no Artifact — an intentional scope cut, see the README", () => {
    const events = decodeSdkMessage(changeAt("4", assistantMessage([toolUseBlock("tu2", "Bash", { command: "ls" })])), emit);
    expect(events).toContainEqual(emit.nodes.ToolCall.upsert("tu2", { name: "Bash", input: JSON.stringify({ command: "ls" }) }));
    expect(events.some((event) => event.op === OP_NODE_UPSERT && event.kind === "Artifact")).toBe(false);
    expect(events.some((event) => event.op === OP_EDGE_UPSERT && event.kind === "touched")).toBe(false);
  });

  it("a Read tool_use with an input that fails the file_path check produces no Artifact", () => {
    const events = decodeSdkMessage(changeAt("5", assistantMessage([toolUseBlock("tu3", "Read", { path: "/wrong/field/name" })])), emit);
    expect(events.some((event) => event.op === OP_NODE_UPSERT && event.kind === "Artifact")).toBe(false);
  });

  it("the same path touched by two different tool calls resolves to one Artifact id", () => {
    const first = decodeSdkMessage(changeAt("6", assistantMessage([toolUseBlock("tu4", "Read", { file_path: "/repo/shared.ts" })])), emit);
    const second = decodeSdkMessage(changeAt("7", assistantMessage([toolUseBlock("tu5", "Edit", { file_path: "/repo/shared.ts" })])), emit);
    const firstArtifactId = first.find((event) => event.op === OP_NODE_UPSERT && event.kind === "Artifact")?.id;
    const secondArtifactId = second.find((event) => event.op === OP_NODE_UPSERT && event.kind === "Artifact")?.id;
    expect(firstArtifactId).toBe("file:/repo/shared.ts");
    expect(firstArtifactId).toBe(secondArtifactId);
  });

  it("an Artifact upsert's props use `artifactKind`, never `kind` — the row-kind naming collision this schema avoids", () => {
    const events = decodeSdkMessage(changeAt("8", assistantMessage([toolUseBlock("tu6", "Read", { file_path: "/repo/x.ts" })])), emit);
    const artifactUpsert = events.find((event) => event.op === OP_NODE_UPSERT && event.kind === "Artifact");
    if (artifactUpsert === undefined || artifactUpsert.op !== OP_NODE_UPSERT) throw new Error("expected an Artifact upsert event");
    expect(artifactUpsert.props).toEqual({ artifactKind: "file", path: "/repo/x.ts" });
    expect(artifactUpsert.props).not.toHaveProperty("kind");
  });

  it("a tool_result produces a ToolResult and a hasResult edge back to the ToolCall it answers", () => {
    const events = decodeSdkMessage(changeAt("9", userMessage([toolResultBlock("tu1", "file contents")])), emit);
    expect(events).toContainEqual(emit.nodes.ToolResult.upsert("tu1", { status: "ok", resultText: "file contents" }));
    expect(events).toContainEqual(emit.edges.hasResult.upsert({ kind: "ToolCall", id: "tu1" }, { kind: "ToolResult", id: "tu1" }));
  });

  it("is_error: true on a tool_result is decoded as status: error", () => {
    const events = decodeSdkMessage(changeAt("9", userMessage([toolResultBlock("tu1", "boom", true)])), emit);
    expect(events).toContainEqual(emit.nodes.ToolResult.upsert("tu1", { status: "error", resultText: "boom" }));
  });

  it("a subagent message (parent_tool_use_id set) births a Subagent, a spawned edge, and attributes its Turn/ToolCall to the Subagent, not the Session", () => {
    const events = decodeSdkMessage(
      changeAt(
        "10",
        assistantMessage([textBlock("investigating"), toolUseBlock("tu-sub-1", "Read", { file_path: "/repo/lock.json" })], {
          parentToolUseId: "tu-task",
          subagentType: "general-purpose",
          taskDescription: "investigate deps",
        }),
      ),
      emit,
    );
    expect(events).toContainEqual(emit.nodes.Subagent.upsert("tu-task", { subagentType: "general-purpose", taskDescription: "investigate deps", sessionId: SESSION_ID }));
    expect(events).toContainEqual(emit.edges.spawned.upsert({ kind: "ToolCall", id: "tu-task" }, { kind: "Subagent", id: "tu-task" }));
    expect(events).toContainEqual(emit.edges.narrated.upsert({ kind: "Subagent", id: "tu-task" }, { kind: "Turn", id: "10" }));
    expect(events).toContainEqual(emit.edges.ran.upsert({ kind: "Subagent", id: "tu-task" }, { kind: "ToolCall", id: "tu-sub-1" }));
    // Not attributed to the main session at all.
    expect(events.some((event) => event.op === OP_EDGE_UPSERT && event.kind === "narrated" && event.from.kind === "Session")).toBe(false);
  });

  it("a subagent message without subagent_type/task_description falls back to 'unknown'/'' rather than dropping the Subagent row", () => {
    const events = decodeSdkMessage(changeAt("11", assistantMessage([textBlock("hi")], { parentToolUseId: "tu-task-2" })), emit);
    expect(events).toContainEqual(emit.nodes.Subagent.upsert("tu-task-2", { subagentType: "unknown", taskDescription: "", sessionId: SESSION_ID }));
  });

  it("never returns zero events for a message isModeledSdkMessage accepts — even one carrying only content blocks this decoder does not otherwise model", () => {
    const events = decodeSdkMessage(changeAt("12", assistantMessage([redactedThinkingBlock()])), emit);
    expect(events.length).toBeGreaterThan(0);
    expect(events).toContainEqual(emit.nodes.Turn.upsert("12", { role: "assistant", text: "" }));
  });

  it("a main-session user message with no session_id at all (SDKUserMessage's one optional-session_id case) skips the narrated edge rather than fabricating a Session id, but still records the Turn and any tool_result", () => {
    const events = decodeSdkMessage(changeAt("13", userMessageWithNoSessionId([userTextBlock("orphaned"), toolResultBlock("tu9", "ok")])), emit);
    expect(events).toContainEqual(emit.nodes.Turn.upsert("13", { role: "user", text: "orphaned" }));
    expect(events).toContainEqual(emit.nodes.ToolResult.upsert("tu9", { status: "ok", resultText: "ok" }));
    expect(events.some((event) => event.op === OP_EDGE_UPSERT && event.kind === "narrated")).toBe(false);
  });

  it("a user text prompt narrates a Turn the same way assistant text does", () => {
    const events = decodeSdkMessage(changeAt("14", userMessage([userTextBlock("please read package.json")])), emit);
    expect(events).toContainEqual(emit.nodes.Turn.upsert("14", { role: "user", text: "please read package.json" }));
    expect(events).toContainEqual(emit.edges.narrated.upsert({ kind: "Session", id: SESSION_ID }, { kind: "Turn", id: "14" }));
  });
});

describe("isModeledSdkMessage", () => {
  it("accepts system/init, assistant, and user messages", () => {
    expect(isModeledSdkMessage(systemInit())).toBe(true);
    expect(isModeledSdkMessage(assistantMessage([textBlock("hi")]))).toBe(true);
    expect(isModeledSdkMessage(userMessage([userTextBlock("hi")]))).toBe(true);
  });

  it("rejects a non-init system message and every other SDKMessage type", () => {
    const workerShuttingDown: SDKMessage = { type: "system", subtype: "worker_shutting_down", reason: "host_exit", uuid: nextUuid(), session_id: SESSION_ID };
    const toolUseSummary: SDKMessage = { type: "tool_use_summary", summary: "read 2 files", preceding_tool_use_ids: ["tu1", "tu2"], uuid: nextUuid(), session_id: SESSION_ID };
    expect(isModeledSdkMessage(workerShuttingDown)).toBe(false);
    expect(isModeledSdkMessage(toolUseSummary)).toBe(false);
  });
});
