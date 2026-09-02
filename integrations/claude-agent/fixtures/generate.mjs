// Generator for session.json in this directory. Run with:
//   node fixtures/generate.mjs
//
// HAND-AUTHORED, not captured from a live SDK run (no ANTHROPIC_API_KEY was
// available while building this package). Every message shape below was
// checked field-by-field against the shipped `sdk.d.ts` inside
// @anthropic-ai/claude-agent-sdk@0.3.250 (SDKAssistantMessage, SDKUserMessage,
// SDKSystemMessage, SDKResultMessage) and the Anthropic SDK's BetaMessage /
// BetaUsage / content-block interfaces, so every REQUIRED field is present
// with a plausible value. It is a weaker claim than a recorded transcript
// would be -- see the package README's "The fixture is hand-authored, not
// recorded" section.
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const MODEL = "claude-opus-5";
const SESSION_ID = "sess-01demo000000000000000000";
const CLI_VERSION = "2.1.240";

let msgCounter = 0;
const nextMsgId = () => `msg_${String(++msgCounter).padStart(3, "0")}demoFixture`;
let uuidCounter = 0;
const nextUuid = () => {
  uuidCounter += 1;
  const hex = uuidCounter.toString(16).padStart(12, "0");
  return `00000000-0000-4000-8000-${hex}`;
};

function usage(inputTokens, outputTokens) {
  return {
    cache_creation: null,
    cache_creation_input_tokens: null,
    cache_read_input_tokens: null,
    fallback_credit: null,
    inference_geo: null,
    input_tokens: inputTokens,
    iterations: null,
    output_tokens: outputTokens,
    output_tokens_details: null,
    server_tool_use: null,
    service_tier: "standard",
    speed: "standard",
  };
}

function textBlock(text) {
  return { citations: null, text, type: "text" };
}

function toolUseBlock(id, name, input) {
  return { id, input, name, type: "tool_use" };
}

function betaMessage({ content, stopReason, inputTokens = 200, outputTokens = 60 }) {
  return {
    id: nextMsgId(),
    container: null,
    content,
    context_management: null,
    diagnostics: null,
    model: MODEL,
    role: "assistant",
    stop_details: null,
    stop_reason: stopReason,
    stop_sequence: null,
    type: "message",
    usage: usage(inputTokens, outputTokens),
  };
}

/** One assistant SDKMessage carrying exactly one content block, matching the
 * documented "one assistant message per completed content block" behavior. */
function assistant({ block, stopReason, parentToolUseId = null, subagentType, taskDescription, timestamp }) {
  return {
    type: "assistant",
    message: betaMessage({ content: [block], stopReason }),
    parent_tool_use_id: parentToolUseId,
    uuid: nextUuid(),
    session_id: SESSION_ID,
    ...(subagentType === undefined ? {} : { subagent_type: subagentType }),
    ...(taskDescription === undefined ? {} : { task_description: taskDescription }),
    ...(timestamp === undefined ? {} : { timestamp }),
  };
}

function toolResultBlock(toolUseId, text, isError = false) {
  return {
    tool_use_id: toolUseId,
    type: "tool_result",
    content: [{ text, type: "text" }],
    is_error: isError,
  };
}

function user({ block, parentToolUseId = null, subagentType, taskDescription, timestamp }) {
  return {
    type: "user",
    message: { role: "user", content: [block] },
    parent_tool_use_id: parentToolUseId,
    uuid: nextUuid(),
    session_id: SESSION_ID,
    ...(subagentType === undefined ? {} : { subagent_type: subagentType }),
    ...(taskDescription === undefined ? {} : { task_description: taskDescription }),
    ...(timestamp === undefined ? {} : { timestamp }),
  };
}

function systemInit() {
  return {
    type: "system",
    subtype: "init",
    agents: ["general-purpose"],
    apiKeySource: "ANTHROPIC_API_KEY",
    claude_code_version: CLI_VERSION,
    cwd: "/repo",
    tools: ["Read", "Write", "Edit", "Bash", "Task", "WebFetch", "Grep", "Glob"],
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

function resultSuccess() {
  return {
    type: "result",
    subtype: "success",
    duration_ms: 41250,
    duration_api_ms: 38310,
    is_error: false,
    num_turns: 6,
    result: "Summary: the project reads cleanly and its dependencies show no anomalies.",
    stop_reason: "end_turn",
    total_cost_usd: 0.0842,
    usage: { input_tokens: 1240, output_tokens: 410, cache_creation_input_tokens: null, cache_read_input_tokens: null },
    modelUsage: {
      [MODEL]: {
        inputTokens: 1240,
        outputTokens: 410,
        cacheReadInputTokens: 0,
        cacheCreationInputTokens: 0,
        webSearchRequests: 0,
        costUSD: 0.0842,
      },
    },
    permission_denials: [],
    uuid: nextUuid(),
    session_id: SESSION_ID,
  };
}

const TASK_TOOL_USE_ID = "toolu_03demoTaskInvestigateDeps";

const messages = [
  systemInit(),

  assistant({
    block: textBlock("I'll read package.json and README.md to get oriented, then summarize the project."),
    stopReason: "end_turn",
    timestamp: "2026-08-28T15:00:01.000Z",
  }),

  assistant({
    block: toolUseBlock("toolu_01demoReadPackageJson", "Read", { file_path: "/repo/package.json" }),
    stopReason: "tool_use",
    timestamp: "2026-08-28T15:00:02.000Z",
  }),
  user({
    block: toolResultBlock("toolu_01demoReadPackageJson", '{ "name": "demo", "version": "0.1.0", "dependencies": { "left-pad": "^1.3.0" } }'),
    timestamp: "2026-08-28T15:00:03.000Z",
  }),

  assistant({
    block: toolUseBlock("toolu_02demoReadReadme", "Read", { file_path: "/repo/README.md" }),
    stopReason: "tool_use",
    timestamp: "2026-08-28T15:00:04.000Z",
  }),
  user({
    block: toolResultBlock("toolu_02demoReadReadme", "# demo\n\nA small demo project used for fixture data."),
    timestamp: "2026-08-28T15:00:05.000Z",
  }),

  assistant({
    block: textBlock("Now let me delegate a closer look at the dependency tree to a subagent."),
    stopReason: "end_turn",
    timestamp: "2026-08-28T15:00:06.000Z",
  }),

  // The Task tool_use that spawns the subagent. Its own id becomes the
  // subagent's parent_tool_use_id on every message the subagent produces.
  assistant({
    block: toolUseBlock(TASK_TOOL_USE_ID, "Task", {
      description: "investigate dependencies",
      subagent_type: "general-purpose",
      prompt: "Check package-lock.json for anything unusual and look up the latest published version of left-pad.",
    }),
    stopReason: "tool_use",
    timestamp: "2026-08-28T15:00:07.000Z",
  }),

  // --- Inside the subagent: every message below carries parent_tool_use_id
  // === TASK_TOOL_USE_ID, subagent_type, and task_description. ---
  assistant({
    block: textBlock("I'll check the lockfile, then look up left-pad's latest release."),
    stopReason: "end_turn",
    parentToolUseId: TASK_TOOL_USE_ID,
    subagentType: "general-purpose",
    taskDescription: "investigate dependencies",
    timestamp: "2026-08-28T15:00:08.000Z",
  }),
  assistant({
    block: toolUseBlock("toolu_04demoReadLockfile", "Read", { file_path: "/repo/package-lock.json" }),
    stopReason: "tool_use",
    parentToolUseId: TASK_TOOL_USE_ID,
    subagentType: "general-purpose",
    taskDescription: "investigate dependencies",
    timestamp: "2026-08-28T15:00:09.000Z",
  }),
  user({
    block: toolResultBlock("toolu_04demoReadLockfile", '{ "lockfileVersion": 3, "packages": { "node_modules/left-pad": { "version": "1.3.0" } } }'),
    parentToolUseId: TASK_TOOL_USE_ID,
    subagentType: "general-purpose",
    taskDescription: "investigate dependencies",
    timestamp: "2026-08-28T15:00:10.000Z",
  }),
  assistant({
    block: toolUseBlock("toolu_05demoFetchRegistry", "WebFetch", {
      url: "https://registry.npmjs.org/left-pad",
      prompt: "What is the latest published version?",
    }),
    stopReason: "tool_use",
    parentToolUseId: TASK_TOOL_USE_ID,
    subagentType: "general-purpose",
    taskDescription: "investigate dependencies",
    timestamp: "2026-08-28T15:00:11.000Z",
  }),
  user({
    block: toolResultBlock("toolu_05demoFetchRegistry", '{ "dist-tags": { "latest": "1.3.0" } }'),
    parentToolUseId: TASK_TOOL_USE_ID,
    subagentType: "general-purpose",
    taskDescription: "investigate dependencies",
    timestamp: "2026-08-28T15:00:12.000Z",
  }),
  assistant({
    block: textBlock("No anomalies: package-lock.json pins left-pad@1.3.0, which is also the latest published version."),
    stopReason: "end_turn",
    parentToolUseId: TASK_TOOL_USE_ID,
    subagentType: "general-purpose",
    taskDescription: "investigate dependencies",
    timestamp: "2026-08-28T15:00:13.000Z",
  }),
  // --- End of subagent activity. The Task tool_use's own result, delivered
  // back to the MAIN session (parent_tool_use_id null again). ---
  user({
    block: toolResultBlock(TASK_TOOL_USE_ID, "Subagent report: no anomalies found. package-lock.json pins left-pad@1.3.0, which matches the latest published release."),
    timestamp: "2026-08-28T15:00:14.000Z",
  }),

  assistant({
    block: textBlock("Summary: the project reads cleanly and its dependencies show no anomalies."),
    stopReason: "end_turn",
    timestamp: "2026-08-28T15:00:15.000Z",
  }),

  resultSuccess(),
];

const outPath = fileURLToPath(new URL("./session.json", import.meta.url));
writeFileSync(outPath, JSON.stringify(messages, null, 2) + "\n");
console.log(`wrote ${messages.length} messages to ${outPath}`);
