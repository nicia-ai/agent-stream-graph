/**
 * Demo — a real Claude agent session, materialized into a bitemporal belief
 * graph: what did the main session do, what did the subagent it spawned do,
 * and which files/URLs did each of them touch.
 *
 * OFFLINE by default (`fixtures/session.json`, a hand-authored transcript —
 * see the README for why it's hand-authored rather than recorded), LIVE when
 * `ANTHROPIC_API_KEY` is set. Either way the exact same `decodeSdkMessage`
 * and `agentSessionGraph` run — only `source.ts`'s upstream (`replaySource`
 * vs `liveSource`) differs. That is the whole point of the decoder seam:
 * `decode.test.ts` proves the decoder correct with no SDK running at all,
 * and this file proves the seam actually plugs into a real message stream.
 *
 * Run with:  pnpm demo
 */
import { fileURLToPath, pathToFileURL } from "node:url";

import { checkpointGraph, consume, graphProjector, typeGraphCheckpoints } from "@nicia-ai/agent-stream-graph";
import type { ShapeChange } from "@nicia-ai/agent-stream-graph";
import { recordedInstantRevision } from "@nicia-ai/typegraph";

import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";

import { decodeSdkMessage } from "./decode.js";
import { agentSessionGraph } from "./graph.js";
import { liveSource } from "./live.js";
import { replaySource } from "./replay.js";
import { newStore } from "./store.js";

const RULE = "━".repeat(74);
function section(title: string): void {
  console.log("\n" + RULE);
  console.log(` ${title}`);
  console.log(RULE);
}

const STREAM_NAME = "claude-agent-demo-session";
const FIXTURE_PATH = fileURLToPath(new URL("../fixtures/session.json", import.meta.url));

/**
 * The offset at which the RESULT of the `n`th tool call (across the whole
 * session, main and subagent alike, in stream order) was recorded — found
 * by walking the raw changes rather than hardcoding a fixture-specific
 * offset, so this stays correct if `fixtures/session.json` grows or shrinks.
 * `undefined` if the stream never reaches `n` tool calls.
 */
function offsetAfterNthToolCallResult(changes: readonly ShapeChange<SDKMessage>[], n: number): string | undefined {
  let toolCallsSeen = 0;
  let targetToolUseId: string | undefined;
  for (const change of changes) {
    const message = change.value;
    if (message.type === "assistant") {
      for (const block of message.message.content) {
        if (block.type === "tool_use") {
          toolCallsSeen += 1;
          if (toolCallsSeen === n) targetToolUseId = block.id;
        }
      }
    }
    if (targetToolUseId === undefined || message.type !== "user") continue;
    const content = message.message.content;
    if (typeof content === "string") continue;
    for (const block of content) {
      if (block.type === "tool_result" && block.tool_use_id === targetToolUseId) return change.offset;
    }
  }
  return undefined;
}

type ArtifactRow = Readonly<{ artifactKind: string; path: string }>;
type TurnRow = Readonly<{ role: string; text: string }>;
type AgentView = { query: Awaited<ReturnType<typeof newStore<typeof agentSessionGraph>>>["query"] };

/** Files/URLs touched by tool calls this agent (a Session or a Subagent) itself ran — the "interesting query" from the package's design brief, as a two-hop traversal. */
async function artifactsTouchedBy(view: AgentView, agentKind: "Session" | "Subagent"): Promise<readonly ArtifactRow[]> {
  const rows = await view
    .query()
    .from(agentKind, "agent")
    .traverse("ran", "r")
    .to("ToolCall", "tc")
    .traverse("touched", "t")
    .to("Artifact", "a")
    .select((c) => ({ artifactKind: c.a.artifactKind, path: c.a.path }))
    .execute();
  return [...rows].sort((left, right) => left.path.localeCompare(right.path));
}

/** Narrated text (assistant/user) this agent kind authored — how a subagent's contribution stays distinguishable from the parent's. */
async function turnsNarratedBy(view: AgentView, agentKind: "Session" | "Subagent"): Promise<readonly TurnRow[]> {
  return view.query().from(agentKind, "agent").traverse("narrated", "n").to("Turn", "t").select((c) => ({ role: c.t.role, text: c.t.text })).execute();
}

async function toolCallCount(view: AgentView): Promise<number> {
  return (await view.query().from("ToolCall", "tc").select((c) => ({ id: c.tc.id })).execute()).length;
}

async function artifactCount(view: AgentView): Promise<number> {
  return (await view.query().from("Artifact", "a").select((c) => ({ id: c.a.id })).execute()).length;
}

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" Claude Agent SDK session -> bitemporal belief graph");
  console.log(RULE);

  const live = process.env.ANTHROPIC_API_KEY !== undefined;
  console.log(`\n  mode: ${live ? "LIVE (ANTHROPIC_API_KEY is set — spawning a real session)" : "OFFLINE (replaying fixtures/session.json)"}`);

  const belief = await newStore(agentSessionGraph, true);
  const cursor = await newStore(checkpointGraph, false);
  const stores = [belief, cursor];

  try {
    const book = typeGraphCheckpoints(cursor);
    const source = live ? liveSource(STREAM_NAME) : replaySource(STREAM_NAME, FIXTURE_PATH);
    const project = graphProjector(agentSessionGraph, decodeSdkMessage);

    section("(a) Durable, resumable ingestion of the message stream");
    const result = await consume({ source, store: belief, checkpoints: book, project });
    console.log(`\n  consumed ${result.processed} SDK messages, cursor at offset ${result.lastOffset}`);

    section("(b) The graph's shape");
    const sessions = await belief.query().from("Session", "s").select((c) => ({ id: c.s.id, model: c.s.model, cwd: c.s.cwd })).execute();
    const subagents = await belief.query().from("Subagent", "sa").select((c) => ({ id: c.sa.id, subagentType: c.sa.subagentType, taskDescription: c.sa.taskDescription })).execute();
    for (const session of sessions) console.log(`\n  Session ${session.id}\n    model: ${session.model}\n    cwd:   ${session.cwd}`);
    for (const subagent of subagents) console.log(`\n  Subagent ${subagent.id}\n    type: ${subagent.subagentType}\n    task: ${subagent.taskDescription}`);
    console.log(`\n  ${await toolCallCount(belief)} tool calls total, ${await artifactCount(belief)} distinct artifacts touched`);

    section("(c) Which files did THIS SESSION touch, and which did the SUBAGENT touch?");
    const firstSession = sessions[0];
    const firstSubagent = subagents[0];
    if (firstSession === undefined || firstSubagent === undefined) {
      throw new Error("demo fixture is broken: expected one Session and one Subagent, found none");
    }
    const sessionArtifacts = await artifactsTouchedBy(belief, "Session");
    const subagentArtifacts = await artifactsTouchedBy(belief, "Subagent");
    console.log(`\n  session ${firstSession.id} directly touched:`);
    for (const artifact of sessionArtifacts) console.log(`    ${artifact.artifactKind.padEnd(4)} ${artifact.path}`);
    console.log(`\n  subagent ${firstSubagent.id} touched:`);
    for (const artifact of subagentArtifacts) console.log(`    ${artifact.artifactKind.padEnd(4)} ${artifact.path}`);

    const overlap = sessionArtifacts.filter((s) => subagentArtifacts.some((sa) => sa.path === s.path));
    if (sessionArtifacts.length === 0 || subagentArtifacts.length === 0) {
      throw new Error("demo fixture is broken: expected both the session and the subagent to have touched at least one artifact");
    }
    if (overlap.length > 0) {
      throw new Error(`expected disjoint artifact sets in this fixture; both session and subagent touched: ${overlap.map((a) => a.path).join(", ")}`);
    }
    console.log(`\n  >>> disjoint sets, two hops each, no transcript search: Subagent -[ran]-> ToolCall -[touched]-> Artifact <<<`);

    section("(d) Subagent contributions stay distinguishable from the parent's");
    const sessionTurns = await turnsNarratedBy(belief, "Session");
    const subagentTurns = await turnsNarratedBy(belief, "Subagent");
    // Every assistant/user message frame gets a `Turn` row, tool_use/tool_result-only
    // frames included (see decode.ts's comment on why that write is unconditional) —
    // only the ones with actual narrated text are worth printing here.
    const sessionNarration = sessionTurns.filter((turn) => turn.text !== "");
    const subagentNarration = subagentTurns.filter((turn) => turn.text !== "");
    console.log(`\n  session: ${sessionTurns.length} message frames, ${sessionNarration.length} with narrated text`);
    console.log(`  subagent: ${subagentTurns.length} message frames, ${subagentNarration.length} with narrated text`);
    for (const turn of subagentNarration) console.log(`    [subagent, ${turn.role}] ${turn.text}`);
    if (sessionNarration.length === 0 || subagentNarration.length === 0) {
      throw new Error(
        "demo fixture is broken, or a live run's forwardSubagentText did not take effect: expected both the " +
          "session and the subagent to have narrated at least one turn with actual text",
      );
    }

    section("(e) Time travel — what had the agent discovered by the 4th tool call?");
    const changes = await source.read(undefined);
    const N = 4;
    const milestoneOffset = offsetAfterNthToolCallResult(changes, N);
    if (milestoneOffset === undefined) {
      throw new Error(`this session never reached ${N} tool calls — cannot demonstrate the milestone`);
    }
    const anchor = await book.anchorFor(STREAM_NAME, milestoneOffset);
    if (anchor === undefined) {
      throw new Error(`no checkpoint anchor recorded at offset ${milestoneOffset}`);
    }
    const pastView = belief.asOfRecorded(anchor);
    const [pastToolCalls, pastArtifacts, currentToolCalls, currentArtifacts] = await Promise.all([
      toolCallCount(pastView),
      artifactCount(pastView),
      toolCallCount(belief),
      artifactCount(belief),
    ]);
    console.log(`\n  book.anchorFor("${STREAM_NAME}", "${milestoneOffset}") -> revision ${recordedInstantRevision(anchor)}`);
    console.log(`  as of tool call ${N}'s result:  ${pastToolCalls} tool calls known, ${pastArtifacts} artifacts discovered`);
    console.log(`  current (end of session):    ${currentToolCalls} tool calls known, ${currentArtifacts} artifacts discovered`);

    if (!(pastToolCalls < currentToolCalls && pastArtifacts < currentArtifacts)) {
      throw new Error(
        `time travel did not show a narrower belief in the past: past(toolCalls=${pastToolCalls}, artifacts=${pastArtifacts}) ` +
          `vs current(toolCalls=${currentToolCalls}, artifacts=${currentArtifacts}) — the milestone offset picked ` +
          "the end of the stream instead of a genuine midpoint",
      );
    }
    console.log(`\n  >>> the belief graph as of tool call ${N} is a STRICT PREFIX of the current one — reconstructed`);
    console.log(`      from the recorded-time anchor, not re-derived by replaying the whole transcript again. <<<`);

    console.log("\n" + RULE);
    console.log(" A session's tool calls, its subagent's tool calls, and everything either");
    console.log(" of them touched are one graph — queryable, and queryable as of any offset.");
    console.log(RULE + "\n");
  } finally {
    await Promise.allSettled(stores.map((store) => store.close()));
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exit(1);
  });
}
