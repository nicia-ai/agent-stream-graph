/**
 * The real path: spawn an actual Claude Agent SDK session and stream it
 * through the same `sdkMessageSource` the offline path uses. Only reachable
 * when `ANTHROPIC_API_KEY` is set — see `demo.ts` for the mode switch.
 *
 * `forwardSubagentText: true` is load-bearing, not decorative: by default the
 * SDK forwards only tool_use/tool_result blocks from a subagent (enough for a
 * heartbeat counter) and drops its text/thinking. Without this flag, a live
 * run's `Turn` nodes for subagent narration would silently be missing — the
 * graph would still show WHICH files a subagent touched (`ToolCall`/
 * `Artifact` survive either way) but not WHAT the subagent said about them.
 * The default is easy to miss precisely because nothing fails when it bites:
 * the run succeeds, the graph populates, and only the subagents' reasoning is
 * quietly absent. Setting the flag here is what keeps the live and replayed
 * graphs comparable in shape.
 */
import { query } from "@anthropic-ai/claude-agent-sdk";

import type { ShapeSource } from "@nicia-ai/agent-stream-graph";

import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";

import { isModeledSdkMessage } from "./decode.js";
import { sdkMessageSource } from "./source.js";

const DEFAULT_PROMPT =
  "Read package.json and README.md in this repository, then use the Task tool to spawn a subagent " +
  "that checks package-lock.json for anything unusual and looks up the latest published npm version " +
  "of one dependency it finds there. Once the subagent reports back, give a one-paragraph summary.";

/**
 * A `ShapeSource` over a real `query()` run.
 *
 * `options.env` REPLACES the subprocess environment rather than merging with
 * it — spreading `process.env` here is what keeps `ANTHROPIC_API_KEY`, `PATH`,
 * and `HOME` reaching the child process at all. Omitting `env` entirely would
 * also inherit `process.env` (the SDK's own default), but stating it
 * explicitly is what lets this function also stamp `CLAUDE_AGENT_SDK_CLIENT_APP`
 * without silently dropping everything else.
 */
export function liveSource(name: string, prompt: string = DEFAULT_PROMPT): ShapeSource<SDKMessage> {
  const result = query({
    prompt,
    options: {
      forwardSubagentText: true,
      env: { ...process.env, CLAUDE_AGENT_SDK_CLIENT_APP: "asg-claude-agent-demo/0.1.0" },
    },
  });
  return sdkMessageSource(name, result, { filter: isModeledSdkMessage });
}
