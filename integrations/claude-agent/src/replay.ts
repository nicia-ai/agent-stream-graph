/**
 * The offline path: a recorded transcript in `fixtures/`, streamed through
 * the exact same `sdkMessageSource` / `decodeSdkMessage` that a live session
 * uses. Nothing here is SDK-specific beyond the message shape — this module
 * proves the decoder seam by construction: swap `replaySource` for
 * `liveSource` (see `live.ts`) and every downstream line in `demo.ts` is
 * unchanged.
 */
import { readFile } from "node:fs/promises";

import type { ShapeSource } from "@nicia-ai/agent-stream-graph";

import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";

import { isModeledSdkMessage } from "./decode.js";
import { sdkMessageSource } from "./source.js";

/** Turn a fixture file's message array into the `AsyncIterable` `sdkMessageSource` expects. */
async function* readFixture(path: string): AsyncGenerator<SDKMessage> {
  const raw = await readFile(path, "utf8");
  // `JSON.parse`'s return type is `any` — assigned straight to a typed const,
  // the same idiom `examples/emit.ts` uses for its own JSON round trip. The
  // fixture is HAND-AUTHORED against the shipped `sdk.d.ts` (see fixtures/
  // in the README), not captured from a live run, so this is trust in the
  // fixture's authorship, not a runtime validation step.
  const messages: readonly SDKMessage[] = JSON.parse(raw);
  for (const message of messages) yield message;
}

/** A `ShapeSource` over the recorded transcript at `fixturePath`. */
export function replaySource(name: string, fixturePath: string): ShapeSource<SDKMessage> {
  return sdkMessageSource(name, readFixture(fixturePath), { filter: isModeledSdkMessage });
}
