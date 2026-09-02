/**
 * A `ShapeSource` over a Claude Agent SDK message stream — live or replayed,
 * both go through this one adapter (`live.ts` feeds it a real `Query`,
 * `replay.ts` feeds it a fixture turned into an async generator).
 *
 * BE HONEST ABOUT WHAT "RESUME" MEANS HERE. `ShapeSource.read(after)`'s
 * contract is a durable-log contract: a fresh instance, pointed at the same
 * name, can ask "everything after `after`" and get it — that is what Electric
 * and `@durable-streams/client` actually provide, because the log lives on a
 * server this process can reconnect to. A live agent session has no such
 * server. `query()` spawns a subprocess and hands back one `AsyncGenerator`;
 * there is nothing else to reconnect to, and no way to ask a dead subprocess
 * to resend message 42. Once the generator this source wraps is exhausted —
 * turn complete, or the process died — `read()` keeps returning whatever it
 * already buffered and NOTHING MORE, forever, regardless of what offset the
 * caller passes. That is not "caught up"; it is "there is nothing left to
 * give", and a caller that cannot tell the two apart will believe a crashed
 * session quietly finished cleanly.
 *
 * What DOES survive a crash is the CONSUMER side of this library: the belief
 * store and the checkpoint book are durable, so a process that dies mid-`
 * consume()` and restarts resumes its PROJECTION correctly from the last
 * commit — see `examples/agents.ts`'s crash-resume section for that half of
 * the story, which this package inherits unchanged. What this adapter cannot
 * do is resurrect the dead SESSION and pull new messages out of it; that
 * would need the SDK itself to expose session reconnection, which — as of
 * `@anthropic-ai/claude-agent-sdk@0.3.250` — it does not.
 *
 * A second, narrower honesty point: offsets are a monotonic counter local to
 * ONE `sdkMessageSource(...)` call. A genuine crash-and-relaunch (a new
 * subprocess, a new `AsyncGenerator`, wrapped in a NEW call to this function)
 * restarts that counter at 1 — which collides with the first instance's
 * offsets in the same checkpoint book. Pass `startAfterSequence` (typically
 * `Number(await book.lastOffset(name))`, when offsets are known to be plain
 * decimal counters) to seed past that collision on a genuine reconnect; the
 * demo never needs this because it runs one source for one session.
 */
import { compareOffsets, type ShapeChange, type ShapeSource } from "@nicia-ai/agent-stream-graph";

import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";

/** Width of the zero-padded decimal offset — generous enough that no real session's message count approaches it. */
const OFFSET_WIDTH = 9;

function offsetFor(sequence: number): string {
  return String(sequence).padStart(OFFSET_WIDTH, "0");
}

function keyFor(message: SDKMessage, sequence: number): string {
  const uuid = "uuid" in message ? message.uuid : undefined;
  return uuid ?? `${message.session_id}:${sequence}`;
}

export type SdkMessageSourceOptions = Readonly<{
  /** Seed the offset counter past a prior instance's high-water mark — see the "genuine reconnect" paragraph above. Default 0. */
  startAfterSequence?: number;
  /**
   * Only messages this returns `true` for become a `ShapeChange` at all —
   * everything else is dropped before it is ever assigned an offset, exactly
   * as `electricShapeSource` (this library's own Electric adapter) drops
   * Electric's control messages rather than surfacing them as changes with
   * nothing in them. Pass `decodeSdkMessage`'s paired `isModeledSdkMessage`
   * (both `replay.ts` and `live.ts` do) so that every `ShapeChange` this
   * source produces is one `decodeSdkMessage` is guaranteed to turn into at
   * least one graph write — a message type neither of them agrees on is
   * silently skipped rather than tripping `consume()`'s
   * `ProjectorRecordedNothingError`. Default: no filtering, i.e. every
   * message becomes a change (useful for a decoder with broader scope than
   * this package's own).
   */
  filter?: (message: SDKMessage) => boolean;
}>;

/**
 * Wrap `messages` — a live `Query` (itself an `AsyncGenerator<SDKMessage>`)
 * or a replayed fixture generator — as a `ShapeSource<SDKMessage>`.
 *
 * `read()` drains `messages` to completion on its FIRST call: an agent
 * session has no natural "batch" boundary the way an Electric catch-up page
 * does, so the whole remaining stream — up to wherever it currently ends —
 * is the batch. This is safe for `consume()`'s `stopAfter` crash simulation:
 * `stopAfter` bounds how many of the RETURNED changes get applied and
 * checkpointed, not how many `read()` fetches, exactly as it already works
 * over `mockShapeSource`'s fixed array.
 */
export function sdkMessageSource(name: string, messages: AsyncIterable<SDKMessage>, options: SdkMessageSourceOptions = {}): ShapeSource<SDKMessage> {
  const { startAfterSequence = 0, filter } = options;
  const buffered: ShapeChange<SDKMessage>[] = [];
  let sequence = startAfterSequence;
  let iterator: AsyncIterator<SDKMessage> | undefined;
  let exhausted = false;

  async function drain(): Promise<void> {
    if (exhausted) return;
    iterator ??= messages[Symbol.asyncIterator]();
    for (;;) {
      const next = await iterator.next();
      if (next.done === true) {
        exhausted = true;
        return;
      }
      if (filter !== undefined && !filter(next.value)) continue;
      sequence += 1;
      buffered.push({
        offset: offsetFor(sequence),
        shape: "sdk-message",
        key: keyFor(next.value, sequence),
        operation: "insert",
        value: next.value,
      });
    }
  }

  return {
    name,
    async read(after) {
      await drain();
      return after === undefined ? buffered : buffered.filter((change) => compareOffsets(change.offset, after) > 0);
    },
  };
}
