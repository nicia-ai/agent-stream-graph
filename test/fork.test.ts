import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  durableStreamSource,
  forkPointFor,
  forkStream,
  StreamForkError,
  type ShapeSource,
} from "../src";
import {
  startDurableStreamsServer,
  type DurableStreamsServer,
} from "./support/durable-streams-server";

const SOURCE_PATH = "agents/researcher";
const FORK_PATH = "agents/researcher-what-if";

type Message = Readonly<{ key: string; label: string }>;

describe("forkStream", () => {
  let server: DurableStreamsServer;

  const sourceFor = (path: string): ShapeSource<Message> =>
    durableStreamSource<Message, Message>({
      url: server.streamUrl(path),
      name: path,
      toChange: (item) => ({ shape: "item", key: item.key, operation: "insert", value: item }),
    });

  const keysOf = async (path: string): Promise<string[]> =>
    (await sourceFor(path).read(undefined)).map((change) => change.key);

  beforeEach(async () => {
    server = await startDurableStreamsServer();
    server.createStream(SOURCE_PATH);
    server.append(SOURCE_PATH, [
      { key: "a", label: "a1" },
      { key: "b", label: "b1" },
    ]);
    server.append(SOURCE_PATH, [{ key: "c", label: "c1" }]);
  });

  afterEach(async () => {
    await server.close();
  });

  it("branches at the exact message a checkpoint cursor names", async () => {
    // The cursor after "a" is mid-append. A fork anchored on the append boundary
    // alone could only branch before "a" or after "b" — the sub-offset is what
    // makes "exactly where this belief graph was" expressible.
    const changes = await sourceFor(SOURCE_PATH).read(undefined);
    const afterA = changes[0]!.offset;

    const result = await forkStream({
      url: server.streamUrl(FORK_PATH),
      sourcePath: SOURCE_PATH,
      at: forkPointFor(afterA),
    });

    expect(result.created).toBe(true);
    expect(await keysOf(FORK_PATH)).toEqual(["a"]);
  });

  it("lets the fork and its source diverge independently", async () => {
    const changes = await sourceFor(SOURCE_PATH).read(undefined);
    await forkStream({
      url: server.streamUrl(FORK_PATH),
      sourcePath: SOURCE_PATH,
      at: forkPointFor(changes[1]!.offset),
    });

    server.append(FORK_PATH, [{ key: "d", label: "d-on-the-fork" }]);
    server.append(SOURCE_PATH, [{ key: "e", label: "e-on-the-source" }]);

    expect(await keysOf(FORK_PATH)).toEqual(["a", "b", "d"]);
    expect(await keysOf(SOURCE_PATH)).toEqual(["a", "b", "c", "e"]);
  });

  it("is idempotent, so a restarted branch job converges", async () => {
    const at = forkPointFor((await sourceFor(SOURCE_PATH).read(undefined))[1]!.offset);
    const args = { url: server.streamUrl(FORK_PATH), sourcePath: SOURCE_PATH, at };

    expect((await forkStream(args)).created).toBe(true);
    expect((await forkStream(args)).created).toBe(false);
  });

  it("refuses a fork point past the source's tail", async () => {
    await expect(
      forkStream({
        url: server.streamUrl(FORK_PATH),
        sourcePath: SOURCE_PATH,
        at: { offset: "0000000000000003", subOffset: 5 },
      }),
    ).rejects.toThrow(StreamForkError);
  });

  it("reports an unknown source rather than creating an empty stream", async () => {
    await expect(
      forkStream({
        url: server.streamUrl(FORK_PATH),
        sourcePath: "agents/does-not-exist",
        at: { offset: "-1", subOffset: 0 },
      }),
    ).rejects.toThrow(/was not found/);
  });

  it("rejects a cursor that cannot name a message-level fork point", () => {
    // An Electric shape cursor addresses a whole batch, so branching on it would
    // silently round to a boundary the belief was never checkpointed at.
    expect(() => forkPointFor("10_0")).toThrow(/not a composite offset/);
  });
});
