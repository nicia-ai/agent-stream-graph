import {
  createStoreWithSchema,
  defineGraph,
  defineNode,
  type Store,
} from "@nicia-ai/typegraph";
import { createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";

import {
  ackSubscription,
  checkpointGraph,
  claimSubscription,
  consumeSubscribed,
  durableStreamSource,
  ensureSubscription,
  releaseSubscription,
  SubscriptionClaimedError,
  SubscriptionFencedError,
  type CheckpointBook,
  type Projector,
  typeGraphCheckpoints,
} from "../src";
import {
  startDurableStreamsServer,
  type DurableStreamsServer,
} from "./support/durable-streams-server";

const SUBSCRIPTION_ID = "materializer";
const STREAM_PATH = "agents/crm-agent";
const WORKER = "worker-1";

type Message = Readonly<{ key: string; label: string }>;

const Item = defineNode("Item", { schema: z.object({ label: z.string() }) });
const beliefGraph = defineGraph({
  id: "subscription_belief",
  nodes: { Item: { type: Item } },
  edges: {},
});
type BeliefStore = Store<typeof beliefGraph>;

const project: Projector<typeof beliefGraph, Message> = async (tx, change) => {
  await tx.nodes.Item.upsertById(change.key, { label: change.value.label });
};

async function labels(store: BeliefStore): Promise<Record<string, string>> {
  const rows = await store
    .query()
    .from("Item", "i")
    .select((context) => ({ id: context.i.id, label: context.i.label }))
    .execute();
  return Object.fromEntries(rows.map((row) => [row.id, row.label]));
}

describe("pull-wake subscriptions", () => {
  let server: DurableStreamsServer;
  let belief: BeliefStore;
  let book: CheckpointBook;

  const ref = (): { rootUrl: string; id: string } => ({ rootUrl: server.url, id: SUBSCRIPTION_ID });

  const sourceFor = (path: string) =>
    durableStreamSource<Message, Message>({
      url: server.streamUrl(path),
      name: path,
      toChange: (item) => ({ shape: "item", key: item.key, operation: "insert", value: item }),
    });

  beforeEach(async () => {
    server = await startDurableStreamsServer();
    server.createStream(STREAM_PATH);
    server.append(STREAM_PATH, [
      { key: "a", label: "a1" },
      { key: "b", label: "b1" },
    ]);
    [belief] = await createStoreWithSchema(beliefGraph, createLocalSqliteBackend().backend, {
      history: true,
      coalesceUnchangedUpserts: true,
    });
    const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
    book = typeGraphCheckpoints(cursor);
  });

  afterEach(async () => {
    await server.close();
  });

  it("creates a subscription, then re-confirms it idempotently", async () => {
    const first = await ensureSubscription({ ...ref(), streams: [STREAM_PATH] });
    const second = await ensureSubscription({ ...ref(), streams: [STREAM_PATH] });

    expect(first.created).toBe(true);
    expect(second.created).toBe(false);
  });

  it("refuses a subscription that would match no stream", async () => {
    await expect(ensureSubscription(ref())).rejects.toThrow(/at least one of/);
  });

  it("links every stream matching a pattern, including ones created later", async () => {
    server.createStream("agents/sales-bot");
    server.append("agents/sales-bot", [{ key: "s", label: "s1" }]);
    // Deliberately outside the glob: `*` matches ONE segment, so a nested path
    // must not link.
    server.createStream("agents/nested/deep-bot");
    server.append("agents/nested/deep-bot", [{ key: "d", label: "d1" }]);

    await ensureSubscription({ ...ref(), pattern: "agents/*" });

    // Created AFTER the subscription: a glob is matched live, so this links too.
    server.createStream("agents/ops-bot");
    server.append("agents/ops-bot", [{ key: "o", label: "o1" }]);

    const claim = await claimSubscription(ref(), WORKER);
    const paths = claim.streams.map((stream) => stream.path).sort();

    expect(paths).toEqual(["agents/crm-agent", "agents/ops-bot", "agents/sales-bot"]);
    await releaseSubscription(ref(), claim);
  });

  it("matches nested paths under a `**` pattern", async () => {
    server.createStream("agents/nested/deep-bot");
    server.append("agents/nested/deep-bot", [{ key: "d", label: "d1" }]);

    await ensureSubscription({ ...ref(), pattern: "agents/**" });

    const claim = await claimSubscription(ref(), WORKER);
    const paths = claim.streams.map((stream) => stream.path).sort();

    expect(paths).toEqual(["agents/crm-agent", "agents/nested/deep-bot"]);
    await releaseSubscription(ref(), claim);
  });

  it("drains a pattern-linked fleet without naming a single stream", async () => {
    server.createStream("agents/sales-bot");
    server.append("agents/sales-bot", [{ key: "s", label: "s1" }]);
    await ensureSubscription({ ...ref(), pattern: "agents/*" });

    const result = await consumeSubscribed({
      subscription: ref(),
      worker: WORKER,
      sourceFor: (stream) => sourceFor(stream.path),
      store: belief,
      checkpoints: book,
      project,
    });

    expect(result.streams.map((stream) => stream.path).sort()).toEqual(["agents/crm-agent", "agents/sales-bot"]);
    expect(await labels(belief)).toEqual({ a: "a1", b: "b1", s: "s1" });
  });

  it("reports which streams have pending work, and fences a second claimant", async () => {
    await ensureSubscription({ ...ref(), streams: [STREAM_PATH] });

    const claim = await claimSubscription(ref(), WORKER);

    expect(claim.streams).toHaveLength(1);
    expect(claim.streams[0]).toMatchObject({ path: STREAM_PATH, hasPending: true });
    await expect(claimSubscription(ref(), "worker-2")).rejects.toThrow(SubscriptionClaimedError);

    await releaseSubscription(ref(), claim);
    // Releasing hands the lease back immediately rather than waiting out the TTL.
    await expect(claimSubscription(ref(), "worker-2")).resolves.toBeDefined();
  });

  it("rejects an ack from a claim the server has moved past", async () => {
    await ensureSubscription({ ...ref(), streams: [STREAM_PATH] });
    const stale = await claimSubscription(ref(), WORKER);
    await releaseSubscription(ref(), stale);
    await claimSubscription(ref(), "worker-2");

    await expect(ackSubscription(ref(), stale, [], true)).rejects.toThrow(SubscriptionFencedError);
  });

  it("drains every pending stream and reports progress back to the server", async () => {
    await ensureSubscription({ ...ref(), streams: [STREAM_PATH] });

    const result = await consumeSubscribed({
      subscription: ref(),
      worker: WORKER,
      sourceFor: (stream) => sourceFor(stream.path),
      store: belief,
      checkpoints: book,
      project,
    });

    expect(result.streams).toEqual([{ path: STREAM_PATH, processed: 2, lastOffset: expect.any(String) }]);
    expect(result.nextWake).toBe(false);
    expect(await labels(belief)).toEqual({ a: "a1", b: "b1" });
    // The ack carries the whole-append base of our composite cursor: the server
    // only needs to know there is nothing left to wake us for.
    expect(server.ackedOffsetOf(SUBSCRIPTION_ID, STREAM_PATH)).toBe("0000000000000002");
  });

  it("still reports pending work when new messages arrive mid-claim", async () => {
    await ensureSubscription({ ...ref(), streams: [STREAM_PATH] });
    let appended = false;

    const result = await consumeSubscribed({
      subscription: ref(),
      worker: WORKER,
      sourceFor: (stream) => {
        // Land a new message after the claim's snapshot was taken but before the
        // ack, which is exactly when a naive "acked == tail" check goes stale.
        if (!appended) {
          server.append(STREAM_PATH, [{ key: "c", label: "c1" }]);
          appended = true;
        }
        return sourceFor(stream.path);
      },
      store: belief,
      checkpoints: book,
      project,
    });

    expect(result.nextWake).toBe(false);
    expect(await labels(belief)).toEqual({ a: "a1", b: "b1", c: "c1" });
  });

  it("hands the lease back when projection fails, instead of holding it", async () => {
    await ensureSubscription({ ...ref(), streams: [STREAM_PATH] });
    const failing: Projector<typeof beliefGraph, Message> = async () => {
      throw new Error("projector exploded");
    };

    await expect(
      consumeSubscribed({
        subscription: ref(),
        worker: WORKER,
        sourceFor: (stream) => sourceFor(stream.path),
        store: belief,
        checkpoints: book,
        project: failing,
      }),
    ).rejects.toThrow(/projector exploded/);

    // Another worker can pick the work up now, not in `leaseTtlMs`.
    await expect(claimSubscription(ref(), "worker-2")).resolves.toBeDefined();
    // ...and nothing was acked, so the server still knows the work is pending.
    expect(server.ackedOffsetOf(SUBSCRIPTION_ID, STREAM_PATH)).toBeUndefined();
  });
});
