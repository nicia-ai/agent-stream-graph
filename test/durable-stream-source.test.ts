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
  checkpointGraph,
  compareOffsets,
  composeOffset,
  consume,
  DurableStreamRetentionError,
  durableStreamSource,
  parseCompositeOffset,
  type CheckpointBook,
  type OffsetGranularity,
  type Projector,
  type ShapeSource,
  typeGraphCheckpoints,
} from "../src";
import {
  startDurableStreamsServer,
  type DurableStreamsServer,
} from "./support/durable-streams-server";

const STREAM_PATH = "agents/crm-agent";

type Message = Readonly<{ key: string; label: string }>;

const Item = defineNode("Item", { schema: z.object({ label: z.string() }) });
const beliefGraph = defineGraph({
  id: "durable_belief",
  nodes: { Item: { type: Item } },
  edges: {},
});
type BeliefStore = Store<typeof beliefGraph>;

const project: Projector<typeof beliefGraph, Message> = async (tx, change) => {
  await tx.nodes.Item.upsertById(change.key, { label: change.value.label });
};

async function labels(view: { query: BeliefStore["query"] }): Promise<Record<string, string>> {
  const rows = await view
    .query()
    .from("Item", "i")
    .select((context) => ({ id: context.i.id, label: context.i.label }))
    .execute();
  return Object.fromEntries(rows.map((row) => [row.id, row.label]));
}

describe("durableStreamSource", () => {
  let server: DurableStreamsServer;

  const sourceFor = (granularity?: OffsetGranularity): ShapeSource<Message> =>
    durableStreamSource<Message, Message>({
      url: server.streamUrl(STREAM_PATH),
      name: STREAM_PATH,
      ...(granularity === undefined ? {} : { granularity }),
      toChange: (item) => ({
        shape: "item",
        key: item.key,
        operation: "insert",
        value: item,
      }),
    });

  beforeEach(async () => {
    server = await startDurableStreamsServer();
    server.createStream(STREAM_PATH);
  });

  afterEach(async () => {
    await server.close();
  });

  it("gives every message its own resumable offset", async () => {
    server.append(STREAM_PATH, [
      { key: "a", label: "a1" },
      { key: "b", label: "b1" },
      { key: "c", label: "c1" },
    ]);

    const changes = await sourceFor().read(undefined);

    expect(changes.map((change) => change.key)).toEqual(["a", "b", "c"]);
    const offsets = changes.map((change) => change.offset);
    expect(new Set(offsets).size).toBe(3);
    // Strictly increasing, which is what makes each one independently
    // checkpointable — `consume` never has to defer past a shared offset.
    expect(compareOffsets(offsets[0]!, offsets[1]!)).toBe(-1);
    expect(compareOffsets(offsets[1]!, offsets[2]!)).toBe(-1);
    // The last message of an append is relabelled onto that append's end offset,
    // so the sub-offset resets at every boundary instead of growing forever.
    expect(parseCompositeOffset(offsets[2]!)?.consumed).toBe(0);
  });

  it("resumes from a mid-append cursor without re-delivering or skipping", async () => {
    server.append(STREAM_PATH, [
      { key: "a", label: "a1" },
      { key: "b", label: "b1" },
      { key: "c", label: "c1" },
    ]);
    const source = sourceFor();
    const all = await source.read(undefined);

    const resumed = await source.read(all[0]!.offset);

    expect(resumed.map((change) => change.key)).toEqual(["b", "c"]);
    expect(resumed.map((change) => change.offset)).toEqual([all[1]!.offset, all[2]!.offset]);
  });

  it("resumes correctly even when the server re-chunks the same messages", async () => {
    // The sub-offset counts messages from the offset the read was ISSUED at, not
    // from a batch boundary, so how the server splits its responses on the second
    // read cannot change which messages survive the skip. A count anchored to
    // batch boundaries would silently drop or duplicate here.
    server.append(STREAM_PATH, [
      { key: "a", label: "a1" },
      { key: "b", label: "b1" },
    ]);
    server.append(STREAM_PATH, [
      { key: "c", label: "c1" },
      { key: "d", label: "d1" },
    ]);
    const source = sourceFor();
    const all = await source.read(undefined);
    expect(all.map((change) => change.key)).toEqual(["a", "b", "c", "d"]);

    server.coalesceChunks(STREAM_PATH);
    const resumed = await source.read(all[0]!.offset);

    expect(resumed.map((change) => change.key)).toEqual(["b", "c", "d"]);
  });

  it("shares one offset across an append at append granularity", async () => {
    server.append(STREAM_PATH, [
      { key: "a", label: "a1" },
      { key: "b", label: "b1" },
    ]);
    server.append(STREAM_PATH, [{ key: "c", label: "c1" }]);

    const changes = await sourceFor("append").read(undefined);

    expect(changes[0]!.offset).toBe(changes[1]!.offset);
    expect(compareOffsets(changes[1]!.offset, changes[2]!.offset)).toBe(-1);
    // Resuming from the shared offset skips the whole append, never half of it.
    const resumed = await sourceFor("append").read(changes[0]!.offset);
    expect(resumed.map((change) => change.key)).toEqual(["c"]);
  });

  it("reports a retention drop as a typed, actionable error", async () => {
    server.append(STREAM_PATH, [
      { key: "a", label: "a1" },
      { key: "b", label: "b1" },
    ]);
    server.append(STREAM_PATH, [
      { key: "c", label: "c1" },
      { key: "d", label: "d1" },
    ]);
    const source = sourceFor();
    const all = await source.read(undefined);
    // Retention now covers the position `b`'s cursor points at.
    server.retainFrom(STREAM_PATH, 3);

    await expect(source.read(all[1]!.offset)).rejects.toThrow(DurableStreamRetentionError);
    // Re-materializing from the start is the documented recovery, and it works
    // because the start sentinel means "everything still retained" — it is what
    // survives compaction, so the error's advice is actionable rather than a
    // dead end.
    const recovered = await source.read(undefined);
    expect(recovered.map((change) => change.key)).toEqual(["d"]);
  });

  it("rejects a cursor that did not come from this source", async () => {
    server.append(STREAM_PATH, [{ key: "a", label: "a1" }]);

    await expect(sourceFor().read("10_0")).rejects.toThrow(/not a composite offset/);
  });

  it("wraps a throwing toChange with the offset it failed on", async () => {
    server.append(STREAM_PATH, [{ key: "a", label: "a1" }]);
    const source = durableStreamSource<Message, Message>({
      url: server.streamUrl(STREAM_PATH),
      name: STREAM_PATH,
      toChange: () => {
        throw new Error("unmapped shape");
      },
    });

    await expect(source.read(undefined)).rejects.toThrow(/unmapped shape/);
  });

  it("anchors each message to its own belief, so any single change is replayable", async () => {
    const [belief] = await createStoreWithSchema(beliefGraph, createLocalSqliteBackend().backend, {
      history: true,
      coalesceUnchangedUpserts: true,
    });
    const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
    const book: CheckpointBook = typeGraphCheckpoints(cursor);
    server.append(STREAM_PATH, [
      { key: "a", label: "a1" },
      { key: "b", label: "b1" },
      { key: "c", label: "c1" },
    ]);
    const source = sourceFor();
    const changes = await source.read(undefined);

    const result = await consume({ source, store: belief, checkpoints: book, project });

    expect(result.processed).toBe(3);
    expect(await labels(belief)).toEqual({ a: "a1", b: "b1", c: "c1" });
    // The middle change is addressable on its own — the whole point of a
    // per-message offset. Under an Electric shape all three would collapse onto
    // one batch offset and only the final belief would be reachable.
    const afterB = await book.anchorFor(STREAM_PATH, changes[1]!.offset);
    expect(afterB).toBeDefined();
    expect(await labels(belief.asOfRecorded(afterB!))).toEqual({ a: "a1", b: "b1" });
  });

  it("keeps composite offsets ordered past a single digit", () => {
    // Lexicographic comparison would put ",10" before ",2"; the sub-offset is
    // compared numerically for exactly this reason.
    expect(compareOffsets(composeOffset("0000000000000001", 2), composeOffset("0000000000000001", 10))).toBe(-1);
    expect(compareOffsets(composeOffset("0000000000000001", 9), composeOffset("0000000000000002", 0))).toBe(-1);
    expect(compareOffsets(composeOffset("-1", 1), composeOffset("0000000000000001", 0))).toBe(-1);
  });

  it("refuses to compare a composite offset against a plain one", () => {
    // A stream that switched offset formats mid-flight has no coherent order;
    // silently picking one would corrupt resumption.
    expect(() => compareOffsets(composeOffset("0000000000000001", 0), "10_0")).toThrow(/incompatible offset formats/);
  });
});
