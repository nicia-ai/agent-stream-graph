import {
  createStoreWithSchema,
  defineGraph,
  defineNode,
} from "@nicia-ai/typegraph";
import { createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";

import {
  checkpointGraph,
  consume,
  durableStateSource,
  StateResetError,
  type CheckpointBook,
  type Projector,
  type StateEvent,
  typeGraphCheckpoints,
} from "../src";
import {
  startDurableStreamsServer,
  type DurableStreamsServer,
} from "./support/durable-streams-server";

const STREAM_PATH = "agents/crm-agent";

type Row = Readonly<{ label: string }>;

/** Build one State Protocol change record. */
const change = (
  type: string,
  key: string,
  operation: "insert" | "update" | "delete" | "upsert",
  label: string,
  txid?: string,
): StateEvent<Row> => ({
  type,
  key,
  value: { label },
  headers: { operation, ...(txid === undefined ? {} : { txid }) },
});

const control = (kind: "snapshot-start" | "snapshot-end" | "reset"): StateEvent<Row> => ({
  headers: { control: kind },
});

const Item = defineNode("Item", { schema: z.object({ label: z.string() }) });
const beliefGraph = defineGraph({
  id: "state_belief",
  nodes: { Item: { type: Item } },
  edges: {},
});
const project: Projector<typeof beliefGraph, Row> = async (tx, change_) => {
  if (change_.operation === "delete") return;
  await tx.nodes.Item.upsertById(change_.key, { label: change_.value.label });
};

describe("durableStateSource", () => {
  let server: DurableStreamsServer;

  const source = (options?: { types?: readonly string[]; granularity?: "message" | "transaction" }) =>
    durableStateSource<Row>({
      url: server.streamUrl(STREAM_PATH),
      name: STREAM_PATH,
      ...(options?.types === undefined ? {} : { types: options.types }),
      ...(options?.granularity === undefined ? {} : { granularity: options.granularity }),
    });

  beforeEach(async () => {
    server = await startDurableStreamsServer();
    server.createStream(STREAM_PATH);
  });

  afterEach(async () => {
    await server.close();
  });

  it("renames a State Protocol record onto a ShapeChange", async () => {
    server.append(STREAM_PATH, [change("item", "a", "insert", "a1")]);

    const [first] = await source().read(undefined);

    expect(first).toMatchObject({ shape: "item", key: "a", operation: "insert", value: { label: "a1" } });
    expect(first!.offset).toEqual(expect.any(String));
  });

  it("collapses `upsert` to `update`, which is all a projector can act on", async () => {
    // The protocol's fourth operation has no counterpart in `Operation` because
    // an idempotent projector cannot distinguish insert from update anyway.
    server.append(STREAM_PATH, [change("item", "a", "upsert", "a1")]);

    const [first] = await source().read(undefined);

    expect(first!.operation).toBe("update");
  });

  it("drops snapshot frames and keeps the changes around them", async () => {
    server.append(STREAM_PATH, [
      control("snapshot-start"),
      change("item", "a", "insert", "a1"),
      change("item", "b", "insert", "b1"),
      control("snapshot-end"),
    ]);

    const changes = await source().read(undefined);

    expect(changes.map((c) => c.key)).toEqual(["a", "b"]);
  });

  it("rejects a reset frame with actionable recovery", async () => {
    server.append(STREAM_PATH, [change("item", "a", "insert", "a1"), control("reset")]);

    await expect(source().read(undefined)).rejects.toThrow(StateResetError);
  });

  it("filters to the requested record types", async () => {
    server.append(STREAM_PATH, [
      change("item", "a", "insert", "a1"),
      change("note", "n", "insert", "n1"),
      change("item", "b", "insert", "b1"),
    ]);

    const changes = await source({ types: ["item"] }).read(undefined);

    expect(changes.map((c) => c.key)).toEqual(["a", "b"]);
  });

  it("gives a source transaction one offset, so it commits atomically", async () => {
    // Two writes in txid t1, one in t2. At transaction granularity `consume`
    // must open exactly two transactions — the source's own commits — rather
    // than one per message or one per transport chunk.
    server.append(STREAM_PATH, [
      change("item", "a", "insert", "a1", "t1"),
      change("item", "b", "insert", "b1", "t1"),
    ]);
    server.append(STREAM_PATH, [change("item", "c", "insert", "c1", "t2")]);

    const changes = await source({ granularity: "transaction" }).read(undefined);

    expect(changes).toHaveLength(3);
    expect(changes[0]!.offset).toBe(changes[1]!.offset);
    expect(changes[2]!.offset).not.toBe(changes[0]!.offset);
  });

  it("groups a transaction that was split across transport chunks", async () => {
    // The transaction's writes land in two separate appends. Grouping happens
    // after the whole catch-up is collected, so chunking cannot tear it.
    server.append(STREAM_PATH, [change("item", "a", "insert", "a1", "t1")]);
    server.append(STREAM_PATH, [change("item", "b", "insert", "b1", "t1")]);

    const changes = await source({ granularity: "transaction" }).read(undefined);

    expect(changes[0]!.offset).toBe(changes[1]!.offset);
  });

  it("falls back to per-message offsets for records carrying no txid", async () => {
    server.append(STREAM_PATH, [
      change("item", "a", "insert", "a1"),
      change("item", "b", "insert", "b1"),
    ]);

    const changes = await source({ granularity: "transaction" }).read(undefined);

    expect(changes[0]!.offset).not.toBe(changes[1]!.offset);
  });

  it("materializes a transaction under a single recorded anchor", async () => {
    const [belief] = await createStoreWithSchema(beliefGraph, createLocalSqliteBackend().backend, {
      history: true,
      coalesceUnchangedUpserts: true,
    });
    const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
    const book: CheckpointBook = typeGraphCheckpoints(cursor);
    server.append(STREAM_PATH, [
      change("item", "a", "insert", "a1", "t1"),
      change("item", "b", "insert", "b1", "t1"),
    ]);
    const stateSource = source({ granularity: "transaction" });
    const changes = await stateSource.read(undefined);

    await consume({ source: stateSource, store: belief, checkpoints: book, project });

    // Both rows share the transaction's anchor: the belief as of that offset
    // contains the whole source transaction, never half of it.
    const anchor = await book.anchorFor(STREAM_PATH, changes[0]!.offset);
    expect(anchor).toBeDefined();
    const rows = await belief
      .asOfRecorded(anchor!)
      .query()
      .from("Item", "i")
      .select((context) => ({ id: context.i.id }))
      .execute();
    expect(rows.map((r) => r.id).sort()).toEqual(["a", "b"]);
  });
});
