import {
  asNodeId,
  asRecordedInstant,
  createStoreWithSchema,
  defineGraph,
  defineNode,
  type RecordedInstant,
  type Store,
} from "@nicia-ai/typegraph";
import { createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";

const electricMock = vi.hoisted(() => ({
  abortSignals: [] as AbortSignal[],
  unsubscribeCount: 0,
  // Constructor options each ShapeStream was built with — to assert the adapter
  // threads `offset`/`handle` on resume.
  constructorOptions: [] as Array<{
    offset: string | undefined;
    handle: string | undefined;
  }>,
  // The `lastOffset` the stream reports on `up-to-date` (override for sentinels).
  lastOffset: "10_0" as string,
  // The server-assigned handle the stream exposes (override or clear per test).
  shapeHandle: "shape-h1" as string | undefined,
  // Override the messages the mock ShapeStream emits. `undefined` = default
  // (one change + up-to-date). Set to a custom batch for must-refetch/timeout.
  batch: undefined as readonly unknown[] | undefined,
  // When set, the stream delivers this through subscribe's `onError` argument
  // instead of emitting messages — exercises the transport-error reject path.
  error: undefined as unknown,
}));

vi.mock("@electric-sql/client", () => ({
  ShapeStream: class {
    constructor(options: {
      signal?: AbortSignal;
      offset?: string;
      handle?: string;
    }) {
      if (options.signal !== undefined)
        electricMock.abortSignals.push(options.signal);
      electricMock.constructorOptions.push({
        offset: options.offset,
        handle: options.handle,
      });
    }

    get lastOffset(): string {
      return electricMock.lastOffset;
    }

    get shapeHandle(): string | undefined {
      return electricMock.shapeHandle;
    }

    subscribe(
      onMessages: (messages: readonly unknown[]) => void,
      onError?: (error: unknown) => void,
    ): () => void {
      if (electricMock.error !== undefined) {
        queueMicrotask(() => onError?.(electricMock.error));
        return () => {
          electricMock.unsubscribeCount += 1;
        };
      }
      const messages = electricMock.batch ?? [
        {
          key: "row-1",
          value: { label: "from-electric" },
          headers: { operation: "insert" },
        },
        { headers: { control: "up-to-date" } },
      ];
      queueMicrotask(() => onMessages(messages));
      return () => {
        electricMock.unsubscribeCount += 1;
      };
    }
  },
  isChangeMessage: (message: unknown) =>
    typeof message === "object" && message !== null && "key" in message,
}));

import {
  checkpointGraph,
  consume,
  electricShapeSource,
  mockShapeSource,
  typeGraphCheckpoints,
  ElectricControlError,
  ElectricMustRefetchError,
  ProjectorRecordedNothingError,
  type CheckpointBook,
  type Projector,
  type ShapeChange,
  type ShapeSource,
} from "../src";

// A recorded anchor is `r1:<16-digit logical revision>:<canonical UTC
// timestamp>`, and only the revision orders commits. Pinning the wall-time
// component makes a test anchor's identity its revision alone.
const ANCHOR_WALL_TIME = "2026-01-01T00:00:00.000Z";

/**
 * A distinct valid anchor per revision, for tests that drive the checkpoint book
 * directly and so have no store to allocate one. `CheckpointBook.record` accepts
 * only a `RecordedInstant`, so an ad-hoc label like `"anchor-001"` cannot stand
 * in for one — the brand rejects it, which is the footgun it exists to prevent.
 *
 * Hand-assembled because TypeGraph publishes no anchor factory: `recordedNow()`
 * and a transaction receipt are the only real sources, and both need a store.
 */
const instant = (revision: number): RecordedInstant =>
  asRecordedInstant(
    `r1:${String(revision).padStart(16, "0")}:${ANCHOR_WALL_TIME}`,
  );

function _portableCheckpointBookOmitsTransactionAdoption(
  book: CheckpointBook,
): void {
  // @ts-expect-error Exactly-once adoption is available only from typeGraphAdoptingCheckpoints.
  void book.recordIn;
}

const Item = defineNode("Item", { schema: z.object({ label: z.string() }) });
const beliefGraph = defineGraph({
  id: "test_belief",
  nodes: { Item: { type: Item } },
  edges: {},
});
type BeliefStore = Store<typeof beliefGraph>;

const project: Projector<typeof beliefGraph> = async (store, change) => {
  if (change.operation === "delete") {
    await store.nodes.Item.delete(asNodeId(change.key));
    return;
  }
  await store.nodes.Item.upsertById(change.key, {
    label: change.value.label as string,
  });
};

const CHANGES: readonly ShapeChange[] = [
  {
    offset: "001",
    shape: "item",
    key: "a",
    operation: "insert",
    value: { label: "a1" },
  },
  {
    offset: "002",
    shape: "item",
    key: "b",
    operation: "insert",
    value: { label: "b1" },
  },
  {
    offset: "003",
    shape: "item",
    key: "a",
    operation: "update",
    value: { label: "a2" },
  },
  {
    offset: "004",
    shape: "item",
    key: "c",
    operation: "insert",
    value: { label: "c1" },
  },
];

async function labels(view: {
  query: BeliefStore["query"];
}): Promise<Record<string, string>> {
  const rows = await view
    .query()
    .from("Item", "i")
    .select((context) => ({ id: context.i.id, label: context.i.label }))
    .execute();
  return Object.fromEntries(rows.map((row) => [row.id, row.label]));
}

describe("consume", () => {
  let belief: BeliefStore;
  let book: CheckpointBook;
  let source: ShapeSource;

  async function setup(history: boolean): Promise<void> {
    [belief] = await createStoreWithSchema(
      beliefGraph,
      createLocalSqliteBackend().backend,
      { history },
    );
    const [cursor] = await createStoreWithSchema(
      checkpointGraph,
      createLocalSqliteBackend().backend,
    );
    book = typeGraphCheckpoints(cursor);
    source = mockShapeSource("test", CHANGES);
  }

  beforeEach(async () => {
    await setup(true);
  });

  it("resumes from the durable checkpoint after a crash", async () => {
    const first = await consume({
      source,
      store: belief,
      checkpoints: book,
      project,
      stopAfter: 2,
    });
    expect(first.processed).toBe(2);
    expect(await book.lastOffset("test")).toBe("002");

    const second = await consume({
      source,
      store: belief,
      checkpoints: book,
      project,
    });
    expect(second.processed).toBe(2);
    expect(second.fromOffset).toBe("002");
    expect(await book.lastOffset("test")).toBe("004");
    expect(await labels(belief)).toEqual({ a: "a2", b: "b1", c: "c1" });
  });

  it("does not advance the cursor past an unfinished shared-offset batch (Electric-style)", async () => {
    // Every change in an Electric catch-up batch carries the one batch offset.
    // A crash mid-batch must NOT durably advance to that offset — read(after)
    // returns only changes STRICTLY after it, so the batch's remaining changes
    // would be skipped forever. The cursor advances only at the batch boundary.
    const sharedOffset = mockShapeSource("test", [
      {
        offset: "10_0",
        shape: "item",
        key: "a",
        operation: "insert",
        value: { label: "a1" },
      },
      {
        offset: "10_0",
        shape: "item",
        key: "b",
        operation: "insert",
        value: { label: "b1" },
      },
      {
        offset: "10_0",
        shape: "item",
        key: "c",
        operation: "insert",
        value: { label: "c1" },
      },
    ]);

    const crashed = await consume({
      source: sharedOffset,
      store: belief,
      checkpoints: book,
      project,
      stopAfter: 2,
    });
    expect(crashed.processed).toBe(2);
    // Nothing checkpointed: the shared offset is not yet a boundary.
    expect(await book.lastOffset("test")).toBeUndefined();

    const resumed = await consume({
      source: sharedOffset,
      store: belief,
      checkpoints: book,
      project,
    });
    // The whole batch re-delivered; "c" is not silently dropped.
    expect(resumed.processed).toBe(3);
    expect(await book.lastOffset("test")).toBe("10_0");
    expect(await labels(belief)).toEqual({ a: "a1", b: "b1", c: "c1" });
  });

  // Counts the transactions `consume` opens, which is the whole point of
  // batching: each COMMIT burns one recorded revision, so "one transaction per
  // offset run" is the property under test, not an implementation detail.
  function countingStore(store: BeliefStore): {
    store: BeliefStore;
    transactions: () => number;
  } {
    let count = 0;
    // A Proxy, not a spread: the store's methods live on its prototype, so
    // spreading yields an object that typechecks and is empty at runtime.
    // Methods are bound to the target because the store reads private class
    // fields, which a Proxy `this` cannot see.
    const wrapped = new Proxy(store, {
      get: (target, property) => {
        if (property === "transactionWithReceipt") {
          return ((fn, options) => {
            count += 1;
            return target.transactionWithReceipt(fn, options);
          }) satisfies BeliefStore["transactionWithReceipt"];
        }
        const value = Reflect.get(target, property, target) as unknown;
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    return { store: wrapped, transactions: () => count };
  }

  const sharedOffsetChanges = (
    keys: readonly string[],
    offset = "10_0",
  ): readonly ShapeChange[] =>
    keys.map((key) => ({
      offset,
      shape: "item",
      key,
      operation: "insert" as const,
      value: { label: `${key}1` },
    }));

  it("applies a whole offset run in ONE transaction", async () => {
    // Three changes share one resume offset, so only that offset is ever
    // addressable by anchorFor — committing per change would burn three
    // recorded instants to produce one reachable anchor.
    const counting = countingStore(belief);
    const result = await consume({
      source: mockShapeSource("test", sharedOffsetChanges(["a", "b", "c"])),
      store: counting.store,
      checkpoints: book,
      project,
    });

    expect(counting.transactions()).toBe(1);
    expect(result.processed).toBe(3);
    expect(await labels(belief)).toEqual({ a: "a1", b: "b1", c: "c1" });
    // The batch's single anchor reconstructs the belief the offset committed.
    const anchor = await book.anchorFor("test", "10_0");
    expect(await labels(belief.asOfRecorded(anchor!))).toEqual({
      a: "a1",
      b: "b1",
      c: "c1",
    });
  });

  it("still opens one transaction per change when every change has its own offset", async () => {
    // The batching is keyed on the offset, not on batch arrival: distinct
    // offsets stay independently checkpointable and must not be coalesced.
    const counting = countingStore(belief);
    await consume({
      source: mockShapeSource("test", CHANGES),
      store: counting.store,
      checkpoints: book,
      project,
    });
    expect(counting.transactions()).toBe(CHANGES.length);
    expect(await book.lastOffset("test")).toBe("004");
  });

  it("detects a change dropped among its neighbours and rolls back the batch", async () => {
    // The batch receipt aggregates, so "b" dropping among two writers still
    // totals 2 writes. Only the per-change `tx.measure` scope can see it.
    const skipsB: Projector<typeof beliefGraph> = async (target, change) => {
      if (change.key === "b") return;
      await target.nodes.Item.upsertById(change.key, {
        label: change.value.label as string,
      });
    };

    await expect(
      consume({
        source: mockShapeSource("test", sharedOffsetChanges(["a", "b", "c"])),
        store: belief,
        checkpoints: book,
        project: skipsB,
      }),
    ).rejects.toThrow(
      expect.objectContaining({
        name: "ProjectorRecordedNothingError",
        key: "b",
      }) as Error,
    );

    // The throw happened inside the transaction, so "a" rolled back with it —
    // a half-applied run must never be left behind an unadvanced cursor.
    expect(await labels(belief)).toEqual({});
    expect(await book.lastOffset("test")).toBeUndefined();
  });

  it("splits an oversized run into several transactions without checkpointing mid-run", async () => {
    // maxBatchSize bounds the write-lock hold and the capture buffer; it is NOT
    // a checkpoint boundary, so the cursor still moves only once, at the end of
    // the run. Otherwise a crash would resume past the run's unapplied tail.
    const counting = countingStore(belief);
    const result = await consume({
      source: mockShapeSource("test", sharedOffsetChanges(["a", "b", "c", "d", "e"])),
      store: counting.store,
      checkpoints: book,
      project,
      maxBatchSize: 2,
    });

    expect(counting.transactions()).toBe(3); // 2 + 2 + 1
    expect(result.processed).toBe(5);
    expect(await labels(belief)).toEqual({
      a: "a1",
      b: "b1",
      c: "c1",
      d: "d1",
      e: "e1",
    });
    // One checkpoint for the run, despite three commits.
    expect(await book.lastOffset("test")).toBe("10_0");
  });

  // Each of these fails SILENTLY without validation rather than loudly: a
  // non-positive or non-finite bound makes every offset run yield an empty
  // batch, so the cursor never advances and consume spins forever; a fractional
  // bound admits ceil(n) changes per transaction, exceeding the stated cap.
  it.each([
    ["zero", 0],
    ["negative", -1],
    ["NaN", Number.NaN],
    ["Infinity", Number.POSITIVE_INFINITY],
    ["fractional", 2.5],
  ])("rejects a %s maxBatchSize before reading any change", async (_label, maxBatchSize) => {
    const read = vi.fn();
    await expect(
      consume({
        source: { name: "test", read },
        store: belief,
        checkpoints: book,
        project,
        maxBatchSize,
      }),
    ).rejects.toThrow(
      expect.objectContaining({
        name: "InvalidMaxBatchSizeError",
        maxBatchSize,
      }) as Error,
    );
    // Rejected up front, so no source read and no partial application.
    expect(read).not.toHaveBeenCalled();
    expect(await labels(belief)).toEqual({});
  });

  it("accepts a maxBatchSize of 1, committing each change separately", async () => {
    // The boundary value: valid, and degenerates to the old per-change
    // behaviour without collapsing the single checkpoint at the run's end.
    const counting = countingStore(belief);
    const result = await consume({
      source: mockShapeSource("test", sharedOffsetChanges(["a", "b", "c"])),
      store: counting.store,
      checkpoints: book,
      project,
      maxBatchSize: 1,
    });
    expect(counting.transactions()).toBe(3);
    expect(result.processed).toBe(3);
    expect(await book.lastOffset("test")).toBe("10_0");
  });

  it("throws when an insert/update projector issues only an empty bulk write", async () => {
    // `bulkCreate([])` resolves having persisted nothing; counting it as a write
    // would silently drop the insert. The observer must treat it as no write.
    const emptyBulk: Projector<typeof beliefGraph> = async (target) => {
      await target.nodes.Item.bulkCreate([]);
    };
    const source = mockShapeSource("test", [
      {
        offset: "001",
        shape: "item",
        key: "a",
        operation: "insert",
        value: { label: "a1" },
      },
    ]);
    await expect(
      consume({ source, store: belief, checkpoints: book, project: emptyBulk }),
    ).rejects.toBeInstanceOf(ProjectorRecordedNothingError);
    expect(await book.lastOffset("test")).toBeUndefined();
  });

  it("is idempotent on at-least-once re-delivery", async () => {
    await consume({ source, store: belief, checkpoints: book, project });
    const replay = await consume({
      source,
      store: belief,
      checkpoints: book,
      project,
    });
    expect(replay.processed).toBe(0);
    expect(await labels(belief)).toEqual({ a: "a2", b: "b1", c: "c1" });
  });

  it("replays the belief at a past offset via the recorded anchor", async () => {
    await consume({ source, store: belief, checkpoints: book, project });

    // `anchorFor` hands back a branded RecordedInstant — no cast at the call site.
    const at2 = await book.anchorFor("test", "002");
    expect(at2).toBeDefined();
    expect(await labels(belief.asOfRecorded(at2!))).toEqual({
      a: "a1",
      b: "b1",
    });

    const at4 = await book.anchorFor("test", "004");
    expect(await labels(belief.asOfRecorded(at4!))).toEqual({
      a: "a2",
      b: "b1",
      c: "c1",
    });
  });

  it("anchors an offset to its own transaction's instant, not a concurrent writer's", async () => {
    // The receipt's `recorded` is the instant THIS transaction allocated.
    // `store.recordedNow()` is the graph-global high-water — advanced by ANY
    // writer — so reading it after the commit hands back whatever a concurrent
    // writer landed in the window between our commit and our read, anchoring the
    // offset to a belief this stream never produced.
    //
    // Model that window exactly: an interloper commits into the same belief
    // store the instant the consumer's transaction resolves, before it can
    // checkpoint.
    const interloping = new Proxy(belief, {
      get(target, prop) {
        if (prop === "transactionWithReceipt") {
          return async (
            fn: Parameters<BeliefStore["transactionWithReceipt"]>[0],
          ) => {
            const outcome = await target.transactionWithReceipt(fn);
            await target.nodes.Item.upsertById("interloper", {
              label: "not from this stream",
            });
            return outcome;
          };
        }
        // Bind methods to the real store: `Store` keeps private fields, which a
        // proxy `this` cannot reach.
        const member = Reflect.get(target, prop);
        return typeof member === "function"
          ? (member as (...args: unknown[]) => unknown).bind(target)
          : member;
      },
    });

    const source = mockShapeSource("test", [
      {
        offset: "001",
        shape: "item",
        key: "a",
        operation: "insert",
        value: { label: "a1" },
      },
    ]);
    await consume({ source, store: interloping, checkpoints: book, project });

    // The offset anchors to the belief THIS stream produced at 001.
    const at1 = await book.anchorFor("test", "001");
    expect(await labels(belief.asOfRecorded(at1!))).toEqual({ a: "a1" });

    // ...and it is genuinely a different instant from the graph-global clock,
    // which by now includes the interloper. Anchoring on `recordedNow()` would
    // have checkpointed THIS belief against offset 001.
    const global = await belief.recordedNow();
    expect(global).not.toBe(at1);
    expect(await labels(belief.asOfRecorded(global!))).toEqual({
      a: "a1",
      interloper: "not from this stream",
    });
  });

  it("rejects a belief store without { history: true }", async () => {
    await setup(false);
    await expect(
      consume({ source, store: belief, checkpoints: book, project }),
    ).rejects.toThrow(/history/i);
  });

  it("orders non-padded Electric-style numeric offsets", async () => {
    await book.record("test", "9_0", instant(9));
    await book.record("test", "10_0", instant(10));
    expect(await book.lastOffset("test")).toBe("10_0");

    const nonPadded = mockShapeSource("test", [
      {
        offset: "10_0",
        shape: "item",
        key: "ten",
        operation: "insert",
        value: { label: "ten" },
      },
      {
        offset: "9_0",
        shape: "item",
        key: "nine",
        operation: "insert",
        value: { label: "nine" },
      },
    ]);
    expect(
      (await nonPadded.read(undefined)).map((change) => change.offset),
    ).toEqual(["9_0", "10_0"]);
    expect(
      (await nonPadded.read("9_0")).map((change) => change.offset),
    ).toEqual(["10_0"]);
  });

  it("keeps checkpoint rows distinct when stream and offset contain spaces", async () => {
    await book.record("a b", "c", instant(1));
    await book.record("a", "b c", instant(2));

    expect(await book.anchorFor("a b", "c")).toBe(instant(1));
    expect(await book.anchorFor("a", "b c")).toBe(instant(2));
  });

  it("checkpoints the prior anchor for a delete that records nothing", async () => {
    // Offset 001 inserts "a"; offset 002 deletes a non-existent key. The delete
    // method resolves (the projector is seen to have written), but the recorded
    // clock does not advance, so 002 checkpoints the same anchor as 001.
    const noopSource = mockShapeSource("test", [
      {
        offset: "001",
        shape: "item",
        key: "a",
        operation: "insert",
        value: { label: "a1" },
      },
      {
        offset: "002",
        shape: "item",
        key: "ghost",
        operation: "delete",
        value: {},
      },
    ]);
    await expect(
      consume({
        source: noopSource,
        store: belief,
        checkpoints: book,
        project,
      }),
    ).resolves.toMatchObject({
      processed: 2,
      lastOffset: "002",
    });
    expect(await book.lastOffset("test")).toBe("002");
    // anchorFor 002 reconstructs the same belief as 001 (nothing changed).
    const at2 = await book.anchorFor("test", "002");
    const at1 = await book.anchorFor("test", "001");
    expect(at2).toBe(at1);
    expect(await labels(belief)).toEqual({ a: "a1" });
  });

  it("skips the checkpoint when the first change is a no-op on an empty graph", async () => {
    // No prior writes, so the recorded high-water is undefined and there is no
    // anchor to checkpoint. The consumer must not throw; it skips the checkpoint
    // so re-delivery re-processes.
    const emptyNoop = mockShapeSource("test", [
      {
        offset: "001",
        shape: "item",
        key: "ghost",
        operation: "delete",
        value: {},
      },
    ]);
    await expect(
      consume({ source: emptyNoop, store: belief, checkpoints: book, project }),
    ).resolves.toMatchObject({
      processed: 1,
    });
    expect(await book.lastOffset("test")).toBeUndefined();
  });

  it("throws when an insert/update change records no write (dropped change)", async () => {
    // A projector that handles no shape — the change it is given writes nothing.
    // Silently advancing the cursor past it would lose the change forever, so
    // the consumer fails loud instead.
    const handlesNothing: Projector<typeof beliefGraph> = async () => {};
    const source = mockShapeSource("test", [
      {
        offset: "001",
        shape: "item",
        key: "a",
        operation: "insert",
        value: { label: "a1" },
      },
    ]);
    await expect(
      consume({
        source,
        store: belief,
        checkpoints: book,
        project: handlesNothing,
      }),
    ).rejects.toBeInstanceOf(ProjectorRecordedNothingError);
    // The cursor never advanced past the dropped change.
    expect(await book.lastOffset("test")).toBeUndefined();
  });

  it("throws when the projector writes to a different store than the handle", async () => {
    // The classic data-loss bug: the projector ignores the handle it is given
    // and writes to some other store. The watched belief gets nothing.
    const [other] = await createStoreWithSchema(
      beliefGraph,
      createLocalSqliteBackend().backend,
      { history: true },
    );
    const wrongStore: Projector<typeof beliefGraph> = async (
      _target,
      change,
    ) => {
      await other.nodes.Item.upsertById(change.key, {
        label: change.value.label as string,
      });
    };
    const source = mockShapeSource("test", [
      {
        offset: "001",
        shape: "item",
        key: "a",
        operation: "insert",
        value: { label: "a1" },
      },
    ]);
    await expect(
      consume({
        source,
        store: belief,
        checkpoints: book,
        project: wrongStore,
      }),
    ).rejects.toBeInstanceOf(ProjectorRecordedNothingError);
    expect(await labels(belief)).toEqual({});
    expect(await book.lastOffset("test")).toBeUndefined();
  });

  it("throws when the projector swallows a failing write", async () => {
    // A write that rejects (invalid value) caught by the projector leaves the
    // handle un-written; the consumer must still treat the change as dropped.
    const swallow: Projector<typeof beliefGraph> = async (target, change) => {
      try {
        await target.nodes.Item.upsertById(change.key, {
          label: 123 as unknown as string,
        });
      } catch {
        // swallowed — exactly the bug we want to catch
      }
    };
    const source = mockShapeSource("test", [
      {
        offset: "001",
        shape: "item",
        key: "a",
        operation: "insert",
        value: { label: "a1" },
      },
    ]);
    await expect(
      consume({ source, store: belief, checkpoints: book, project: swallow }),
    ).rejects.toBeInstanceOf(ProjectorRecordedNothingError);
    expect(await book.lastOffset("test")).toBeUndefined();
  });

  it("carries the anchor forward for a delete the projector skips", async () => {
    // A projector that writes inserts but ignores deletes. The skipped delete
    // records nothing (wrote === false) but is tolerated as a no-op; its offset
    // checkpoints the prior anchor so replay-by-offset still works.
    const insertsOnly: Projector<typeof beliefGraph> = async (
      target,
      change,
    ) => {
      if (change.operation === "delete") return;
      await target.nodes.Item.upsertById(change.key, {
        label: change.value.label as string,
      });
    };
    const source = mockShapeSource("test", [
      {
        offset: "001",
        shape: "item",
        key: "a",
        operation: "insert",
        value: { label: "a1" },
      },
      {
        offset: "002",
        shape: "item",
        key: "a",
        operation: "delete",
        value: {},
      },
    ]);
    const result = await consume({
      source,
      store: belief,
      checkpoints: book,
      project: insertsOnly,
    });
    expect(result.processed).toBe(2);
    expect(await book.lastOffset("test")).toBe("002");
    const at1 = await book.anchorFor("test", "001");
    const at2 = await book.anchorFor("test", "002");
    expect(at2).toBe(at1);
    expect(await labels(belief)).toEqual({ a: "a1" });
  });

  it("rolls back and propagates when the projector throws after a write", async () => {
    // Each change applies inside one transaction, so a mid-projection throw
    // leaves no partial write behind and the cursor unmoved.
    const boom = new Error("projector boom");
    const throwsAfterWrite: Projector<typeof beliefGraph> = async (
      target,
      change,
    ) => {
      await target.nodes.Item.upsertById(change.key, {
        label: change.value.label as string,
      });
      throw boom;
    };
    const source = mockShapeSource("test", [
      {
        offset: "001",
        shape: "item",
        key: "a",
        operation: "insert",
        value: { label: "a1" },
      },
    ]);
    await expect(
      consume({
        source,
        store: belief,
        checkpoints: book,
        project: throwsAfterWrite,
      }),
    ).rejects.toBe(boom);
    expect(await labels(belief)).toEqual({});
    expect(await book.lastOffset("test")).toBeUndefined();
  });

  it("lastOffset is an O(1) stream read independent of checkpoint row count", async () => {
    // Record many offsets; lastOffset must still return the high-water without
    // scanning every Checkpoint row.
    for (let i = 1; i <= 50; i += 1) {
      await book.record("test", String(i).padStart(3, "0"), instant(i));
    }
    expect(await book.lastOffset("test")).toBe("050");
    // Per-offset anchors are still retrievable.
    expect(await book.anchorFor("test", "001")).toBe(instant(1));
    expect(await book.anchorFor("test", "050")).toBe(instant(50));
  });
});

describe("consume with coalesceUnchangedUpserts (replay churn)", () => {
  // A full replay re-applies every already-applied change. Without coalescing
  // each identical upsert rewrites the row and advances the recorded clock;
  // with it, an identical re-delivery is a true no-op. We force a replay by
  // handing the second consume a FRESH checkpoint book, so it re-reads from the
  // start and re-applies everything.
  async function beliefWith(
    coalesceUnchangedUpserts: boolean,
  ): Promise<BeliefStore> {
    const [store] = await createStoreWithSchema(
      beliefGraph,
      createLocalSqliteBackend().backend,
      {
        history: true,
        coalesceUnchangedUpserts,
      },
    );
    return store;
  }
  const freshBook = async (): Promise<CheckpointBook> => {
    const [cursor] = await createStoreWithSchema(
      checkpointGraph,
      createLocalSqliteBackend().backend,
    );
    return typeGraphCheckpoints(cursor);
  };

  // A stream whose rows never supersede each other (each key inserted once). A
  // full replay-from-zero then re-applies each row's CURRENT value, so every
  // re-apply is value-identical and coalesces. (A stream with an in-place update
  // — like the module `CHANGES` fixture — is different: replaying the earlier
  // value over the current later one is a real backward change and correctly
  // still writes. Coalescing removes re-DELIVERY churn, not the cost of a replay
  // that faithfully re-walks superseded intermediate states.)
  const DISTINCT: readonly ShapeChange[] = [
    {
      offset: "001",
      shape: "item",
      key: "a",
      operation: "insert",
      value: { label: "a1" },
    },
    {
      offset: "002",
      shape: "item",
      key: "b",
      operation: "insert",
      value: { label: "b1" },
    },
    {
      offset: "003",
      shape: "item",
      key: "c",
      operation: "insert",
      value: { label: "c1" },
    },
  ];

  it("does not churn recorded history when a no-supersession batch is replayed", async () => {
    const belief = await beliefWith(true);
    const source = mockShapeSource("test", DISTINCT);

    await consume({
      source,
      store: belief,
      checkpoints: await freshBook(),
      project,
    });
    const afterFirst = await belief.recordedNow();

    // Replay the whole batch through a fresh cursor: every change re-delivered,
    // each value-identical to the current row.
    const replay = await consume({
      source,
      store: belief,
      checkpoints: await freshBook(),
      project,
    });
    expect(replay.processed).toBe(DISTINCT.length);
    // Coalesced: the identical re-applies advanced the clock by nothing.
    expect(await belief.recordedNow()).toBe(afterFirst);
    expect(await labels(belief)).toEqual({ a: "a1", b: "b1", c: "c1" });
  });

  it("without coalescing, the same replay does churn the clock (contrast)", async () => {
    const belief = await beliefWith(false);
    const source = mockShapeSource("test", DISTINCT);

    await consume({
      source,
      store: belief,
      checkpoints: await freshBook(),
      project,
    });
    const afterFirst = await belief.recordedNow();

    await consume({
      source,
      store: belief,
      checkpoints: await freshBook(),
      project,
    });
    // Each identical upsert rewrote its row, so the clock advanced.
    expect(await belief.recordedNow()).not.toBe(afterFirst);
  });

  it("coalesces a crash-window re-delivery of an already-applied change", async () => {
    // The realistic at-least-once case: a change was projected but its offset
    // was not durably checkpointed before the crash. On resume it is re-read and
    // re-applied with a byte-identical value — which coalesces, so the replay
    // adds no history row.
    const belief = await beliefWith(true);
    const book = await freshBook();
    const source = mockShapeSource("test", DISTINCT);

    // Apply 001+002, checkpoint only through 002 (as if the crash lost 003's
    // checkpoint after its projection).
    await consume({
      source,
      store: belief,
      checkpoints: book,
      project,
      stopAfter: 2,
    });
    const third = (await source.read(await book.lastOffset("test")))[0]!;
    await belief.transaction((tx) => project(tx, third)); // 003 projected, not checkpointed
    const beforeResume = await belief.recordedNow();

    // Resume: 003 is re-delivered and re-applied — identical, so it coalesces.
    const resumed = await consume({
      source,
      store: belief,
      checkpoints: book,
      project,
    });
    expect(resumed.processed).toBe(1);
    expect(await belief.recordedNow()).toBe(beforeResume);
    expect(await labels(belief)).toEqual({ a: "a1", b: "b1", c: "c1" });
  });

  it("still detects a dropped change with coalescing on", async () => {
    // Coalescing must not mask a projector that writes nothing: a coalesced
    // upsert still counts one write intent, so `writes.total === 0` still means
    // dropped.
    const belief = await beliefWith(true);
    const handlesNothing: Projector<typeof beliefGraph> = async () => {};
    const source = mockShapeSource("test", [
      {
        offset: "001",
        shape: "item",
        key: "a",
        operation: "insert",
        value: { label: "a1" },
      },
    ]);
    await expect(
      consume({
        source,
        store: belief,
        checkpoints: await freshBook(),
        project: handlesNothing,
      }),
    ).rejects.toBeInstanceOf(ProjectorRecordedNothingError);
  });
});

describe("compareOffsets (mixed formats)", () => {
  it("throws when numeric-tuple and non-numeric offsets are mixed", () => {
    // The sort inside mockShapeSource calls compareOffsets, so the error fires
    // at construction — fail loud before any read can produce a partial order.
    expect(() =>
      mockShapeSource("test", [
        {
          offset: "10_0",
          shape: "item",
          key: "a",
          operation: "insert",
          value: {},
        },
        {
          offset: "live-wiki-001",
          shape: "item",
          key: "b",
          operation: "insert",
          value: {},
        },
      ]),
    ).toThrow(/incompatible offset formats/);
  });
});

describe("electricShapeSource", () => {
  beforeEach(() => {
    electricMock.abortSignals.length = 0;
    electricMock.unsubscribeCount = 0;
    electricMock.constructorOptions.length = 0;
    electricMock.lastOffset = "10_0";
    electricMock.shapeHandle = "shape-h1";
    electricMock.batch = undefined;
    electricMock.error = undefined;
  });

  it("uses ShapeStream.lastOffset and tags the whole batch with it", async () => {
    const source = electricShapeSource({
      name: "electric",
      url: "http://localhost:3000/v1/shape",
      params: { table: "items" },
      toChange: (message) => ({
        shape: "item",
        key: message.key,
        operation: message.headers.operation,
        value: message.value,
      }),
    });

    const changes = await source.read("9_0");

    expect(changes).toEqual([
      {
        offset: "10_0",
        shape: "item",
        key: "row-1",
        operation: "insert",
        value: { label: "from-electric" },
      },
    ]);
    expect(electricMock.unsubscribeCount).toBe(1);
    expect(electricMock.abortSignals).toHaveLength(1);
    expect(electricMock.abortSignals[0]?.aborted).toBe(true);
  });

  it("rejects with ElectricMustRefetchError on a must-refetch control message", async () => {
    electricMock.batch = [{ headers: { control: "must-refetch" } }];
    const source = electricShapeSource({
      name: "electric",
      url: "http://localhost:3000/v1/shape",
      params: { table: "items" },
      timeoutMs: 1000,
      toChange: () => ({
        shape: "item",
        key: "x",
        operation: "insert",
        value: {},
      }),
    });
    await expect(source.read(undefined)).rejects.toBeInstanceOf(
      ElectricMustRefetchError,
    );
  });

  it("rejects with ElectricControlError on an error control message", async () => {
    electricMock.batch = [
      { headers: { control: "error", detail: "shape exploded" } },
    ];
    const source = electricShapeSource({
      name: "electric",
      url: "http://localhost:3000/v1/shape",
      params: { table: "items" },
      timeoutMs: 1000,
      toChange: () => ({
        shape: "item",
        key: "x",
        operation: "insert",
        value: {},
      }),
    });
    await expect(source.read(undefined)).rejects.toBeInstanceOf(
      ElectricControlError,
    );
  });

  it("times out when up-to-date never arrives", async () => {
    electricMock.batch = [];
    const source = electricShapeSource({
      name: "electric",
      url: "http://localhost:3000/v1/shape",
      params: { table: "items" },
      timeoutMs: 50,
      toChange: () => ({
        shape: "item",
        key: "x",
        operation: "insert",
        value: {},
      }),
    });
    await expect(source.read(undefined)).rejects.toThrow(/up-to-date.*50ms/);
    expect(electricMock.abortSignals[0]?.aborted).toBe(true);
  });

  it("captures the shape handle and threads it (with the offset) into the next read", async () => {
    electricMock.shapeHandle = "shape-h7";
    const handles: string[] = [];
    const source = electricShapeSource({
      name: "electric",
      url: "http://localhost:3000/v1/shape",
      params: { table: "items" },
      onHandle: (handle) => handles.push(handle),
      toChange: (message) => ({
        shape: "item",
        key: message.key,
        operation: message.headers.operation,
        value: message.value,
      }),
    });

    // First read starts from the beginning: no offset, no handle yet.
    await source.read(undefined);
    expect(electricMock.constructorOptions[0]).toEqual({
      offset: undefined,
      handle: undefined,
    });
    expect(handles).toEqual(["shape-h7"]);

    // Second read resumes: Electric requires BOTH the offset and the captured handle.
    await source.read("10_0");
    expect(electricMock.constructorOptions[1]).toEqual({
      offset: "10_0",
      handle: "shape-h7",
    });
  });

  it("falls back to a full re-fetch when resuming without a handle (cold start)", async () => {
    electricMock.shapeHandle = undefined; // server handle never captured/persisted
    const source = electricShapeSource({
      name: "electric",
      url: "http://localhost:3000/v1/shape",
      params: { table: "items" },
      toChange: (message) => ({
        shape: "item",
        key: message.key,
        operation: message.headers.operation,
        value: message.value,
      }),
    });

    // A real offset but no handle: rather than letting Electric throw
    // MissingShapeHandleError, the adapter omits both and re-fetches from the start.
    await source.read("10_0");
    expect(electricMock.constructorOptions[0]).toEqual({
      offset: undefined,
      handle: undefined,
    });
  });

  it("rejects with the transport error delivered through subscribe's onError", async () => {
    const boom = new Error("connection reset");
    electricMock.error = boom;
    const source = electricShapeSource({
      name: "electric",
      url: "http://localhost:3000/v1/shape",
      params: { table: "items" },
      timeoutMs: 1000,
      toChange: () => ({
        shape: "item",
        key: "x",
        operation: "insert",
        value: {},
      }),
    });
    await expect(source.read(undefined)).rejects.toBe(boom);
    expect(electricMock.abortSignals[0]?.aborted).toBe(true);
  });

  it("rejects (does not hang) when toChange throws, surfacing the real cause", async () => {
    const source = electricShapeSource({
      name: "electric",
      url: "http://localhost:3000/v1/shape",
      params: { table: "items" },
      timeoutMs: 1000,
      toChange: () => {
        throw new Error("bad message shape");
      },
    });
    await expect(source.read(undefined)).rejects.toThrow(
      /toChange.*bad message shape/,
    );
    expect(electricMock.abortSignals[0]?.aborted).toBe(true);
  });

  it("rejects a change message with a precise reason (not a misleading operation error)", async () => {
    // Valid operation, but the value is absent — the error must say so, not blame `operation`.
    electricMock.batch = [
      { key: "row-1", headers: { operation: "delete" } },
      { headers: { control: "up-to-date" } },
    ];
    const source = electricShapeSource({
      name: "electric",
      url: "http://localhost:3000/v1/shape",
      params: { table: "items" },
      timeoutMs: 1000,
      toChange: (message) => ({
        shape: "item",
        key: message.key,
        operation: message.headers.operation,
        value: message.value,
      }),
    });
    await expect(source.read(undefined)).rejects.toThrow(/missing `value`/);
  });

  it("rejects a sentinel lastOffset that carries changes (non-resumable)", async () => {
    electricMock.lastOffset = "-1";
    const source = electricShapeSource({
      name: "electric",
      url: "http://localhost:3000/v1/shape",
      params: { table: "items" },
      timeoutMs: 1000,
      toChange: (message) => ({
        shape: "item",
        key: message.key,
        operation: message.headers.operation,
        value: message.value,
      }),
    });
    await expect(source.read(undefined)).rejects.toThrow(
      /non-resumable sentinel/,
    );
  });
});
