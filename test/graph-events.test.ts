import {
  CardinalityError,
  createStoreWithSchema,
  defineEdge,
  defineGraph,
  defineNode,
  getEdgeKinds,
  getNodeKinds,
  type Store,
} from "@nicia-ai/typegraph";
import { createLocalSqliteBackend } from "@nicia-ai/typegraph/adapters/drizzle/sqlite/local";
import { beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";

import {
  applyGraphEvents,
  checkpointGraph,
  consume,
  type Decoder,
  graphEmitter,
  type GraphEvent,
  graphProjector,
  mockShapeSource,
  OP_NODE_UPSERT,
  type ShapeChange,
  typeGraphCheckpoints,
  type ValidTime,
} from "../src";

const Person = defineNode("Person", {
  schema: z.object({ name: z.string(), email: z.string(), title: z.string() }),
});
const Company = defineNode("Company", {
  schema: z.object({ name: z.string(), domain: z.string(), stage: z.string().default("unknown") }),
});
const worksAt = defineEdge("worksAt", { schema: z.object({}) });
const rates = defineEdge("rates", { schema: z.object({ score: z.number() }) });

const intelGraph = defineGraph({
  id: "graph_events_intel",
  nodes: { Person: { type: Person }, Company: { type: Company } },
  edges: {
    worksAt: { type: worksAt, from: [Person], to: [Company] },
    rates: { type: rates, from: [Person], to: [Company] },
  },
});
type IntelStore = Store<typeof intelGraph>;

type Row = Readonly<{
  name?: string;
  email?: string;
  title?: string;
  domain?: string;
  person?: string;
  company?: string;
}>;

const g = graphEmitter(intelGraph);

const decode: Decoder<typeof intelGraph, Row> = (change, emit) => {
  switch (change.shape) {
    case "person": {
      if (change.operation === "delete") return [emit.nodes.Person.remove(change.key)];
      const { name = "", email = "", title = "" } = change.value;
      return [emit.nodes.Person.upsert(change.key, { name, email, title })];
    }
    case "company": {
      if (change.operation === "delete") return [emit.nodes.Company.remove(change.key)];
      const { name = "", domain = "" } = change.value;
      // `stage` omitted: z.input honours the schema default.
      return [emit.nodes.Company.upsert(change.key, { name, domain })];
    }
    case "employment": {
      const from = { kind: "Person", id: change.value.person ?? "" } as const;
      const to = { kind: "Company", id: change.value.company ?? "" } as const;
      // props omitted entirely — worksAt's schema is empty.
      return change.operation === "delete"
        ? [emit.edges.worksAt.remove(from, to)]
        : [emit.edges.worksAt.upsert(from, to)];
    }
    default:
      return [];
  }
};

const change = (
  offset: string,
  shape: string,
  key: string,
  value: Row,
  operation: "insert" | "update" | "delete" = "insert",
): ShapeChange<Row> => ({ offset, shape, key, operation, value });

async function newStore(): Promise<IntelStore> {
  const [store] = await createStoreWithSchema(intelGraph, createLocalSqliteBackend().backend, {
    history: true,
    coalesceUnchangedUpserts: true,
  });
  return store;
}

describe("graph events", () => {
  describe("typing", () => {
    it("rejects every way a decoder can name the graph wrongly", () => {
      // These are the mistakes the hand-rolled projectors could make silently.
      // @ts-expect-error — misspelled kind; the emitter suggests 'Person'.
      void (() => g.nodes.Persson.upsert("p1", { name: "n", email: "e", title: "t" }));
      // @ts-expect-error — missing a required prop.
      void (() => g.nodes.Person.upsert("p1", { name: "n", email: "e" }));
      // @ts-expect-error — another kind's props.
      void (() => g.nodes.Company.upsert("c1", { name: "n", email: "e", title: "t" }));
      // @ts-expect-error — endpoints swapped; worksAt runs Person -> Company.
      void (() => g.edges.worksAt.upsert({ kind: "Company", id: "c" }, { kind: "Person", id: "p" }));
      // @ts-expect-error — a kind that is not a declared endpoint.
      void (() => g.edges.worksAt.upsert({ kind: "Robot", id: "r" }, { kind: "Company", id: "c" }));
      // @ts-expect-error — `rates` has a non-empty schema, so props are required.
      void (() => g.edges.rates.upsert({ kind: "Person", id: "p" }, { kind: "Company", id: "c" }));
      expect(true).toBe(true);
    });

    it("rejects a hand-written event pairing one kind with another's props", () => {
      // The distributive conditional is what makes this fail. A plain generic
      // alias degrades to the cross product here and accepts it.
      const bad = {
        op: OP_NODE_UPSERT,
        kind: "Person",
        id: "p1",
        props: { name: "n", domain: "d" },
      };
      // @ts-expect-error — Person does not take Company's props.
      const typed: GraphEvent<typeof intelGraph> = bad;
      expect(typed).toBeDefined();
    });

    it("narrows two levels deep with no default case", () => {
      const describe_ = (event: GraphEvent<typeof intelGraph>): string => {
        if (event.op === OP_NODE_UPSERT) {
          switch (event.kind) {
            case "Person":
              return event.props.title;
            case "Company":
              return event.props.domain;
          }
        }
        return event.op;
      };
      expect(describe_(g.nodes.Person.upsert("p", { name: "n", email: "e", title: "VP" }))).toBe("VP");
    });
  });

  describe("emitter", () => {
    it("covers every kind the graph declares", () => {
      // Guards the one assertion in the module: the factory fills the mapped
      // type from a runtime loop, which the compiler cannot check.
      const emitter = graphEmitter(intelGraph) as unknown as {
        nodes: Record<string, unknown>;
        edges: Record<string, unknown>;
      };
      expect(Object.keys(emitter.nodes).sort()).toEqual([...getNodeKinds(intelGraph)].sort());
      expect(Object.keys(emitter.edges).sort()).toEqual([...getEdgeKinds(intelGraph)].sort());
    });

    it("emits plain data that survives a JSON round trip", () => {
      const events = decode(change("1", "person", "p1", { name: "A", email: "a@x", title: "VP" }), g);
      expect(JSON.parse(JSON.stringify(events))).toEqual(events);
    });

    it("is pure — decoding twice gives equal events and touches nothing", () => {
      const input = change("1", "person", "p1", { name: "A", email: "a@x", title: "VP" });
      expect(decode(input, g)).toEqual(decode(input, g));
    });
  });

  describe("applyGraphEvents", () => {
    let belief: IntelStore;

    beforeEach(async () => {
      belief = await newStore();
    });

    const run = async (changes: readonly ShapeChange<Row>[]): Promise<void> => {
      const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
      await consume({
        source: mockShapeSource("intel", changes),
        store: belief,
        checkpoints: typeGraphCheckpoints(cursor),
        project: graphProjector(intelGraph, decode),
      });
    };

    it("materializes nodes and edges from one decoded batch", async () => {
      await run([
        change("1", "person", "p1", { name: "Jane", email: "j@acme.com", title: "VP" }),
        change("2", "company", "c1", { name: "Acme", domain: "acme.com" }),
        change("3", "employment", "e1", { person: "p1", company: "c1" }),
      ]);

      const people = await belief
        .query()
        .from("Person", "p")
        .select((context) => ({ id: context.p.id, title: context.p.title }))
        .execute();
      expect(people).toEqual([{ id: "p1", title: "VP" }]);
      const links = await belief.edges.worksAt.findByEndpoints(
        { kind: "Person", id: "p1" },
        { kind: "Company", id: "c1" },
      );
      expect(links).toBeDefined();
    });

    it("applies a defaulted prop the decoder omitted", async () => {
      await run([change("1", "company", "c1", { name: "Acme", domain: "acme.com" })]);

      const rows = await belief
        .query()
        .from("Company", "c")
        .select((context) => ({ stage: context.c.stage }))
        .execute();
      expect(rows).toEqual([{ stage: "unknown" }]);
    });

    it("creates a node before the edge that references it, whatever the emit order", async () => {
      // The decoder emits the edge FIRST; APPLY_ORDER has to fix it, otherwise
      // endpoint validation fails. This is the rule every hand-rolled projector
      // had to remember for itself.
      const edgeFirst: Decoder<typeof intelGraph, Row> = (_change, emit) => [
        emit.edges.worksAt.upsert({ kind: "Person", id: "p9" }, { kind: "Company", id: "c9" }),
        emit.nodes.Company.upsert("c9", { name: "Zeta", domain: "zeta.io" }),
        emit.nodes.Person.upsert("p9", { name: "Zed", email: "z@zeta.io", title: "Eng" }),
      ];
      const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);

      await consume({
        source: mockShapeSource("intel", [change("1", "any", "x", {})]),
        store: belief,
        checkpoints: typeGraphCheckpoints(cursor),
        project: graphProjector(intelGraph, edgeFirst),
      });

      const link = await belief.edges.worksAt.findByEndpoints(
        { kind: "Person", id: "p9" },
        { kind: "Company", id: "c9" },
      );
      expect(link).toBeDefined();
    });

    it("removes an edge before the node it hangs off", async () => {
      await run([
        change("1", "person", "p1", { name: "Jane", email: "j@acme.com", title: "VP" }),
        change("2", "company", "c1", { name: "Acme", domain: "acme.com" }),
        change("3", "employment", "e1", { person: "p1", company: "c1" }),
      ]);

      const removeBoth: Decoder<typeof intelGraph, Row> = (_change, emit) => [
        emit.nodes.Person.remove("p1"),
        emit.edges.worksAt.remove({ kind: "Person", id: "p1" }, { kind: "Company", id: "c1" }),
      ];
      const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
      await consume({
        source: mockShapeSource("intel", [change("9", "any", "x", {})]),
        store: belief,
        checkpoints: typeGraphCheckpoints(cursor),
        project: graphProjector(intelGraph, removeBoth),
      });

      expect(
        await belief.edges.worksAt.findByEndpoints({ kind: "Person", id: "p1" }, { kind: "Company", id: "c1" }),
      ).toBeUndefined();
      expect(await belief.nodes.Person.getById(asId("p1"))).toBeUndefined();
    });

    it("treats a re-delivered batch as a no-op", async () => {
      const changes = [
        change("1", "person", "p1", { name: "Jane", email: "j@acme.com", title: "VP" }),
        change("2", "company", "c1", { name: "Acme", domain: "acme.com" }),
        change("3", "employment", "e1", { person: "p1", company: "c1" }),
      ];
      await run(changes);
      await run(changes);

      const people = await belief.query().from("Person", "p").select((c) => ({ id: c.p.id })).execute();
      expect(people).toHaveLength(1);
    });

    it("carries valid time from the event onto the node", async () => {
      const timed: Decoder<typeof intelGraph, Row> = (c, emit) => [
        emit.nodes.Person.upsert(c.key, { name: "Jane", email: "j@x", title: "VP" }, {
          validFrom: "2019-03-01T00:00:00.000Z",
        }),
      ];
      const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
      await consume({
        source: mockShapeSource("intel", [change("1", "person", "p1", {})]),
        store: belief,
        checkpoints: typeGraphCheckpoints(cursor),
        project: graphProjector(intelGraph, timed),
      });

      // The window opens in March 2019, so an asOf read before it sees nothing
      // and one after it sees the row — proof the event's valid time reached the
      // store rather than defaulting to ingest time.
      const before = await belief.nodes.Person.find({}, { temporalMode: "asOf", asOf: "2019-01-01T00:00:00.000Z" });
      const after = await belief.nodes.Person.find({}, { temporalMode: "asOf", asOf: "2020-01-01T00:00:00.000Z" });
      expect(before).toHaveLength(0);
      expect(after.map((row) => row.id)).toEqual(["p1"]);
    });

    it("carries valid time from the event onto the edge", async () => {
      // `getOrCreateByEndpoints` is the replay-safe edge write, and it carries
      // the window: the event's valid time reaches the relationship, not just
      // the nodes it connects.
      const timed: Decoder<typeof intelGraph, Row> = (_c, emit) => [
        emit.nodes.Person.upsert("p1", { name: "J", email: "j@x", title: "VP" }),
        emit.nodes.Company.upsert("c1", { name: "Acme", domain: "acme.com" }),
        emit.edges.worksAt.upsert({ kind: "Person", id: "p1" }, { kind: "Company", id: "c1" }, undefined, {
          validFrom: "2019-03-01T00:00:00.000Z",
        }),
      ];
      const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
      await consume({
        source: mockShapeSource("intel", [change("1", "any", "x", {})]),
        store: belief,
        checkpoints: typeGraphCheckpoints(cursor),
        project: graphProjector(intelGraph, timed),
      });

      const endpoints = [{ kind: "Person", id: "p1" }, { kind: "Company", id: "c1" }] as const;
      const before = await belief.edges.worksAt.findByEndpoints(endpoints[0], endpoints[1], undefined, {
        temporalMode: "asOf",
        asOf: "2019-01-01T00:00:00.000Z",
      });
      const after = await belief.edges.worksAt.findByEndpoints(endpoints[0], endpoints[1], undefined, {
        temporalMode: "asOf",
        asOf: "2020-01-01T00:00:00.000Z",
      });
      expect(before).toBeUndefined();
      expect(after).toBeDefined();
    });

    it("re-delivering an edge event does not move the window it opened", async () => {
      // The store's asymmetry, surfaced deliberately: `validFrom` applies only
      // on create, so a replayed event cannot rewrite when the relationship
      // started. `validTo` does apply on the update branch, which is what lets a
      // later event close the window idempotently.
      const opened: Decoder<typeof intelGraph, Row> = (_c, emit) => [
        emit.nodes.Person.upsert("p1", { name: "J", email: "j@x", title: "VP" }),
        emit.nodes.Company.upsert("c1", { name: "Acme", domain: "acme.com" }),
        emit.edges.worksAt.upsert({ kind: "Person", id: "p1" }, { kind: "Company", id: "c1" }, undefined, {
          validFrom: "2019-03-01T00:00:00.000Z",
        }),
      ];
      const replay = async (decoder: Decoder<typeof intelGraph, Row>, offset: string): Promise<void> => {
        const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
        await consume({
          source: mockShapeSource("intel", [change(offset, "any", "x", {})]),
          store: belief,
          checkpoints: typeGraphCheckpoints(cursor),
          project: graphProjector(intelGraph, decoder),
        });
      };

      await replay(opened, "1");
      // Same relationship, a different start — re-delivery must not move it.
      await replay(
        (_c, emit) => [
          emit.edges.worksAt.upsert({ kind: "Person", id: "p1" }, { kind: "Company", id: "c1" }, undefined, {
            validFrom: "2021-01-01T00:00:00.000Z",
          }),
        ],
        "2",
      );

      const between = await belief.edges.worksAt.findByEndpoints(
        { kind: "Person", id: "p1" },
        { kind: "Company", id: "c1" },
        undefined,
        { temporalMode: "asOf", asOf: "2020-01-01T00:00:00.000Z" },
      );
      expect(between).toBeDefined();
    });

    it("re-delivering a node event does not move the window it opened", async () => {
      // A `validFrom` naming an instant other than the one a live row holds is
      // refused by default (`IMMUTABLE_VALIDITY_LOWER_BOUND`); the node path
      // writes under `onImmutableLowerBound: "preserve"` instead. A stream
      // carrying event time re-states the start of a row it already created on
      // every redelivery, so the strict verdict would abort the batch, leave
      // the cursor unadvanced, and re-deliver the same change forever.
      const born = async (validFrom: string, title: string, offset: string): Promise<void> => {
        const timed: Decoder<typeof intelGraph, Row> = (_c, emit) => [
          emit.nodes.Person.upsert("p1", { name: "J", email: "j@x", title }, { validFrom }),
        ];
        const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
        await consume({
          source: mockShapeSource("intel", [change(offset, "any", "x", {})]),
          store: belief,
          checkpoints: typeGraphCheckpoints(cursor),
          project: graphProjector(intelGraph, timed),
        });
      };

      await born("2019-03-01T00:00:00.000Z", "VP", "1");
      await born("2021-01-01T00:00:00.000Z", "SVP", "2");

      // The later start was dropped, not applied and not thrown: the row is
      // still visible at a coordinate between the two instants...
      const between = await belief.nodes.Person.find(
        {},
        { temporalMode: "asOf", asOf: "2020-01-01T00:00:00.000Z" },
      );
      expect(between.map((row) => row.id)).toEqual(["p1"]);
      // ...and the props the second event carried still landed.
      expect((await belief.nodes.Person.getById(asId("p1")))?.title).toBe("SVP");
    });

    it("materializes a fact that arrives already historical", async () => {
      // One change that both creates the row and ends it in the past. The store
      // stamps no lower bound rather than the ingest instant, so the row reads
      // back at every coordinate before its end — where an ingest-time start
      // would have made it readable at none.
      const historical: Decoder<typeof intelGraph, Row> = (_c, emit) => [
        emit.nodes.Person.upsert("p1", { name: "J", email: "j@x", title: "VP" }, {
          validTo: "2021-06-01T00:00:00.000Z",
        }),
      ];
      const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
      await consume({
        source: mockShapeSource("intel", [change("1", "any", "x", {})]),
        store: belief,
        checkpoints: typeGraphCheckpoints(cursor),
        project: graphProjector(intelGraph, historical),
      });

      const during = await belief.nodes.Person.find({}, { temporalMode: "asOf", asOf: "2020-01-01T00:00:00.000Z" });
      expect(during.map((row) => row.id)).toEqual(["p1"]);
      const after = await belief.nodes.Person.find({}, { temporalMode: "asOf", asOf: "2022-01-01T00:00:00.000Z" });
      expect(after).toHaveLength(0);
      // "ended at T, start unknown" — the lower bound reads back absent.
      const stored = await belief.nodes.Person.getById(asId("p1"), { temporalMode: "includeEnded" });
      expect(stored?.meta?.validFrom).toBeUndefined();
    });

    it("still refuses a window that ends before it starts", async () => {
      // Preserving the lower bound is not the same as ignoring valid time: a
      // stated bound is validated on every write, and a window of negative
      // width describes a row observable at no coordinate at all. The refusal
      // has to stay loud, because the fix is to emit `validFrom` on the
      // creating event rather than to let the row through.
      const inverted: Decoder<typeof intelGraph, Row> = (_c, emit) => [
        emit.nodes.Person.upsert("p1", { name: "J", email: "j@x", title: "VP" }, {
          validFrom: "2021-01-01T00:00:00.000Z",
          validTo: "2019-01-01T00:00:00.000Z",
        }),
      ];
      const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);

      await expect(
        consume({
          source: mockShapeSource("intel", [change("1", "any", "x", {})]),
          store: belief,
          checkpoints: typeGraphCheckpoints(cursor),
          project: graphProjector(intelGraph, inverted),
        }),
      ).rejects.toThrow(/[Ii]nverted validity window/);
      // The batch rolled back, so the cursor never advanced past the bad change.
      expect(await belief.nodes.Person.getById(asId("p1"))).toBeUndefined();
    });

    it("converges an existing edge's props", async () => {
      // `getOrCreateByEndpoints` defaults to `ifExists: "return"`, which hands
      // back the existing edge and writes NOTHING — so without an explicit
      // update an `edge.upsert` event could never revise a relationship.
      const rate = (score: number): Decoder<typeof intelGraph, Row> => (_c, emit) => [
        emit.nodes.Person.upsert("p1", { name: "J", email: "j@x", title: "VP" }),
        emit.nodes.Company.upsert("c1", { name: "Acme", domain: "acme.com" }),
        emit.edges.rates.upsert({ kind: "Person", id: "p1" }, { kind: "Company", id: "c1" }, { score }),
      ];
      const apply = async (score: number, offset: string): Promise<void> => {
        const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
        await consume({
          source: mockShapeSource("intel", [change(offset, "any", "x", {})]),
          store: belief,
          checkpoints: typeGraphCheckpoints(cursor),
          project: graphProjector(intelGraph, rate(score)),
        });
      };

      await apply(1, "1");
      await apply(5, "2");

      const rated = await belief.edges.rates.findByEndpoints(
        { kind: "Person", id: "p1" },
        { kind: "Company", id: "c1" },
      );
      expect(rated?.score).toBe(5);
    });

    it("closes an existing relationship's window, idempotently", async () => {
      const endpoints = [
        { kind: "Person", id: "p1" },
        { kind: "Company", id: "c1" },
      ] as const;
      const relate = (valid: { validFrom?: string; validTo?: string }): Decoder<typeof intelGraph, Row> =>
        (_c, emit) => [
          emit.nodes.Person.upsert("p1", { name: "J", email: "j@x", title: "VP" }),
          emit.nodes.Company.upsert("c1", { name: "Acme", domain: "acme.com" }),
          emit.edges.worksAt.upsert(endpoints[0], endpoints[1], undefined, valid),
        ];
      const apply = async (valid: { validFrom?: string; validTo?: string }, offset: string): Promise<void> => {
        const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
        await consume({
          source: mockShapeSource("intel", [change(offset, "any", "x", {})]),
          store: belief,
          checkpoints: typeGraphCheckpoints(cursor),
          project: graphProjector(intelGraph, relate(valid)),
        });
      };

      await apply({ validFrom: "2019-03-01T00:00:00.000Z" }, "1");
      const opened = await belief.edges.worksAt.findByEndpoints(endpoints[0], endpoints[1]);
      expect(opened).toBeDefined();

      await apply({ validFrom: "2019-03-01T00:00:00.000Z", validTo: "2021-06-01T00:00:00.000Z" }, "2");
      // Ended, so no longer current...
      expect(await belief.edges.worksAt.findByEndpoints(endpoints[0], endpoints[1])).toBeUndefined();
      // ...but still true inside the window it held.
      expect(
        await belief.edges.worksAt.findByEndpoints(endpoints[0], endpoints[1], undefined, {
          temporalMode: "asOf",
          asOf: "2020-01-01T00:00:00.000Z",
        }),
      ).toBeDefined();

      // Re-delivering the close matches the SAME (ended) row rather than
      // minting a parallel edge — the match is by endpoints, not by id.
      await apply({ validFrom: "2019-03-01T00:00:00.000Z", validTo: "2021-06-01T00:00:00.000Z" }, "3");
      const still = await belief.edges.worksAt.findByEndpoints(endpoints[0], endpoints[1], undefined, {
        temporalMode: "asOf",
        asOf: "2020-01-01T00:00:00.000Z",
      });
      expect(still?.id).toBe(opened?.id);
    });

    it("replays a batch of nodes AND edges without churning history", async () => {
      // The README's churn-free promise has to cover relationships too, not
      // just entities: an endpoint-matched edge write coalesces only when the
      // store finds nothing changed, so a full replay must leave the recorded
      // clock exactly where it was.
      const changes = [
        change("1", "person", "p1", { name: "Jane", email: "j@acme.com", title: "VP" }),
        change("2", "company", "c1", { name: "Acme", domain: "acme.com" }),
        change("3", "employment", "e1", { person: "p1", company: "c1" }),
      ];
      await run(changes);
      const settled = await belief.recordedNow();

      await run(changes);
      expect(await belief.recordedNow()).toBe(settled);
    });

    it("reopens a closed window on the same row", async () => {
      // A relationship that resumes is one continuous row, not a second
      // incarnation: `clearValidTo` keeps the edge id AND the original
      // `validFrom`, so the gap is visible in valid time rather than in a
      // duplicated entity.
      const endpoints = [
        { kind: "Person", id: "p1" },
        { kind: "Company", id: "c1" },
      ] as const;
      const relate = (valid: ValidTime): Decoder<typeof intelGraph, Row> => (_c, emit) => [
        emit.nodes.Person.upsert("p1", { name: "J", email: "j@x", title: "VP" }),
        emit.nodes.Company.upsert("c1", { name: "Acme", domain: "acme.com" }),
        emit.edges.worksAt.upsert(endpoints[0], endpoints[1], undefined, valid),
      ];
      const apply = async (valid: ValidTime, offset: string): Promise<void> => {
        const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);
        await consume({
          source: mockShapeSource("intel", [change(offset, "any", "x", {})]),
          store: belief,
          checkpoints: typeGraphCheckpoints(cursor),
          project: graphProjector(intelGraph, relate(valid)),
        });
      };
      const opened = { validFrom: "2019-03-01T00:00:00.000Z" } as const;

      await apply(opened, "1");
      const first = await belief.edges.worksAt.findByEndpoints(endpoints[0], endpoints[1]);

      await apply({ ...opened, validTo: "2021-06-01T00:00:00.000Z" }, "2");
      expect(await belief.edges.worksAt.findByEndpoints(endpoints[0], endpoints[1])).toBeUndefined();

      await apply({ ...opened, clearValidTo: true }, "3");
      const resumed = await belief.edges.worksAt.findByEndpoints(endpoints[0], endpoints[1]);
      expect(resumed?.id).toBe(first?.id);
      expect(resumed?.meta?.validTo).toBeUndefined();
      // The window still starts where it always did — reopening is not rebirth.
      expect(resumed?.meta?.validFrom).toBe(opened.validFrom);
    });

    it("rejects an event that both ends and reopens a window", () => {
      const contradictory: ValidTime = {
        validTo: "2021-06-01T00:00:00.000Z",
        // @ts-expect-error — an end and a reopening are mutually exclusive.
        clearValidTo: true,
      };
      expect(contradictory).toBeDefined();
    });

    it("tolerates removing an edge that is not there", async () => {
      const removeMissing: Decoder<typeof intelGraph, Row> = (_c, emit) => [
        emit.nodes.Person.upsert("p1", { name: "J", email: "j@x", title: "VP" }),
        emit.edges.worksAt.remove({ kind: "Person", id: "p1" }, { kind: "Company", id: "nope" }),
      ];
      const [cursor] = await createStoreWithSchema(checkpointGraph, createLocalSqliteBackend().backend);

      await expect(
        consume({
          source: mockShapeSource("intel", [change("1", "any", "x", {})]),
          store: belief,
          checkpoints: typeGraphCheckpoints(cursor),
          project: graphProjector(intelGraph, removeMissing),
        }),
      ).resolves.toMatchObject({ processed: 1 });
    });
  });

  // TypeGraph 0.50 turned declared edge cardinality from a probe into a CLAIM:
  // a reservation in `typegraph_edge_claims` that the relation's primary key
  // refuses a second live claimant of. The projector's edge write is
  // endpoint-matched and re-delivered constantly, so it is worth pinning that
  // the fence refuses the violation it exists for WITHOUT refusing the replay
  // of the row that already holds the claim — an edge event that re-stated its
  // own relationship would otherwise abort the batch, leave the cursor
  // unadvanced, and re-deliver forever.
  describe("declared edge cardinality", () => {
    const soleEmployer = defineEdge("soleEmployer", { schema: z.object({}) });
    const soleGraph = defineGraph({
      id: "graph_events_sole",
      nodes: { Person: { type: Person }, Company: { type: Company } },
      edges: {
        soleEmployer: { type: soleEmployer, from: [Person], to: [Company], cardinality: "one" },
      },
    });
    const emit = graphEmitter(soleGraph);
    const person = { kind: "Person", id: "p1" } as const;
    const acme = { kind: "Company", id: "c1" } as const;
    const globex = { kind: "Company", id: "c2" } as const;

    let belief: Store<typeof soleGraph>;

    beforeEach(async () => {
      [belief] = await createStoreWithSchema(soleGraph, createLocalSqliteBackend().backend, {
        history: true,
        coalesceUnchangedUpserts: true,
      });
    });

    const apply = (events: readonly GraphEvent<typeof soleGraph>[]): Promise<void> =>
      belief.transaction(async (tx) => {
        await applyGraphEvents(tx, events);
      });

    const opened = { validFrom: "2019-03-01T00:00:00.000Z" } as const;
    const endpoints = [
      emit.nodes.Person.upsert("p1", { name: "J", email: "j@x", title: "VP" }),
      emit.nodes.Company.upsert("c1", { name: "Acme", domain: "acme.com" }),
      emit.nodes.Company.upsert("c2", { name: "Globex", domain: "globex.com" }),
    ];

    it("replays the edge holding the claim without churning history", async () => {
      const batch = [...endpoints, emit.edges.soleEmployer.upsert(person, acme, undefined, opened)];

      await apply(batch);
      const settled = await belief.recordedNow();

      await apply(batch);
      expect(await belief.recordedNow()).toBe(settled);
    });

    it("reopens the claimed window on the same row", async () => {
      await apply([...endpoints, emit.edges.soleEmployer.upsert(person, acme, undefined, opened)]);
      const first = await belief.edges.soleEmployer.findByEndpoints(person, acme);

      await apply([emit.edges.soleEmployer.upsert(person, acme, undefined, { validTo: "2021-06-01T00:00:00.000Z" })]);
      expect(await belief.edges.soleEmployer.findByEndpoints(person, acme)).toBeUndefined();

      // Reopening takes the axis back rather than minting a rival claimant: an
      // ended holder fails the claim's liveness predicate and is replaced in
      // place, so the resumed edge is the very row that ended.
      await apply([emit.edges.soleEmployer.upsert(person, acme, undefined, { clearValidTo: true })]);
      const resumed = await belief.edges.soleEmployer.findByEndpoints(person, acme);
      expect(resumed?.id).toBe(first?.id);
      expect(resumed?.meta?.validFrom).toBe(opened.validFrom);
    });

    it("refuses a second live employer for one person", async () => {
      await apply([...endpoints, emit.edges.soleEmployer.upsert(person, acme, undefined, opened)]);

      await expect(apply([emit.edges.soleEmployer.upsert(person, globex)])).rejects.toThrow(CardinalityError);
      // The refusal rolls the whole batch back, so the incumbent is untouched.
      expect(await belief.edges.soleEmployer.findByEndpoints(person, acme)).toBeDefined();
    });

    it("reports no pre-fence violation on a store built after the claim relations", async () => {
      await apply([...endpoints, emit.edges.soleEmployer.upsert(person, acme, undefined, opened)]);

      // `verifyConstraintFences` reads the relation each constraint is DECLARED
      // over, not a claim key, so it is the audit that finds violations a
      // pre-0.50 database committed before the fence existed. A store this
      // package created has none by construction.
      expect(await belief.verifyConstraintFences()).toEqual([]);
    });
  });
});

/** Local id brand helper — `getById` takes a branded id. */
function asId(id: string): Parameters<IntelStore["nodes"]["Person"]["getById"]>[0] {
  return id as Parameters<IntelStore["nodes"]["Person"]["getById"]>[0];
}
