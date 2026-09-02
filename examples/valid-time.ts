/**
 * Demo — valid time on events: "the fact that came back."
 *
 * An HR feed reports employment as it happens: a person joins, leaves, and is
 * later rehired. The feed carries EVENT time (`validFrom` / `validTo` /
 * `clearValidTo`); the store assigns RECORDED time on its own.
 *
 *   (a) worksAt closes then reopens IN PLACE — one row, not two: same edge
 *       id, original `validFrom` survives, gap visible before the resumption.
 *   (b) heldTitle records a March promotion HR doesn't enter until June:
 *       "what was true in March" (valid time, `store.asOf`) and "what did we
 *       KNOW in March" (recorded time, `store.asOfRecorded` via
 *       `book.anchorFor`) give DIFFERENT answers on the same data.
 *   (c) The documented trap: a row created with no stated `validFrom` takes
 *       the ingest instant as its lower bound, so closing it in the past is
 *       refused (`INVERTED_VALIDITY_WINDOW`) — then the fix.
 *
 * Run with:  pnpm tsx examples/valid-time.ts
 */
import { defineEdge, defineGraph, defineNode, INVERTED_VALIDITY_WINDOW_CODE, ValidationError } from "@nicia-ai/typegraph";
import { z } from "zod";

import {
  applyGraphEvents,
  checkpointGraph,
  consume,
  type Decoder,
  graphEmitter,
  graphProjector,
  mockShapeSource,
  typeGraphCheckpoints,
  type Projector,
  type ShapeChange,
} from "../src";
import { type DemoHistoryStore, newStore, runAsMain } from "./_support";

// ============================================================
// The graph: employment (worksAt) and a title fact (heldTitle), kept as
// separate edge kinds so a promotion is its own row rather than an in-place
// prop edit — valid time is a property of a row's EXISTENCE window, so a
// fact that must answer "was this true on date X" needs its own row.
// ============================================================

const Person = defineNode("Person", { schema: z.object({ name: z.string() }) });
const Company = defineNode("Company", { schema: z.object({ name: z.string() }) });
const worksAt = defineEdge("worksAt", { schema: z.object({}) });
const heldTitle = defineEdge("heldTitle", { schema: z.object({ title: z.string() }) });

const hrGraph = defineGraph({
  id: "hr_feed",
  nodes: { Person: { type: Person }, Company: { type: Company } },
  edges: {
    worksAt: { type: worksAt, from: [Person], to: [Company] },
    heldTitle: { type: heldTitle, from: [Person], to: [Company] },
  },
});
type HrStore = DemoHistoryStore<typeof hrGraph>;

const ALICE = { kind: "Person", id: "alice" } as const;
const ACME = { kind: "Company", id: "acme" } as const;

const JOINED_AT = "2026-01-15T00:00:00.000Z";
const LEFT_AT = "2026-04-01T00:00:00.000Z";
const REJOINED_AT = "2026-07-01T00:00:00.000Z";
const PROMOTED_AT = "2026-03-01T00:00:00.000Z"; // effective date, entered into HR in June
const GAP_CHECK_AT = "2026-05-01T00:00:00.000Z"; // between LEFT_AT and REJOINED_AT
const MARCH_COORD = "2026-03-15T00:00:00.000Z"; // inside the promotion's valid window

// ============================================================
// The HR feed: event time rides in the row, ingest/recorded time is the
// store's to assign. offsets 001-004 arrive on time; 005 is late — HR files
// the March promotion while processing Alice's June rehire paperwork.
// ============================================================

type HrRow = Readonly<{ name?: string; title?: string; effectiveAt?: string }>;

const HR_FEED: readonly ShapeChange<HrRow>[] = [
  { offset: "001", shape: "company", key: ACME.id, operation: "insert", value: { name: "Acme Corp" } },
  { offset: "002", shape: "hire", key: ALICE.id, operation: "insert", value: { name: "Alice Kim", effectiveAt: JOINED_AT } },
  { offset: "003", shape: "leave", key: ALICE.id, operation: "update", value: { effectiveAt: LEFT_AT } },
  { offset: "004", shape: "rehire", key: ALICE.id, operation: "update", value: { effectiveAt: REJOINED_AT } },
  { offset: "005", shape: "promote", key: ALICE.id, operation: "insert", value: { title: "Senior Engineer", effectiveAt: PROMOTED_AT } },
];

const decode: Decoder<typeof hrGraph, HrRow> = (change, emit) => {
  const person = { kind: "Person", id: change.key } as const;
  switch (change.shape) {
    case "company":
      return [emit.nodes.Company.upsert(change.key, { name: change.value.name ?? "" })];
    case "hire": {
      const { name = "", effectiveAt } = change.value;
      const validFrom = effectiveAt === undefined ? {} : { validFrom: effectiveAt };
      return [emit.nodes.Person.upsert(change.key, { name }, validFrom), emit.edges.worksAt.upsert(person, ACME, undefined, validFrom)];
    }
    case "leave": {
      const { effectiveAt } = change.value;
      return effectiveAt === undefined ? [] : [emit.edges.worksAt.upsert(person, ACME, undefined, { validTo: effectiveAt })];
    }
    case "rehire":
      // The report date is narrative only — `clearValidTo` needs no `validFrom`:
      // the row already remembers when the relationship first began.
      return [emit.edges.worksAt.upsert(person, ACME, undefined, { clearValidTo: true })];
    case "promote": {
      const { title, effectiveAt } = change.value;
      return title === undefined || effectiveAt === undefined
        ? []
        : [emit.edges.heldTitle.upsert(person, ACME, { title }, { validFrom: effectiveAt })];
    }
    default:
      return [];
  }
};

const project: Projector<typeof hrGraph, HrRow> = graphProjector(hrGraph, decode);

function must<T>(value: T | undefined, label: string): T {
  if (value === undefined) throw new Error(`expected ${label}, got none`);
  return value;
}

const RULE = "━".repeat(74);
function section(title: string): void {
  console.log("\n" + RULE);
  console.log(` ${title}`);
  console.log(RULE);
}

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" Valid time on events — one row across a gap, and two axes of time");
  console.log(RULE);

  const belief: HrStore = await newStore(hrGraph, true);
  const cursor = await newStore(checkpointGraph);
  try {
    const book = typeGraphCheckpoints(cursor);
    const source = mockShapeSource("hr-feed", HR_FEED);

    // ----------------------------------------------------------
    // (a) The employment window: opened, closed, reopened in place
    // ----------------------------------------------------------
    section("(a) worksAt — closed, then reopened: the same row, not a second one");

    await consume({ source, store: belief, checkpoints: book, project, stopAfter: 3 }); // company, hire, leave
    const foundClosed = await belief.edges.worksAt.findByEndpoints(ALICE, ACME, undefined, { temporalMode: "includeEnded" });
    const closed = must(foundClosed, "worksAt edge after hire + leave");
    console.log(`\n  hired ${JOINED_AT}, left ${LEFT_AT}`);
    console.log(`    edge ${closed.id} — validFrom=${closed.meta.validFrom} validTo=${closed.meta.validTo}`);

    const gapBeforeRehire = await belief.asOf(GAP_CHECK_AT).edges.worksAt.findByEndpoints(ALICE, ACME);
    console.log(`  asOf(${GAP_CHECK_AT}) before the rehire -> ${gapBeforeRehire === undefined ? "not employed" : "employed"}`);
    if (gapBeforeRehire !== undefined) throw new Error("expected the gap to read as not-employed before the rehire lands");

    // Pin recorded time on the closed state, so the gap can be reconstructed
    // after the reopening has extended the window past it.
    const anchorBeforeRehire = must(await belief.recordedNow(), "recorded anchor before the rehire");

    await consume({ source, store: belief, checkpoints: book, project, stopAfter: 1 }); // rehire
    const foundReopened = await belief.edges.worksAt.getById(closed.id, { temporalMode: "includeEnded" });
    const reopened = must(foundReopened, "worksAt edge after rehire");
    console.log(`\n  rehired ${REJOINED_AT}`);
    console.log(`    edge ${reopened.id} — validFrom=${reopened.meta.validFrom} validTo=${reopened.meta.validTo ?? "(open)"}`);
    if (reopened.id !== closed.id) throw new Error("resuming the relationship minted a new row instead of reopening the old one");
    if (reopened.meta.validFrom !== closed.meta.validFrom) throw new Error("the reopen rewrote the row's original validFrom");
    console.log("    -> ONE row: same id, original validFrom survived the reopen");

    const gapAfterRehire = await belief.asOf(GAP_CHECK_AT).edges.worksAt.findByEndpoints(ALICE, ACME);
    console.log(`  asOf(${GAP_CHECK_AT}) after the rehire  -> ${gapAfterRehire === undefined ? "not employed" : "employed"}`);
    // The same coordinate, the opposite answer. A row carries ONE window, so
    // reopening EXTENDS it across the interruption instead of recording two
    // disjoint spans — which is precisely why `clearValidTo` models a fact that
    // RESUMED, not one with a hole in it. A gap you need to query in valid time
    // has to be two rows.
    if (gapAfterRehire === undefined) {
      throw new Error(
        "expected the reopened window to cover the former gap: `clearValidTo` extends the row's " +
          "single window rather than leaving a hole in it",
      );
    }
    console.log("    -> reopening covers the live row's whole span; the gap read as not-employed");
    console.log("       only while the resumption had not yet been applied.");

    // ...but the interruption is not LOST. Ask both axes at once and it comes
    // back. Views compose valid-time-then-recorded, and a RecordedStoreView is a
    // reconstructing read — it exposes getById/getByIds/scan/query/subgraph but
    // NOT findByEndpoints, which is why the id is captured from a live read
    // first and addressed on the composed view here.
    const bitemporal = belief.asOf(GAP_CHECK_AT).asOfRecorded(anchorBeforeRehire);
    const gapReconstructed = await bitemporal.edges.worksAt.getById(closed.id);
    console.log(
      `\n  asOf(${GAP_CHECK_AT}).asOfRecorded(before the rehire) -> ` +
        `${gapReconstructed === undefined ? "not employed" : "employed"}`,
    );
    if (gapReconstructed !== undefined) {
      throw new Error(
        "expected the composed bitemporal view to reconstruct the gap: at a valid-time coordinate " +
          "inside the interruption, pinned to recorded time before the rehire, the row must read as ended",
      );
    }
    console.log("    -> BOTH axes together recover it: 'was she employed mid-gap, as far as we");
    console.log("       knew before the rehire was filed?' — no. One axis alone cannot say that.");

    // ----------------------------------------------------------
    // (b) The bitemporal payoff: a fact learned late
    // ----------------------------------------------------------
    section("(b) heldTitle — a March promotion HR doesn't file until June");

    const anchorBeforeReport = must(await book.anchorFor("hr-feed", "004"), "recorded anchor at hr-feed@004");
    await consume({ source, store: belief, checkpoints: book, project }); // promote
    const anchorAfterReport = must(await book.anchorFor("hr-feed", "005"), "recorded anchor at hr-feed@005");

    const promotion = must(await belief.edges.heldTitle.findByEndpoints(ALICE, ACME), "heldTitle edge after promote");
    console.log(`\n  HR enters, in June, that Alice became "${promotion.title}" effective ${PROMOTED_AT}`);

    const trueInMarch = await belief.asOf(MARCH_COORD).edges.heldTitle.findByEndpoints(ALICE, ACME);
    const knownBefore = await belief.asOfRecorded(anchorBeforeReport).edges.heldTitle.getById(promotion.id);
    const knownAfter = await belief.asOfRecorded(anchorAfterReport).edges.heldTitle.getById(promotion.id);

    console.log(`\n  "what was true on ${MARCH_COORD}"      (valid time,    store.asOf)         -> ${trueInMarch?.title ?? "no title on record"}`);
    console.log(`  "what did we know before the report" (recorded time, store.asOfRecorded) -> ${knownBefore?.title ?? "nothing yet"}`);
    console.log(`  "what do we know now"                (recorded time, store.asOfRecorded) -> ${knownAfter?.title ?? "nothing yet"}`);

    if (trueInMarch?.title !== "Senior Engineer") throw new Error("expected valid time to report the March title once it is known");
    if (knownBefore !== undefined) throw new Error("expected recorded time, pinned before the report, to know nothing about it");
    if (knownAfter?.title !== "Senior Engineer") throw new Error("expected recorded time, pinned after the report, to know about it");
    console.log("\n  -> the two axes disagree: it WAS true in March, but we didn't KNOW it");
    console.log("     until the June batch was processed. That gap is the entire point of");
    console.log("     keeping both clocks.");

    // ----------------------------------------------------------
    // (c) The trap, and the fix
    // ----------------------------------------------------------
    section("(c) The trap — an unstated validFrom takes the ingest instant");

    const emit = graphEmitter(hrGraph);
    const applyOne = (event: ReturnType<typeof emit.edges.worksAt.upsert>) => belief.transaction((tx) => applyGraphEvents(tx, [event]));

    const priya = { kind: "Person", id: "priya" } as const;
    await belief.nodes.Person.create({ name: "Priya" }, { id: priya.id });
    await applyOne(emit.edges.worksAt.upsert(priya, ACME, undefined, {}));
    console.log("\n  hired Priya with NO validFrom stated -> her edge's lower bound is now (ingest time)");

    try {
      await applyOne(emit.edges.worksAt.upsert(priya, ACME, undefined, { validTo: "2020-01-01T00:00:00.000Z" }));
      throw new Error("expected INVERTED_VALIDITY_WINDOW — the library accepted an impossible window");
    } catch (error) {
      const isExpected = error instanceof ValidationError && error.details.issues.some((issue) => issue.code === INVERTED_VALIDITY_WINDOW_CODE);
      if (!isExpected) throw error;
      console.log(`  closing her window at a 2020 date was REFUSED:\n    ${(error as ValidationError).message}`);
    }

    const sam = { kind: "Person", id: "sam" } as const;
    await belief.nodes.Person.create({ name: "Sam" }, { id: sam.id });
    await applyOne(emit.edges.worksAt.upsert(sam, ACME, undefined, { validFrom: "2019-01-01T00:00:00.000Z" }));
    await applyOne(emit.edges.worksAt.upsert(sam, ACME, undefined, { validTo: "2020-01-01T00:00:00.000Z" }));
    console.log("\n  the fix: Sam's HIRE event states validFrom itself -> closing in the past then succeeds.");
    console.log("  If your stream carries event time, emit validFrom on every event, not just the closing one.");

    // A fact that arrives ALREADY historical is the one case with no trap: one
    // event that both creates a row and ends it in the past stores NO lower
    // bound at all ("ended at T, start unknown") — verified below.
    const dana = { kind: "Person", id: "dana" } as const;
    await belief.nodes.Person.create({ name: "Dana" }, { id: dana.id });
    await applyOne(emit.edges.worksAt.upsert(dana, ACME, undefined, { validTo: "2020-06-01T00:00:00.000Z" }));
    const foundDana = await belief.edges.worksAt.findByEndpoints(dana, ACME, undefined, { temporalMode: "includeEnded" });
    const danaEdge = must(foundDana, "Dana's already-historical edge");
    console.log(`\n  Dana's one event both creates and ends her row -> meta.validFrom is ${danaEdge.meta.validFrom ?? "undefined"} (no lower bound)`);
    if (danaEdge.meta.validFrom !== undefined) throw new Error("expected an already-historical row to store no lower bound");

    console.log("\n" + RULE);
    console.log(" One row survives a close/reopen; valid time and recorded time can");
    console.log(" genuinely disagree; an unstated validFrom is a trap for a replayed log.");
    console.log(RULE + "\n");
  } finally {
    await Promise.allSettled([belief.close(), cursor.close()]);
  }
}

runAsMain(import.meta.url, main);
