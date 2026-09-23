/**
 * Demo — valid time on events: "the fact that came back."
 *
 * An HR feed reports employment as it happens: a person joins, leaves, and is
 * later rehired. The feed carries EVENT time (`validFrom` / `validTo` /
 * `clearValidTo`); the store assigns RECORDED time on its own.
 *
 *   (a) worksAt closes then reopens IN PLACE — one row, not two: same edge
 *       id, original `validFrom` survives. The gap reads as not-employed only
 *       before the reopening lands, or when BOTH time axes are pinned.
 *   (b) heldTitle records a March promotion HR doesn't enter until June:
 *       "what was true in March" (valid time, `store.asOf`) and "what did we
 *       KNOW before the report" (recorded time, `store.asOfRecorded` via
 *       `book.anchorFor`) give DIFFERENT answers on the same data.
 *   (c) The documented trap: a row created with no stated `validFrom` takes
 *       the ingest instant as its lower bound, so closing it in the past is
 *       refused (`INVERTED_VALIDITY_WINDOW`) — then the fix.
 *
 * Run with:  pnpm demo:valid-time
 */
import { defineEdge, defineGraph, defineNode, INVERTED_VALIDITY_WINDOW_CODE, ValidationError } from "@nicia-ai/typegraph";
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
  typeGraphCheckpoints,
  type Projector,
  type ShapeChange,
} from "../src";
import { newStore, RULE, runAsMain, section } from "./_support";

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

const ALICE = { kind: "Person", id: "alice" } as const;
const ACME = { kind: "Company", id: "acme" } as const;

const JOINED_AT = "2026-01-15T00:00:00.000Z";
const LEFT_AT = "2026-04-01T00:00:00.000Z";
const REJOINED_AT = "2026-07-01T00:00:00.000Z";
const PROMOTED_AT = "2026-03-01T00:00:00.000Z"; // effective date, entered into HR in June
const GAP_CHECK_AT = "2026-05-01T00:00:00.000Z"; // between LEFT_AT and REJOINED_AT
const MARCH_COORD = "2026-03-15T00:00:00.000Z"; // inside the promotion's valid window

// Section (c) only: dates safely before any ingest instant.
const LONG_AGO_HIRED_AT = "2019-01-01T00:00:00.000Z";
const LONG_AGO_LEFT_AT = "2020-01-01T00:00:00.000Z";

// ============================================================
// The HR feed: event time rides in the row, ingest/recorded time is the
// store's to assign. The promotion arrives late — HR files the March
// promotion while processing Alice's June rehire paperwork.
// ============================================================

const STREAM_NAME = "hr-feed";
const LEAVE_OFFSET = "003";
const REHIRE_OFFSET = "004";
const PROMOTE_OFFSET = "005";
const PROMOTED_TITLE = "Senior Engineer";

type HrRow = Readonly<{ name?: string; title?: string; effectiveAt?: string }>;

const HR_FEED: readonly ShapeChange<HrRow>[] = [
  { offset: "001", shape: "company", key: ACME.id, operation: "insert", value: { name: "Acme Corp" } },
  { offset: "002", shape: "hire", key: ALICE.id, operation: "insert", value: { name: "Alice Kim", effectiveAt: JOINED_AT } },
  { offset: LEAVE_OFFSET, shape: "leave", key: ALICE.id, operation: "update", value: { effectiveAt: LEFT_AT } },
  { offset: REHIRE_OFFSET, shape: "rehire", key: ALICE.id, operation: "update", value: { effectiveAt: REJOINED_AT } },
  { offset: PROMOTE_OFFSET, shape: "promote", key: ALICE.id, operation: "insert", value: { title: PROMOTED_TITLE, effectiveAt: PROMOTED_AT } },
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

function isInvertedValidityWindow(error: unknown): error is ValidationError {
  return error instanceof ValidationError && error.details.issues.some((issue) => issue.code === INVERTED_VALIDITY_WINDOW_CODE);
}

function employment(edge: unknown): string {
  return edge === undefined ? "not employed" : "employed";
}

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" Valid time on events — one row across a gap, and two axes of time");
  console.log(RULE);

  const belief = await newStore(hrGraph, true);
  const cursor = await newStore(checkpointGraph);
  try {
    const book = typeGraphCheckpoints(cursor);
    const source = mockShapeSource(STREAM_NAME, HR_FEED);

    // ----------------------------------------------------------
    // (a) The employment window: opened, closed, reopened in place
    // ----------------------------------------------------------
    section("(a) worksAt — closed, then reopened: the same row, not a second one");

    await consume({ source, store: belief, checkpoints: book, project, stopAfter: 3 }); // company, hire, leave
    const foundClosed = await belief.edges.worksAt.findByEndpoints(ALICE, ACME, undefined, { temporalMode: "includeEnded" });
    const closed = must(foundClosed, "worksAt edge after hire + leave");
    console.log(`\n  hired ${JOINED_AT}, left ${LEFT_AT}`);
    console.log(`    worksAt window: validFrom=${closed.meta.validFrom} validTo=${closed.meta.validTo}`);

    const gapBeforeRehire = await belief.asOf(GAP_CHECK_AT).edges.worksAt.findByEndpoints(ALICE, ACME);
    console.log(`  asOf(${GAP_CHECK_AT}) before the rehire -> ${employment(gapBeforeRehire)}`);
    if (gapBeforeRehire !== undefined) throw new Error("expected the gap to read as not-employed before the rehire lands");

    await consume({ source, store: belief, checkpoints: book, project, stopAfter: 1 }); // rehire
    const foundReopened = await belief.edges.worksAt.getById(closed.id, { temporalMode: "includeEnded" });
    const reopened = must(foundReopened, "worksAt edge after rehire");
    console.log(`\n  rehired ${REJOINED_AT}`);
    console.log(`    worksAt window: validFrom=${reopened.meta.validFrom} validTo=${reopened.meta.validTo ?? "(open)"}`);
    if (reopened.id !== closed.id) throw new Error("resuming the relationship minted a new row instead of reopening the old one");
    if (reopened.meta.validFrom !== closed.meta.validFrom) throw new Error("the reopen rewrote the row's original validFrom");
    console.log("    -> ONE row: same id, original validFrom survived the reopen");

    // A row carries ONE window, so reopening EXTENDS it across the
    // interruption: the same valid-time coordinate now gives the opposite
    // answer. A gap you need to query in valid time alone has to be two rows.
    const gapAfterRehire = await belief.asOf(GAP_CHECK_AT).edges.worksAt.findByEndpoints(ALICE, ACME);
    console.log(`  asOf(${GAP_CHECK_AT}) after the rehire  -> ${employment(gapAfterRehire)}`);
    if (gapAfterRehire === undefined) {
      throw new Error("expected the reopened window to cover the former gap: `clearValidTo` extends the row's single window");
    }
    console.log("    -> reopening covers the live row's whole span; the gap read as not-employed");
    console.log("       only while the resumption had not yet been applied.");

    // ...but the interruption is not LOST: pin recorded time to the leave's
    // checkpoint anchor as well. A RecordedStoreView has no endpoint reads
    // (no findByEndpoints), so the row is addressed by the id captured above.
    const anchorBeforeRehire = must(await book.anchorFor(STREAM_NAME, LEAVE_OFFSET), `anchor at ${STREAM_NAME}@${LEAVE_OFFSET}`);
    const gapReconstructed = await belief.asOf(GAP_CHECK_AT).asOfRecorded(anchorBeforeRehire).edges.worksAt.getById(closed.id);
    console.log(`\n  asOf(${GAP_CHECK_AT}).asOfRecorded(before the rehire) -> ${employment(gapReconstructed)}`);
    if (gapReconstructed !== undefined) {
      throw new Error("expected the composed bitemporal view, pinned before the rehire, to reconstruct the gap as ended");
    }
    console.log("    -> BOTH axes together recover it: 'was she employed mid-gap, as far as we");
    console.log("       knew before the rehire was filed?' — no. One axis alone cannot say that.");

    // ----------------------------------------------------------
    // (b) The bitemporal payoff: a fact learned late
    // ----------------------------------------------------------
    section("(b) heldTitle — a March promotion HR doesn't file until June");

    await consume({ source, store: belief, checkpoints: book, project }); // promote
    const anchorBeforeReport = must(await book.anchorFor(STREAM_NAME, REHIRE_OFFSET), `anchor at ${STREAM_NAME}@${REHIRE_OFFSET}`);
    const anchorAfterReport = must(await book.anchorFor(STREAM_NAME, PROMOTE_OFFSET), `anchor at ${STREAM_NAME}@${PROMOTE_OFFSET}`);

    const promotion = must(await belief.edges.heldTitle.findByEndpoints(ALICE, ACME), "heldTitle edge after promote");
    console.log(`\n  HR enters, in June, that Alice became "${promotion.title}" effective ${PROMOTED_AT}`);

    const trueInMarch = await belief.asOf(MARCH_COORD).edges.heldTitle.findByEndpoints(ALICE, ACME);
    const knownBefore = await belief.asOfRecorded(anchorBeforeReport).edges.heldTitle.getById(promotion.id);
    const knownAfter = await belief.asOfRecorded(anchorAfterReport).edges.heldTitle.getById(promotion.id);

    console.log(`\n  "what was true on ${MARCH_COORD}" (valid time,    store.asOf)         -> ${trueInMarch?.title ?? "no title on record"}`);
    console.log(`  "what did we know before the report"      (recorded time, store.asOfRecorded) -> ${knownBefore?.title ?? "nothing yet"}`);
    console.log(`  "what do we know now"                     (recorded time, store.asOfRecorded) -> ${knownAfter?.title ?? "nothing yet"}`);

    if (trueInMarch?.title !== PROMOTED_TITLE) throw new Error("expected valid time to report the March title once it is known");
    if (knownBefore !== undefined) throw new Error("expected recorded time, pinned before the report, to know nothing about it");
    if (knownAfter?.title !== PROMOTED_TITLE) throw new Error("expected recorded time, pinned after the report, to know about it");
    console.log("\n  -> the two axes disagree: it WAS true in March, but we didn't KNOW it");
    console.log("     until the June batch was processed. That gap is the entire point of");
    console.log("     keeping both clocks.");

    // ----------------------------------------------------------
    // (c) The trap, and the fix
    // ----------------------------------------------------------
    section("(c) The trap — an unstated validFrom takes the ingest instant");

    // Events applied directly rather than through the stream, one per
    // transaction, to isolate the valid-time rule from the consumer.
    const emit = graphEmitter(hrGraph);
    const applyOne = (event: GraphEvent<typeof hrGraph>) => belief.transaction((tx) => applyGraphEvents(tx, [event]));

    const priya = { kind: "Person", id: "priya" } as const;
    await belief.nodes.Person.create({ name: "Priya" }, { id: priya.id });
    await applyOne(emit.edges.worksAt.upsert(priya, ACME, undefined, {}));
    console.log("\n  hired Priya with NO validFrom stated -> her edge's lower bound is now (ingest time)");

    const refusal = await applyOne(emit.edges.worksAt.upsert(priya, ACME, undefined, { validTo: LONG_AGO_LEFT_AT })).then(
      () => undefined,
      (error: unknown) => error,
    );
    if (!isInvertedValidityWindow(refusal)) {
      throw new Error(`expected ${INVERTED_VALIDITY_WINDOW_CODE} closing Priya's window at ${LONG_AGO_LEFT_AT}`, { cause: refusal });
    }
    console.log(`  closing her window at ${LONG_AGO_LEFT_AT} was REFUSED (${INVERTED_VALIDITY_WINDOW_CODE}):`);
    console.log("    her ingest-time validFrom is after that validTo");

    const sam = { kind: "Person", id: "sam" } as const;
    await belief.nodes.Person.create({ name: "Sam" }, { id: sam.id });
    await applyOne(emit.edges.worksAt.upsert(sam, ACME, undefined, { validFrom: LONG_AGO_HIRED_AT }));
    await applyOne(emit.edges.worksAt.upsert(sam, ACME, undefined, { validTo: LONG_AGO_LEFT_AT }));
    console.log("\n  the fix: Sam's HIRE event states validFrom itself -> closing in the past then succeeds.");
    console.log("  If your stream carries event time, emit validFrom on every event, not just the closing one.");

    // A fact that arrives ALREADY historical is the one case with no trap: one
    // event that both creates a row and ends it in the past stores NO lower
    // bound at all ("ended at T, start unknown").
    const dana = { kind: "Person", id: "dana" } as const;
    await belief.nodes.Person.create({ name: "Dana" }, { id: dana.id });
    await applyOne(emit.edges.worksAt.upsert(dana, ACME, undefined, { validTo: LONG_AGO_LEFT_AT }));
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
