/**
 * Recorded reporter transcripts — the newsroom's default, offline input.
 *
 * These are the events three reporters emitted while covering one story. They
 * are checked in so the whole demo runs deterministically with no API key, no
 * network, and no cost: `reporters/replay.ts` streams them, `reporters/live.ts`
 * produces the same shape from real Claude agents. Everything downstream — the
 * belief stores, the fork, the editor's review, the retraction cascade — is
 * identical either way, because the only thing that changes is which
 * `ShapeSource` the desk reads from.
 *
 * The transcripts are built to make five things happen, in this order:
 *
 *   1. ash and brook AGREE on a shared prefix (both read the budget filing).
 *   2. they DIVERGE on the contract's value — $41M vs $38M, both "confirmed".
 *      That is the conflict the editor has to arbitrate, and neither reporter
 *      is obviously wrong, which is the point.
 *   3. they spell the same subject differently — "M. Vance" @mvance versus
 *      "Marisa Vance" @MVance. The handle differs only in case, so entity
 *      resolution collapses them and the name becomes a second conflict.
 *   4. cass files a claim resting on ONE source: an anonymous tip.
 *   5. the tip is later burned. cass's claim loses its only support and goes
 *      non-current; ash's claim, independently supported by the filing,
 *      survives. That asymmetry is the whole argument for justification nodes.
 */
import type { ReporterEvent } from "../src/decode.js";

export type ReporterId = "reporter-ash" | "reporter-brook" | "reporter-cass";

/** Every reporter's stream is keyed by the desk's stream path convention. */
export const REPORTERS: readonly ReporterId[] = ["reporter-ash", "reporter-brook", "reporter-cass"];

const FILING = "wire:budget-filing";
const MINUTES = "wire:council-minutes";
const TIP = "tip:insider-01";

/** The procurement official both reporters are writing about, spelled two ways. */
const VANCE_ASH = { id: "subject-vance", name: "M. Vance", handle: "@mvance", role: "procurement chief" } as const;
const VANCE_BROOK = { id: "subject-vance-b", name: "Marisa Vance", handle: "@MVance", role: "procurement director" } as const;
const HALCYON = { id: "subject-halcyon", name: "Halcyon Transit Systems", handle: "@halcyon", role: "vendor" } as const;

const AWARDED = "2026-03-01T00:00:00.000Z";
const REVISED = "2026-03-14T00:00:00.000Z";

/**
 * ash: two sources, and the claim that survives the retraction because the
 * filing backs it independently of the tip.
 */
const ASH: readonly ReporterEvent[] = [
  { type: "wire", id: FILING, label: "Q1 capital budget filing", outlet: "City Clerk" },
  { type: "tip", id: TIP, label: "Anonymous procurement tip", handle: "insider-01" },
  {
    type: "claim",
    id: "claim-award-ash",
    text: "Halcyon Transit Systems was awarded the fleet modernisation contract",
    predicate: "awardedContract",
    value: "fleet-modernisation",
    confidence: "confirmed",
    subject: HALCYON,
    sources: [FILING, TIP],
    rule: "two independent sources",
    validFrom: AWARDED,
  },
  {
    type: "claim",
    id: "claim-value",
    text: "The fleet modernisation contract is worth $41M",
    predicate: "contractValue",
    value: "$41M",
    confidence: "confirmed",
    subject: HALCYON,
    sources: [FILING],
    rule: "primary document",
    validFrom: AWARDED,
  },
  {
    type: "claim",
    id: "claim-signatory-ash",
    text: "M. Vance signed off on the award",
    predicate: "signedBy",
    value: "M. Vance",
    confidence: "single-source",
    subject: VANCE_ASH,
    sources: [FILING],
    rule: "primary document",
    validFrom: AWARDED,
  },
];

/**
 * brook: reads the council minutes instead of the tip, and reads a different
 * number out of them. Same subject, different spelling and a different value.
 */
const BROOK: readonly ReporterEvent[] = [
  { type: "wire", id: FILING, label: "Q1 capital budget filing", outlet: "City Clerk" },
  { type: "wire", id: MINUTES, label: "Transit council minutes, 14 Mar", outlet: "Transit Council" },
  {
    type: "claim",
    id: "claim-award-brook",
    text: "Halcyon Transit Systems holds the fleet modernisation contract",
    predicate: "awardedContract",
    value: "fleet-modernisation",
    confidence: "confirmed",
    subject: HALCYON,
    sources: [FILING],
    rule: "primary document",
    validFrom: AWARDED,
  },
  {
    // The disagreement. Same predicate, same subject, same stated confidence —
    // the merge cannot break this tie on strength, and must not invent one.
    type: "claim",
    id: "claim-value",
    text: "The fleet modernisation contract is worth $38M after the March revision",
    predicate: "contractValue",
    value: "$38M",
    confidence: "confirmed",
    subject: HALCYON,
    sources: [MINUTES],
    rule: "primary document",
    // Deliberately the same `validFrom` as ash's version of this row. The two
    // branches must differ on the PROPERTY and nothing else: `valid_from` is
    // part of a merge branch's `base@V` fingerprint, so varying it here would
    // entangle a validity-window arbitration with the property conflict this
    // row exists to demonstrate. Valid time gets its own demo
    // (`examples/valid-time.ts`); this one is about the disagreement.
    validFrom: AWARDED,
  },
  {
    type: "claim",
    id: "claim-signatory-brook",
    text: "Marisa Vance approved the award",
    predicate: "signedBy",
    value: "Marisa Vance",
    confidence: "confirmed",
    subject: VANCE_BROOK,
    sources: [MINUTES],
    rule: "primary document",
    validFrom: REVISED,
  },
];

/**
 * cass: one claim, one source, and that source is the tip. When the tip is
 * burned this claim has nothing left holding it up.
 */
const CASS: readonly ReporterEvent[] = [
  { type: "tip", id: TIP, label: "Anonymous procurement tip", handle: "insider-01" },
  {
    type: "claim",
    id: "claim-kickback",
    text: "The award followed an undisclosed payment to a council aide",
    predicate: "allegation",
    value: "undisclosed-payment",
    confidence: "single-source",
    subject: HALCYON,
    sources: [TIP],
    rule: "single source, uncorroborated",
    validFrom: AWARDED,
  },
  {
    type: "story",
    id: "story-kickback",
    headline: "Questions over transit contract award",
    status: "draft",
    claims: ["claim-kickback"],
    rule: "draft pending corroboration",
    validFrom: AWARDED,
  },
];

export const TRANSCRIPTS: Readonly<Record<ReporterId, readonly ReporterEvent[]>> = {
  "reporter-ash": ASH,
  "reporter-brook": BROOK,
  "reporter-cass": CASS,
};

/**
 * The source the desk later burns. Retracting it is a WRITE to the source row,
 * which the provenance capability then reads to decide what falls with it —
 * see `desk/retract.ts`.
 */
export const BURNED_SOURCE = { kind: "Tipster", id: TIP } as const;
