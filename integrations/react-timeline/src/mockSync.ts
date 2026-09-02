/**
 * Offline fixture source.
 *
 * `pnpm dev` runs against THIS by default — no Electric service, no
 * Postgres, no network. It implements the exact same `Collection` interface
 * `collections.ts` hands back for the live Electric path
 * (`createCollection` + `localOnlyCollectionOptions`, TanStack DB's own
 * sanctioned "in-memory, no external sync" collection kind — not a bespoke
 * stand-in), so `useRecordedTimeline` and every component downstream of it
 * cannot tell which mode they're in.
 *
 * The narrative below is small but exercises every idea the demo claims to
 * show:
 *  - `person-jdoe` — two source agents report the same person under
 *    different names/emails; entity resolution merges them and FLAGS the
 *    disagreement (`contested`) rather than picking a winner, until a later
 *    revision resolves it.
 *  - `person-ada` — an attribute revision (Engineer -> Staff Engineer), and
 *    a valid-time window that closes then reopens IN PLACE: same entity,
 *    `validFrom` never moves, the gap shows up as a closed-then-reopened
 *    window rather than a duplicate row.
 *  - `company-globex` — doesn't arrive until late. Anyone scrubbed to an
 *    earlier recorded instant genuinely cannot know about it yet.
 *  - `company-acme` — a plain later revision (territory reassignment), to
 *    show "known now, but different from what was known then."
 */
import { createCollection, localOnlyCollectionOptions } from "@tanstack/react-db";
import type { BeliefVersionRow, RecordedAnchorRow } from "./types";

const BASE_MS = Date.parse("2026-08-20T09:00:00.000Z");

function tsAt(minutesFromBase: number): string {
  return new Date(BASE_MS + minutesFromBase * 60_000).toISOString();
}

interface AnchorDef {
  readonly revision: number;
  readonly minute: number;
  readonly agent: string;
  readonly offset: string;
  readonly label: string;
  readonly summary: string;
}

const ANCHOR_DEFS: readonly AnchorDef[] = [
  {
    revision: 1,
    minute: 9,
    agent: "crm-agent",
    offset: "0001",
    label: "CRM ingest",
    summary: 'crm-agent reports a new contact: Ada Lovelace, Engineer.',
  },
  {
    revision: 2,
    minute: 18,
    agent: "crm-agent",
    offset: "0002",
    label: "CRM ingest",
    summary: 'crm-agent reports a second contact: "J. Doe".',
  },
  {
    revision: 3,
    minute: 27,
    agent: "directory-agent",
    offset: "0001",
    label: "Directory sync",
    summary: "directory-agent reports a new company: Acme Corp.",
  },
  {
    revision: 4,
    minute: 36,
    agent: "support-agent",
    offset: "0001",
    label: "Support ticket",
    summary:
      'support-agent reports "Jane Doe" at a different email — entity resolution merges her with J. Doe and flags the conflict.',
  },
  {
    revision: 5,
    minute: 45,
    agent: "crm-agent",
    offset: "0003",
    label: "CRM update",
    summary: "crm-agent promotes Ada Lovelace to Staff Engineer.",
  },
  {
    revision: 6,
    minute: 54,
    agent: "directory-agent",
    offset: "0002",
    label: "Directory sync",
    summary: "directory-agent reports Ada Lovelace has gone on leave — her assignment record's window closes.",
  },
  {
    revision: 7,
    minute: 63,
    agent: "support-agent",
    offset: "0002",
    label: "Support review",
    summary: "support-agent resolves the Doe conflict — one canonical identity, contested flag cleared.",
  },
  {
    revision: 8,
    minute: 72,
    agent: "directory-agent",
    offset: "0003",
    label: "Directory sync",
    summary: "directory-agent reports a new company: Globex Inc.",
  },
  {
    revision: 9,
    minute: 81,
    agent: "directory-agent",
    offset: "0004",
    label: "Directory sync",
    summary: "directory-agent reports Ada Lovelace is back — the SAME record reopens, validFrom unchanged.",
  },
  {
    revision: 10,
    minute: 90,
    agent: "crm-agent",
    offset: "0004",
    label: "CRM update",
    summary: "crm-agent reports Acme Corp's territory reassigned to EMEA.",
  },
];

const ANCHORS: readonly RecordedAnchorRow[] = ANCHOR_DEFS.map((def) => ({
  id: `a${def.revision}`,
  recorded: `r1:${def.revision}:${tsAt(def.minute)}`,
  recordedAt: tsAt(def.minute),
  agent: def.agent,
  offset: def.offset,
  label: def.label,
  summary: def.summary,
}));

function recordedAt(revision: number): string {
  const anchor = ANCHORS.find((candidate) => candidate.id === `a${revision}`);
  if (anchor === undefined) {
    throw new Error(`mockSync fixture bug: no anchor defined for revision ${revision}`);
  }
  return anchor.recorded;
}

const ADA_VALID_FROM = tsAt(9);
const ADA_ATTRIBUTES_JUNIOR = { name: "Ada Lovelace", title: "Engineer", team: "Platform" };
const ADA_ATTRIBUTES_STAFF = { name: "Ada Lovelace", title: "Staff Engineer", team: "Platform" };

const JDOE_VALID_FROM = tsAt(18);
const JDOE_ATTRIBUTES_UNMERGED = { name: "J. Doe", email: "j.doe@example.com", team: "Support" };
const JDOE_ATTRIBUTES_CONTESTED = { name: "Jane Doe", email: "jane.doe@example.com", team: "Support" };
const JDOE_ATTRIBUTES_RESOLVED = { name: "Jane Doe", email: "jane.doe@example.com", team: "Support" };

const ACME_VALID_FROM = tsAt(27);
const ACME_ATTRIBUTES_AMERICAS = { name: "Acme Corp", territory: "Americas" };
const ACME_ATTRIBUTES_EMEA = { name: "Acme Corp", territory: "EMEA" };

const GLOBEX_VALID_FROM = tsAt(72);
const GLOBEX_ATTRIBUTES = { name: "Globex Inc", territory: "APAC" };

const VERSIONS: readonly BeliefVersionRow[] = [
  // person-ada: title revision, then a valid-time window that closes and reopens IN PLACE.
  {
    id: `person-ada@${recordedAt(1)}`,
    entityId: "person-ada",
    kind: "Person",
    label: "Ada Lovelace",
    attributes: ADA_ATTRIBUTES_JUNIOR,
    validFrom: ADA_VALID_FROM,
    validTo: null,
    recordedFrom: recordedAt(1),
    recordedTo: recordedAt(5),
    sourceAgents: ["crm-agent"],
    contested: false,
    contestedFields: [],
  },
  {
    id: `person-ada@${recordedAt(5)}`,
    entityId: "person-ada",
    kind: "Person",
    label: "Ada Lovelace",
    attributes: ADA_ATTRIBUTES_STAFF,
    validFrom: ADA_VALID_FROM,
    validTo: null,
    recordedFrom: recordedAt(5),
    recordedTo: recordedAt(6),
    sourceAgents: ["crm-agent"],
    contested: false,
    contestedFields: [],
  },
  {
    id: `person-ada@${recordedAt(6)}`,
    entityId: "person-ada",
    kind: "Person",
    label: "Ada Lovelace",
    attributes: ADA_ATTRIBUTES_STAFF,
    validFrom: ADA_VALID_FROM,
    validTo: tsAt(54), // window closes: on leave
    recordedFrom: recordedAt(6),
    recordedTo: recordedAt(9),
    sourceAgents: ["crm-agent", "directory-agent"],
    contested: false,
    contestedFields: [],
  },
  {
    id: `person-ada@${recordedAt(9)}`,
    entityId: "person-ada",
    kind: "Person",
    label: "Ada Lovelace",
    attributes: ADA_ATTRIBUTES_STAFF,
    validFrom: ADA_VALID_FROM, // unchanged — reopened in place, not a new entity
    validTo: null, // reopened
    recordedFrom: recordedAt(9),
    recordedTo: null,
    sourceAgents: ["crm-agent", "directory-agent"],
    contested: false,
    contestedFields: [],
  },

  // person-jdoe: two agents, one entity, a flagged disagreement that later resolves.
  {
    id: `person-jdoe@${recordedAt(2)}`,
    entityId: "person-jdoe",
    kind: "Person",
    label: "J. Doe",
    attributes: JDOE_ATTRIBUTES_UNMERGED,
    validFrom: JDOE_VALID_FROM,
    validTo: null,
    recordedFrom: recordedAt(2),
    recordedTo: recordedAt(4),
    sourceAgents: ["crm-agent"],
    contested: false,
    contestedFields: [],
  },
  {
    id: `person-jdoe@${recordedAt(4)}`,
    entityId: "person-jdoe",
    kind: "Person",
    label: "Jane Doe",
    attributes: JDOE_ATTRIBUTES_CONTESTED,
    validFrom: JDOE_VALID_FROM,
    validTo: null,
    recordedFrom: recordedAt(4),
    recordedTo: recordedAt(7),
    sourceAgents: ["crm-agent", "support-agent"],
    contested: true,
    contestedFields: ["name", "email"],
  },
  {
    id: `person-jdoe@${recordedAt(7)}`,
    entityId: "person-jdoe",
    kind: "Person",
    label: "Jane Doe",
    attributes: JDOE_ATTRIBUTES_RESOLVED,
    validFrom: JDOE_VALID_FROM,
    validTo: null,
    recordedFrom: recordedAt(7),
    recordedTo: null,
    sourceAgents: ["crm-agent", "support-agent"],
    contested: false,
    contestedFields: [],
  },

  // company-acme: a plain later revision.
  {
    id: `company-acme@${recordedAt(3)}`,
    entityId: "company-acme",
    kind: "Company",
    label: "Acme Corp",
    attributes: ACME_ATTRIBUTES_AMERICAS,
    validFrom: ACME_VALID_FROM,
    validTo: null,
    recordedFrom: recordedAt(3),
    recordedTo: recordedAt(10),
    sourceAgents: ["directory-agent"],
    contested: false,
    contestedFields: [],
  },
  {
    id: `company-acme@${recordedAt(10)}`,
    entityId: "company-acme",
    kind: "Company",
    label: "Acme Corp",
    attributes: ACME_ATTRIBUTES_EMEA,
    validFrom: ACME_VALID_FROM,
    validTo: null,
    recordedFrom: recordedAt(10),
    recordedTo: null,
    sourceAgents: ["directory-agent", "crm-agent"],
    contested: false,
    contestedFields: [],
  },

  // company-globex: arrives late. Nothing before revision 8 can know about it.
  {
    id: `company-globex@${recordedAt(8)}`,
    entityId: "company-globex",
    kind: "Company",
    label: "Globex Inc",
    attributes: GLOBEX_ATTRIBUTES,
    validFrom: GLOBEX_VALID_FROM,
    validTo: null,
    recordedFrom: recordedAt(8),
    recordedTo: null,
    sourceAgents: ["directory-agent"],
    contested: false,
    contestedFields: [],
  },
];

// No explicit return-type annotation: `createCollection` composes the exact
// `Collection<Row, string, Utils> & NonSingleResult` type from its config
// argument, and annotating it by hand here would only risk drifting from
// that (or accidentally widening away the `NonSingleResult` brand
// `useLiveQuery` relies on to accept a collection directly).

export function mockRecordedAnchors() {
  return createCollection(
    localOnlyCollectionOptions({
      id: "asg-recorded-anchors-mock",
      getKey: (row: RecordedAnchorRow) => row.id,
      initialData: [...ANCHORS],
    }),
  );
}

export function mockBeliefVersions() {
  return createCollection(
    localOnlyCollectionOptions({
      id: "asg-belief-versions-mock",
      getKey: (row: BeliefVersionRow) => row.id,
      initialData: [...VERSIONS],
    }),
  );
}
