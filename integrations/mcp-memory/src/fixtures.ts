/**
 * Seed data for a fresh memory store: two observation streams that see the
 * same person under different spellings, and a two-source verification
 * stream that gives `whySoFar` a real justification chain to walk.
 *
 * These are consumed once, on first open, by `store.ts`. They are also what
 * `demo.ts` and the tests narrate against, so the story is told from one
 * place instead of drifting between fixture data and prose.
 */
import type { ShapeChange } from "@nicia-ai/agent-stream-graph";

/** A sighting of a person (and optionally their employer) from one source. */
export type ObservationValue = Readonly<{
  personEmail: string;
  personName: string;
  title?: string;
  orgDomain?: string;
  orgName?: string;
}>;

/** One source independently vouching for a person's identity. */
export type VerificationValue = Readonly<{
  personEmail: string;
  sourceId: string;
  sourceLabel: string;
}>;

export const JANE_EMAIL = "jane.doe@acme.example";

/** A recruiting tool's scrape: first sighting, then a title correction. */
export const STREAM_LINKEDIN = "linkedin-scrape";
export const LINKEDIN_CHANGES: readonly ShapeChange<ObservationValue>[] = [
  {
    offset: "001",
    shape: "person",
    key: "jane-linkedin",
    operation: "insert",
    value: {
      personEmail: JANE_EMAIL,
      personName: "Jane Doe",
      title: "VP Engineering",
      orgDomain: "acme.example",
      orgName: "Acme Corp",
    },
  },
  {
    // The correction `believedAt` is built to catch: the title changes, but
    // nothing else about the row does.
    offset: "002",
    shape: "person",
    key: "jane-linkedin",
    operation: "update",
    value: { personEmail: JANE_EMAIL, personName: "Jane Doe", title: "VP Engineering & Product" },
  },
];

/** A CRM import that has seen the same person, under an abbreviated
 * spelling — the case `recall()`'s entity resolution exists for. */
export const STREAM_CRM = "crm-import";
export const CRM_CHANGES: readonly ShapeChange<ObservationValue>[] = [
  {
    offset: "001",
    shape: "person",
    key: "jane-crm",
    operation: "insert",
    value: { personEmail: JANE_EMAIL, personName: "J. Doe", title: "VP Eng" },
  },
];

/** Two independent sources vouching for the same identity fact. */
export const STREAM_VERIFICATION = "identity-verification";
export const ID_CHECK_SOURCE = "id-check-vendor";
export const BACKGROUND_SCAN_SOURCE = "background-scan";
export const VERIFICATION_CHANGES: readonly ShapeChange<VerificationValue>[] = [
  {
    offset: "001",
    shape: "verification",
    key: ID_CHECK_SOURCE,
    operation: "insert",
    value: { personEmail: JANE_EMAIL, sourceId: ID_CHECK_SOURCE, sourceLabel: "ID Check Vendor scan" },
  },
  {
    offset: "002",
    shape: "verification",
    key: BACKGROUND_SCAN_SOURCE,
    operation: "insert",
    value: { personEmail: JANE_EMAIL, sourceId: BACKGROUND_SCAN_SOURCE, sourceLabel: "Background Screening Co" },
  },
];
