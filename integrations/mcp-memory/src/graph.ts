/**
 * The belief graph this memory server materializes: people, orgs, and
 * source-justified facts about them.
 *
 *   Person --worksAt--> Org
 *   Source --premiseOf--> Justification --derives--> Fact --about--> Person
 *
 * Two ideas ride on top of the plain node/edge shapes:
 *
 *  - ENTITY RESOLUTION: `Person.email` (and `Org.domain`) is the stable
 *    identity key. The node id is a normalized form of that key, so two
 *    observations of the same person under different spellings ("Jane Doe"
 *    vs "J. Doe") land on the SAME row — the projector in `store.ts` reads
 *    the existing row before writing and folds the new spelling into
 *    `aliases` instead of creating a second person. `recall()` in
 *    `tools.ts` resolves any of those spellings back to the one canonical
 *    row.
 *  - PROVENANCE: a `Fact` is never written directly — it is *derived* from
 *    a `Justification`, which is in turn premised on one or more `Source`
 *    nodes. This is the same shape `examples/provenance-retraction.ts` in
 *    the parent package uses, reused here for exactly one predicate
 *    ("verified") so `whySoFar()` has a real justification chain to walk
 *    and `@nicia-ai/typegraph/provenance`'s retraction capability has
 *    something to cascade through.
 */
import { defineEdge, defineGraph, defineNode } from "@nicia-ai/typegraph";
import type { ProvenanceRetractionConfig } from "@nicia-ai/typegraph/provenance";
import { z } from "zod";

export const Person = defineNode("Person", {
  schema: z.object({
    name: z.string(),
    email: z.string(),
    title: z.string(),
    aliases: z.array(z.string()),
  }),
});

export const Org = defineNode("Org", {
  schema: z.object({
    name: z.string(),
    domain: z.string(),
    aliases: z.array(z.string()),
  }),
});

export const Source = defineNode("Source", {
  schema: z.object({
    label: z.string(),
    retracted: z.boolean().default(false),
  }),
});

export const Justification = defineNode("Justification", {
  schema: z.object({ rule: z.string() }),
});

export const Fact = defineNode("Fact", {
  schema: z.object({ predicate: z.string(), value: z.string() }),
});

const worksAt = defineEdge("worksAt", { schema: z.object({}) });
const about = defineEdge("about", { schema: z.object({}) });
const premiseOf = defineEdge("premiseOf", { schema: z.object({}) });
const derives = defineEdge("derives", { schema: z.object({}) });

export const memoryGraph = defineGraph({
  id: "asg_mcp_memory",
  nodes: {
    Person: {
      type: Person,
      unique: [{ name: "person_email", fields: ["email"], scope: "kind", collation: "caseInsensitive" }],
    },
    Org: {
      type: Org,
      unique: [{ name: "org_domain", fields: ["domain"], scope: "kind", collation: "caseInsensitive" }],
    },
    Source: { type: Source },
    Justification: { type: Justification },
    Fact: { type: Fact },
  },
  edges: {
    worksAt: { type: worksAt, from: [Person], to: [Org] },
    about: { type: about, from: [Fact], to: [Person] },
    premiseOf: { type: premiseOf, from: [Source], to: [Justification] },
    derives: { type: derives, from: [Justification], to: [Fact] },
  },
});

export type MemoryGraph = typeof memoryGraph;

/** Config for `createRetractionCapability` — `retractedField` is explicit so
 * `Source.retracted` is the one place retraction state actually lives; a
 * retracted source stays visible (for `whySoFar` to report on), it just
 * stops counting as support for whatever it justified. */
export const retractionConfig = {
  source: { kind: "Source", retractedField: "retracted" },
  justification: { kind: "Justification" },
  fact: { kinds: ["Fact"] },
  premiseOf: { kind: "premiseOf" },
  derives: { kind: "derives" },
} as const satisfies ProvenanceRetractionConfig<MemoryGraph>;

/** The predicate this package demonstrates provenance on. Kept to one
 * predicate so the justification graph in `demo.ts` stays legible. */
export const VERIFIED_PREDICATE = "verified";

/** `Person`/`Org` node ids are normalized identity keys, not opaque ids —
 * this is what makes two observations of the same email collapse into one
 * row without a merge step. */
export function personId(email: string): string {
  return email.trim().toLowerCase();
}

export function orgId(domain: string): string {
  return domain.trim().toLowerCase();
}

export function factId(subject: string, predicate: string): string {
  return `${subject}#${predicate}`;
}

export function justificationId(sourceId: string, forFactId: string): string {
  return `${sourceId}>>${forFactId}`;
}

/** Folds newly-observed spellings into an alias set, preserving order of
 * first sighting and never dropping one that was already known. */
export function mergeAliases(existing: readonly string[], observed: readonly string[]): string[] {
  const merged = new Set(existing);
  for (const alias of observed) merged.add(alias);
  return [...merged];
}
