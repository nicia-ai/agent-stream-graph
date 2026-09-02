/**
 * The newsroom's schema.
 *
 * A developing story is covered by several reporters. Each reporter reads
 * SOURCES, asserts CLAIMS about SUBJECTS, and eventually files a STORY. Claims
 * are grounded in their sources through explicit JUSTIFICATION nodes, which is
 * what makes a retraction cascade rather than merely delete: pull a source, and
 * every claim that rested on it alone goes non-current, while claims with
 * independent support survive.
 *
 * Two things are deliberately NOT in this schema.
 *
 * There is no `Reporter` node. Each reporter materializes its own belief store,
 * and staging that belief for the merge names the branch after the reporter — so
 * "who contributed this value" is answered by TypeGraph's own merge provenance
 * rather than by an edge we would have to maintain and keep honest. The branch
 * IS the byline.
 *
 * And there is no `retracted` flag on a claim. Retraction is a property of the
 * SOURCE; what happens to the claims is derived from the justification graph by
 * the provenance capability. Storing it twice would let the two disagree.
 */
import { defineEdge, defineGraph, defineNode, searchable } from "@nicia-ai/typegraph";
import { z } from "zod";

/** A filed dispatch, document, or transcript — the citable kind of source. */
const Wire = defineNode("Wire", {
  schema: z.object({
    label: z.string(),
    outlet: z.string(),
    retracted: z.boolean().default(false),
  }),
});

/**
 * An unattributable human tip. A second source KIND, not just a flag, because
 * the desk retracts these as a class — `retractMany` over every tip from a
 * burned informant is the operation this shape exists to make expressible.
 */
const Tipster = defineNode("Tipster", {
  schema: z.object({
    label: z.string(),
    handle: z.string(),
    retracted: z.boolean().default(false),
  }),
});

/**
 * The person or organisation a claim is ABOUT — and the entity-resolution axis.
 *
 * `handle` is the resolution key: two reporters who write "J. Doe" and
 * "Jane Doe" against the same handle are talking about one subject, and the
 * merge is what collapses them. The uniqueness constraint is scoped to the kind
 * and compared case-insensitively, so `@JDoe` and `@jdoe` do not survive as
 * two subjects.
 *
 * `name` is `searchable` so fulltext similarity can propose matches the handle
 * alone would miss.
 */
const Subject = defineNode("Subject", {
  schema: z.object({
    name: searchable({ language: "english" }),
    handle: z.string(),
    role: z.string(),
  }),
});

/**
 * One assertion a reporter is willing to stand behind.
 *
 * `text` is searchable because two reporters rarely word the same finding
 * identically, and `confidence` is a string rather than a number so the merge
 * surfaces a disagreement as a legible conflict ("confirmed" vs "single-source")
 * instead of quietly averaging it.
 */
const Claim = defineNode("Claim", {
  schema: z.object({
    text: searchable({ language: "english" }),
    predicate: z.string(),
    value: z.string(),
    confidence: z.string(),
  }),
});

/**
 * The published piece. A TERMINAL fact: a story is derived FROM claims and is
 * never itself a premise, so it is absent from `premiseOf.from` below and the
 * schema admits no meaningless edge into a justification.
 */
const Story = defineNode("Story", {
  schema: z.object({
    headline: searchable({ language: "english" }),
    status: z.string(),
  }),
});

/** Why a claim (or a story) follows from its premises — the editorial rule. */
const Justification = defineNode("Justification", {
  schema: z.object({ rule: z.string() }),
});

const premiseOf = defineEdge("premiseOf", { schema: z.object({}) });
const derives = defineEdge("derives", { schema: z.object({}) });
const about = defineEdge("about", { schema: z.object({}) });

export const newsroomGraph = defineGraph({
  id: "newsroom",
  nodes: {
    Wire: { type: Wire },
    Tipster: { type: Tipster },
    Subject: {
      type: Subject,
      unique: [{ name: "subject_handle", fields: ["handle"], scope: "kind", collation: "caseInsensitive" }],
    },
    Claim: { type: Claim },
    Story: { type: Story },
    Justification: { type: Justification },
  },
  edges: {
    // Sources and claims are premises; a story never is.
    premiseOf: { type: premiseOf, from: [Wire, Tipster, Claim], to: [Justification] },
    derives: { type: derives, from: [Justification], to: [Claim, Story] },
    about: { type: about, from: [Claim], to: [Subject] },
  },
});

export type NewsroomGraph = typeof newsroomGraph;

/**
 * Wiring for `@nicia-ai/typegraph/provenance`. The kinds are graph-typed, so a
 * typo here is a compile error rather than a runtime `ConfigurationError`.
 */
export const retractionConfig = {
  source: { kinds: ["Wire", "Tipster"] },
  justification: { kind: "Justification" },
  fact: { kinds: ["Claim", "Story"] },
  premiseOf: { kind: "premiseOf" },
  derives: { kind: "derives" },
} as const;

/**
 * A justification's id is derived from the pair it joins, so a re-delivered
 * event re-derives the same node rather than minting a parallel one. Every id
 * in this demo is content-derived for the same reason: at-least-once delivery
 * means the projector will see this event again.
 */
export function justificationId(premiseId: string, factId: string): string {
  return `${premiseId}>>${factId}`;
}
