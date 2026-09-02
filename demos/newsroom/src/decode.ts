/**
 * Reporter event → graph mutations.
 *
 * PURE: no store, no I/O, not async. A reporter's contribution is therefore
 * testable without a database, and — more importantly — replayable. A decoder
 * that could read the store could make its output depend on WHEN it ran, which
 * is exactly what breaks a replay.
 *
 * One rule governs the shape of everything below: **decode both endpoints of an
 * edge from the same change.** `applyGraphEvents` orders nodes before edges
 * within one batch, but `Projector` runs per change, so an edge whose endpoints
 * arrive on a later change fails endpoint validation. Every event type here
 * therefore carries everything its edges need.
 *
 * The second rule is that every id is CONTENT-DERIVED. At-least-once delivery
 * means the projector will see each of these events again; deriving ids from
 * their content is what makes the second delivery land on the row the first one
 * wrote instead of minting a parallel one.
 */
import type { Decoder, GraphEmitter, GraphEvent } from "@nicia-ai/agent-stream-graph";

import { justificationId, type NewsroomGraph } from "./graph.js";

/** The subject a claim is about, carried inline so the `about` edge can be decoded here. */
export type SubjectRef = Readonly<{
  id: string;
  name: string;
  handle: string;
  role: string;
}>;

/** A source the reporter read. Two kinds, discriminated — see `graph.ts`. */
export type SourceEvent =
  | Readonly<{ type: "wire"; id: string; label: string; outlet: string }>
  | Readonly<{ type: "tip"; id: string; label: string; handle: string }>;

/**
 * A claim the reporter is willing to stand behind.
 *
 * `validFrom` is REQUIRED, not optional, and that is a deliberate piece of API
 * design rather than an oversight. A row created with no stated lower bound
 * takes the INGEST instant as its start, so a later event closing the window at
 * a past instant describes a fact that stopped being true before it started and
 * is refused with `INVERTED_VALIDITY_WINDOW`. A stream that carries event time
 * has to carry it on EVERY event, so the type insists.
 */
export type ClaimEvent = Readonly<{
  type: "claim";
  id: string;
  text: string;
  predicate: string;
  value: string;
  confidence: string;
  subject: SubjectRef;
  /** Source ids this claim rests on. Each becomes its own justification. */
  sources: readonly string[];
  rule: string;
  validFrom: string;
  /** Set when the reporter later learns the claim stopped holding. */
  validTo?: string;
}>;

/** The filed piece: a terminal fact derived from claims. */
export type StoryEvent = Readonly<{
  type: "story";
  id: string;
  headline: string;
  status: string;
  claims: readonly string[];
  rule: string;
  validFrom: string;
}>;

export type ReporterEvent = SourceEvent | ClaimEvent | StoryEvent;

/**
 * Build the `premiseOf` → `Justification` → `derives` scaffold joining one
 * premise to one fact.
 *
 * Each premise gets its OWN justification, which is what makes retraction
 * discriminating: a claim with two independent sources has two justifications,
 * so retracting one source leaves the other standing and the claim survives.
 * Collapsing them into a single AND-justification would make every claim only
 * as strong as its weakest source.
 */
function ground(
  emit: GraphEmitter<NewsroomGraph>,
  premise: { kind: "Wire" | "Tipster" | "Claim"; id: string },
  fact: { kind: "Claim" | "Story"; id: string },
  rule: string,
  validFrom: string,
): readonly GraphEvent<NewsroomGraph>[] {
  const id = justificationId(premise.id, fact.id);
  return [
    emit.nodes.Justification.upsert(id, { rule }, { validFrom }),
    emit.edges.premiseOf.upsert(premise, { kind: "Justification", id }, undefined, { validFrom }),
    emit.edges.derives.upsert({ kind: "Justification", id }, fact, undefined, { validFrom }),
  ];
}

/**
 * The decoder `graphProjector` adapts into a `Projector`.
 *
 * Kind comes from the object path — `emit.nodes.Subject` — never from a string,
 * so a typo is a compile error naming the kinds that do exist rather than an
 * unresolved constraint blob at runtime.
 */
export const decodeReporterEvent: Decoder<NewsroomGraph, ReporterEvent> = (change, emit) => {
  const event = change.value;

  // A retracted source is a WRITE to the source row, not a removal: the
  // provenance capability reads the flag and decides what falls with it.
  if (event.type === "wire") {
    return [emit.nodes.Wire.upsert(event.id, { label: event.label, outlet: event.outlet })];
  }
  if (event.type === "tip") {
    return [emit.nodes.Tipster.upsert(event.id, { label: event.label, handle: event.handle })];
  }

  if (event.type === "claim") {
    const validity =
      event.validTo === undefined ? { validFrom: event.validFrom } : { validFrom: event.validFrom, validTo: event.validTo };
    return [
      emit.nodes.Subject.upsert(
        event.subject.id,
        { name: event.subject.name, handle: event.subject.handle, role: event.subject.role },
        { validFrom: event.validFrom },
      ),
      emit.nodes.Claim.upsert(
        event.id,
        { text: event.text, predicate: event.predicate, value: event.value, confidence: event.confidence },
        validity,
      ),
      emit.edges.about.upsert({ kind: "Claim", id: event.id }, { kind: "Subject", id: event.subject.id }, undefined, {
        validFrom: event.validFrom,
      }),
      ...event.sources.flatMap((sourceId) =>
        // The premise kind is not knowable from the id alone, so the scaffold is
        // built against the kind the source event already established. Wire and
        // Tipster ids are namespaced by their emitter (see `fixtures/`).
        ground(
          emit,
          { kind: sourceId.startsWith("tip:") ? "Tipster" : "Wire", id: sourceId },
          { kind: "Claim", id: event.id },
          event.rule,
          event.validFrom,
        ),
      ),
    ];
  }

  return [
    emit.nodes.Story.upsert(event.id, { headline: event.headline, status: event.status }, { validFrom: event.validFrom }),
    ...event.claims.flatMap((claimId) =>
      ground(emit, { kind: "Claim", id: claimId }, { kind: "Story", id: event.id }, event.rule, event.validFrom),
    ),
  ];
};
