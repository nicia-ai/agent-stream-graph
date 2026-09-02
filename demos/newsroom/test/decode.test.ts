/**
 * Unit tests for `src/decode.ts` — pure, no store, no I/O. Every case here
 * checks the EXACT `GraphEvent[]` a reporter event decodes to, because
 * `decode.ts`'s whole reason to exist is that a projector built on top of it
 * (`graphProjector`) trusts these events to be right without a database in
 * the loop to catch a mistake.
 */
import { graphEmitter, OP_EDGE_UPSERT, OP_NODE_UPSERT, type ShapeChange } from "@nicia-ai/agent-stream-graph";
import { describe, expect, it } from "vitest";

import { decodeReporterEvent, type ClaimEvent, type ReporterEvent, type StoryEvent } from "../src/decode.js";
import { justificationId, newsroomGraph } from "../src/graph.js";

const emit = graphEmitter(newsroomGraph);

function change(value: ReporterEvent): ShapeChange<ReporterEvent> {
  return { offset: "001", shape: "reporter-event", key: value.id, operation: "insert", value };
}

const VANCE = { id: "subject-vance", name: "M. Vance", handle: "@mvance", role: "procurement chief" } as const;

describe("decodeReporterEvent", () => {
  it("decodes a wire event to a single Wire upsert", () => {
    const events = decodeReporterEvent(change({ type: "wire", id: "wire:filing", label: "Q1 filing", outlet: "City Clerk" }), emit);
    expect(events).toEqual([emit.nodes.Wire.upsert("wire:filing", { label: "Q1 filing", outlet: "City Clerk" })]);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ op: OP_NODE_UPSERT, kind: "Wire", id: "wire:filing" });
    // No validity was stated — the event must not carry a `validFrom` key at all.
    expect(events[0]).not.toHaveProperty("validFrom");
  });

  it("decodes a tip event to a single Tipster upsert", () => {
    const events = decodeReporterEvent(change({ type: "tip", id: "tip:insider-01", label: "Anonymous tip", handle: "insider-01" }), emit);
    expect(events).toEqual([emit.nodes.Tipster.upsert("tip:insider-01", { label: "Anonymous tip", handle: "insider-01" })]);
  });

  it("decodes a single-source claim to Subject + Claim + about + one justification scaffold", () => {
    const claim: ClaimEvent = {
      type: "claim",
      id: "claim-1",
      text: "The award went to Halcyon",
      predicate: "awardedContract",
      value: "fleet-modernisation",
      confidence: "confirmed",
      subject: VANCE,
      sources: ["wire:filing"],
      rule: "primary document",
      validFrom: "2026-03-01T00:00:00.000Z",
    };
    const events = decodeReporterEvent(change(claim), emit);
    expect(events).toHaveLength(6);

    const [subjectEvent, claimEvent, aboutEvent, justificationEvent, premiseOfEvent, derivesEvent] = events;
    expect(subjectEvent).toMatchObject({ op: OP_NODE_UPSERT, kind: "Subject", id: "subject-vance" });
    expect(claimEvent).toMatchObject({ op: OP_NODE_UPSERT, kind: "Claim", id: "claim-1", validFrom: claim.validFrom });
    expect(aboutEvent).toMatchObject({
      op: OP_EDGE_UPSERT,
      kind: "about",
      from: { kind: "Claim", id: "claim-1" },
      to: { kind: "Subject", id: "subject-vance" },
    });

    const jId = justificationId("wire:filing", "claim-1");
    expect(justificationEvent).toMatchObject({ op: OP_NODE_UPSERT, kind: "Justification", id: jId, props: { rule: claim.rule } });
    expect(premiseOfEvent).toMatchObject({
      op: OP_EDGE_UPSERT,
      kind: "premiseOf",
      from: { kind: "Wire", id: "wire:filing" },
      to: { kind: "Justification", id: jId },
    });
    expect(derivesEvent).toMatchObject({
      op: OP_EDGE_UPSERT,
      kind: "derives",
      from: { kind: "Justification", id: jId },
      to: { kind: "Claim", id: "claim-1" },
    });
  });

  it("gives a claim with TWO sources its own justification per source — the retraction asymmetry's foundation", () => {
    const claim: ClaimEvent = {
      type: "claim",
      id: "claim-award-ash",
      text: "Halcyon was awarded the contract",
      predicate: "awardedContract",
      value: "fleet-modernisation",
      confidence: "confirmed",
      subject: { id: "subject-halcyon", name: "Halcyon Transit Systems", handle: "@halcyon", role: "vendor" },
      sources: ["wire:filing", "tip:insider-01"],
      rule: "two independent sources",
      validFrom: "2026-03-01T00:00:00.000Z",
    };
    const events = decodeReporterEvent(change(claim), emit);
    // Subject + Claim + about (3) + two justification scaffolds (3 each) = 9.
    expect(events).toHaveLength(9);

    const wireJustification = justificationId("wire:filing", "claim-award-ash");
    const tipJustification = justificationId("tip:insider-01", "claim-award-ash");
    expect(wireJustification).not.toBe(tipJustification);

    const premiseOfEdges = events.filter((event) => "kind" in event && event.kind === "premiseOf");
    expect(premiseOfEdges).toHaveLength(2);
    // The premise kind is derived from the id prefix — "tip:" is a Tipster, everything else a Wire.
    expect(premiseOfEdges).toContainEqual(
      expect.objectContaining({ from: { kind: "Wire", id: "wire:filing" }, to: { kind: "Justification", id: wireJustification } }),
    );
    expect(premiseOfEdges).toContainEqual(
      expect.objectContaining({ from: { kind: "Tipster", id: "tip:insider-01" }, to: { kind: "Justification", id: tipJustification } }),
    );
  });

  it("decodes a story to a Story upsert plus one justification scaffold per claim", () => {
    const story: StoryEvent = {
      type: "story",
      id: "story-1",
      headline: "Contract awarded",
      status: "draft",
      claims: ["claim-1", "claim-2"],
      rule: "draft pending corroboration",
      validFrom: "2026-03-01T00:00:00.000Z",
    };
    const events = decodeReporterEvent(change(story), emit);
    // Story (1) + two justification scaffolds (3 each) = 7.
    expect(events).toHaveLength(7);
    expect(events[0]).toMatchObject({ op: OP_NODE_UPSERT, kind: "Story", id: "story-1" });
    const derivesEdges = events.filter((event) => "kind" in event && event.kind === "derives");
    expect(derivesEdges.map((edge) => ("to" in edge ? edge.to.id : undefined))).toEqual(["story-1", "story-1"]);
  });

  it("omits validTo entirely when the event does not state one (exactOptionalPropertyTypes-safe)", () => {
    const claim: ClaimEvent = {
      type: "claim",
      id: "claim-open",
      text: "still true",
      predicate: "p",
      value: "v",
      confidence: "confirmed",
      subject: VANCE,
      sources: ["wire:filing"],
      rule: "r",
      validFrom: "2026-01-01T00:00:00.000Z",
    };
    const [, claimEvent] = decodeReporterEvent(change(claim), emit);
    expect(claimEvent).not.toHaveProperty("validTo");
  });

  it("carries validTo through when the event states one", () => {
    const claim: ClaimEvent = {
      type: "claim",
      id: "claim-closed",
      text: "was true",
      predicate: "p",
      value: "v",
      confidence: "confirmed",
      subject: VANCE,
      sources: ["wire:filing"],
      rule: "r",
      validFrom: "2026-01-01T00:00:00.000Z",
      validTo: "2026-02-01T00:00:00.000Z",
    };
    const [, claimEvent] = decodeReporterEvent(change(claim), emit);
    expect(claimEvent).toMatchObject({ validFrom: claim.validFrom, validTo: claim.validTo });
  });

  it("is pure — the same input decodes to a deep-equal result every time", () => {
    const claim: ClaimEvent = {
      type: "claim",
      id: "claim-repeat",
      text: "text",
      predicate: "p",
      value: "v",
      confidence: "confirmed",
      subject: VANCE,
      sources: ["wire:filing", "tip:insider-01"],
      rule: "r",
      validFrom: "2026-01-01T00:00:00.000Z",
    };
    const first = decodeReporterEvent(change(claim), emit);
    const second = decodeReporterEvent(change(claim), emit);
    expect(first).toEqual(second);
  });
});
