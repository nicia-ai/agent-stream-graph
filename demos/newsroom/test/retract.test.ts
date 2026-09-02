/**
 * Integration test for the retraction asymmetry — the argument the whole
 * package exists to make: a claim grounded in TWO independent sources
 * survives a retraction that kills a claim grounded in only one, because
 * `decode.ts` gives every premise its own `Justification` rather than one
 * AND-justification per claim.
 */
import { graphProjector, type ShapeChange } from "@nicia-ai/agent-stream-graph";
import { describe, expect, it } from "vitest";

import { TRANSCRIPTS } from "../fixtures/dispatches.js";
import { newStore } from "../src/backend.js";
import { decodeReporterEvent, type ReporterEvent } from "../src/decode.js";
import { retractBurnedSource } from "../src/desk/retract.js";
import { newsroomGraph } from "../src/graph.js";

const project = graphProjector(newsroomGraph, decodeReporterEvent);

async function seed(reporterIds: readonly ("reporter-ash" | "reporter-cass")[]) {
  const store = await newStore(newsroomGraph, true);
  await store.transaction(async (tx) => {
    for (const reporterId of reporterIds) {
      for (const event of TRANSCRIPTS[reporterId]) {
        const change: ShapeChange<ReporterEvent> = { offset: reporterId, shape: "reporter-event", key: event.id, operation: "insert", value: event };
        await project(tx, change);
      }
    }
  });
  return store;
}

describe("retractBurnedSource", () => {
  it("kills cass's tip-only claim and its story, but leaves ash's independently-grounded claim standing", async () => {
    const canonical = await seed(["reporter-ash", "reporter-cass"]);
    try {
      const outcome = await retractBurnedSource(canonical);

      expect(outcome.before.survivingClaim).toBe("fleet-modernisation");
      expect(outcome.before.dyingClaim).toBe("undisclosed-payment");
      expect(outcome.before.dyingStory).toBe("Questions over transit contract award");

      // The asymmetry: ash's claim (two sources) survives; cass's (one source) does not.
      expect(outcome.after.survivingClaim).toBe("fleet-modernisation");
      expect(outcome.after.dyingClaim).toBeUndefined();
      expect(outcome.after.dyingStory).toBeUndefined();

      expect(outcome.report.died.map((ref) => `${ref.kind}/${ref.id}`).sort()).toEqual([
        "Claim/claim-kickback",
        "Story/story-kickback",
      ]);
      expect(outcome.report.died.some((ref) => ref.id === "claim-award-ash")).toBe(false);
      expect(outcome.report.survivedVia.some((entry) => entry.fact.id === "claim-award-ash")).toBe(true);
    } finally {
      await canonical.close();
    }
  });

  it("throws rather than reporting a false survival when the 'surviving' claim never existed", async () => {
    // No ash events at all: claim-award-ash never exists, so it reads as
    // non-current even BEFORE retraction. retractBurnedSource must refuse to
    // call that a survival rather than print a misleading before/after.
    const canonical = await seed(["reporter-cass"]);
    try {
      await expect(retractBurnedSource(canonical)).rejects.toThrow(/independently grounded/);
    } finally {
      await canonical.close();
    }
  });
});
