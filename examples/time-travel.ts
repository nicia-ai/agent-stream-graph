/**
 * Demo — "why did the agent do that?" Post-hoc debugging of an agent decision
 * by reconstructing the exact belief it held at the moment it acted, built on
 * this package's durable consumer + TypeGraph's recorded-time replay.
 *
 * An ops agent watches a host-health stream and pages on-call based on
 * whatever the belief says right now. Partway through the stream a monitoring
 * glitch reports the host "healthy" — the agent reads that belief and decides
 * NOT to page. Later changes correct the record: the host was never actually
 * healthy at that point, and by the end of the stream it is fully down. The
 * "healthy" belief that justified the agent's decision no longer exists
 * anywhere in the current graph.
 *
 * The payoff: `book.anchorFor(stream, offsetItActedAt)` names the exact
 * recorded instant the agent's decision was made against, and
 * `belief.asOfRecorded(anchor)` reconstructs that belief even though the
 * current graph has moved on. Re-running the SAME decision function against
 * that historical view reproduces the original (wrong, in hindsight) decision
 * — while running it against the current view gives the opposite answer. Both
 * are asserted, not just printed: this demo throws if time travel does not
 * actually reproduce what happened.
 *
 * Run with:  pnpm tsx examples/time-travel.ts
 */
import { asNodeId, defineGraph, defineNode, type NodeId, recordedInstantRevision } from "@nicia-ai/typegraph";
import { z } from "zod";

import {
  checkpointGraph,
  consume,
  type Decoder,
  graphProjector,
  mockShapeSource,
  typeGraphCheckpoints,
  type Projector,
  type ShapeChange,
} from "../src";
import { newStore, runAsMain } from "./_support";

// ============================================================
// The belief graph: one node kind, one field worth debugging
// ============================================================

const Host = defineNode("Host", { schema: z.object({ status: z.string() }) });

const opsGraph = defineGraph({
  id: "ops_host_health",
  nodes: { Host: { type: Host } },
  edges: {},
});

type HostRow = Readonly<{ status: string }>;

// ============================================================
// The stream: a real degradation, a glitch that looks like recovery, then
// the correction that reveals the glitch for what it was.
// ============================================================

const STREAM_NAME = "ops-agent";
const HOST_ID = "db-primary";

/** The offset the agent acted at — recorded up front so the demo can prove it. */
const ACTED_AT_OFFSET = "003";

const CHANGES: readonly ShapeChange<HostRow>[] = [
  { offset: "001", shape: "host", key: HOST_ID, operation: "insert", value: { status: "healthy" } },
  { offset: "002", shape: "host", key: HOST_ID, operation: "update", value: { status: "degraded" } },
  // A monitoring glitch reports recovery. This is the belief the agent acts on.
  { offset: "003", shape: "host", key: HOST_ID, operation: "update", value: { status: "healthy" } },
  { offset: "004", shape: "host", key: HOST_ID, operation: "update", value: { status: "degraded" } },
  // The correction: the host never recovered — it was degrading toward outage
  // the whole time offset 003 called it "healthy".
  { offset: "005", shape: "host", key: HOST_ID, operation: "update", value: { status: "down" } },
];

// ============================================================
// Idempotent projection — a pure decoder, unit-testable without a store
// ============================================================

const decode: Decoder<typeof opsGraph, HostRow> = (change, g) => {
  if (change.operation === "delete") return [g.nodes.Host.remove(change.key)];
  return [g.nodes.Host.upsert(change.key, { status: change.value.status })];
};

const project: Projector<typeof opsGraph, HostRow> = graphProjector(opsGraph, decode);

// ============================================================
// The agent's on-call policy — the decision under debugging
// ============================================================

type Decision = "NO_ACTION" | "PAGE_ON_CALL";

/**
 * Anything that can point-read a `Host` by id — a live belief store, or a
 * `RecordedStoreView` pinned to a past recorded instant. Naming just this
 * much of the surface is what lets `decideAction`/`statusOf` run unmodified
 * against either.
 */
type HostReader = { nodes: { Host: { getById: (id: NodeId<typeof Host>) => Promise<{ status: string } | undefined> } } };

/**
 * Reads the host's belief from `view` and decides whether to page. This is
 * the whole point of the demo: it is a plain function of a view, so handing
 * it a historical view (instead of the live store) replays the exact decision
 * that view would have produced — no separate "explain" code path to
 * maintain, no risk of the explanation drifting from what actually ran.
 */
async function decideAction(view: HostReader, hostId: string): Promise<Decision> {
  const host = await view.nodes.Host.getById(asNodeId(hostId));
  return host?.status === "healthy" ? "NO_ACTION" : "PAGE_ON_CALL";
}

async function statusOf(view: HostReader, hostId: string): Promise<string> {
  const host = await view.nodes.Host.getById(asNodeId(hostId));
  return host?.status ?? "(unknown)";
}

// ============================================================
// Main
// ============================================================

const RULE = "━".repeat(74);
function section(title: string): void {
  console.log("\n" + RULE);
  console.log(` ${title}`);
  console.log(RULE);
}

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" Time travel — reconstructing the belief behind a past agent decision");
  console.log(RULE);

  const cursor = await newStore(checkpointGraph);
  const book = typeGraphCheckpoints(cursor);
  const belief = await newStore(opsGraph, true);
  const stores = [belief, cursor];

  try {
    const source = mockShapeSource(STREAM_NAME, CHANGES);

    // ----------------------------------------------------------
    // (a) The agent consumes the stream up to the moment it acts, and acts.
    // ----------------------------------------------------------
    section("(a) The agent consumes the stream and takes an action");

    const untilAction = await consume({ source, store: belief, checkpoints: book, project, stopAfter: 3 });
    const statusWhenActed = await statusOf(belief, HOST_ID);
    const decisionTaken = await decideAction(belief, HOST_ID);
    console.log(`\n  consumed ${untilAction.processed} changes, stopped at offset ${untilAction.lastOffset}`);
    console.log(`  belief at that moment: ${HOST_ID} is "${statusWhenActed}"`);
    console.log(`  >>> ACTION TAKEN: ${decisionTaken} <<<`);

    // ----------------------------------------------------------
    // (b) The rest of the stream arrives and corrects the record.
    // ----------------------------------------------------------
    section("(b) The stream continues — the record is corrected");

    await consume({ source, store: belief, checkpoints: book, project });
    const currentStatus = await statusOf(belief, HOST_ID);
    const decisionNow = await decideAction(belief, HOST_ID);
    console.log(`\n  current belief: ${HOST_ID} is "${currentStatus}"`);
    console.log(`  current graph queried for the offset-003 belief ("${statusWhenActed}"): not there — it is`);
    console.log(`  "${currentStatus}" now. The evidence behind the original decision is gone.`);
    console.log(`  a decision made against belief TODAY would be: ${decisionNow}`);

    if (currentStatus === statusWhenActed) {
      throw new Error(
        "demo setup is broken: the stream never actually corrected the record — " +
          `status is still "${statusWhenActed}"`,
      );
    }
    if (decisionNow === decisionTaken) {
      throw new Error(
        `demo setup is broken: today's decision (${decisionNow}) should differ from the original ` +
          `decision (${decisionTaken}) — otherwise time travel proves nothing`,
      );
    }

    // ----------------------------------------------------------
    // (c) Time travel: reconstruct the belief the agent actually acted on.
    // ----------------------------------------------------------
    section("(c) Time travel — reconstruct the belief as of the acted-on offset");

    const anchor = await book.anchorFor(STREAM_NAME, ACTED_AT_OFFSET);
    if (anchor === undefined) {
      throw new Error(`no checkpoint anchor recorded for ${STREAM_NAME}@${ACTED_AT_OFFSET}`);
    }
    const pastView = belief.asOfRecorded(anchor);
    const reconstructedStatus = await statusOf(pastView, HOST_ID);
    const reconstructedDecision = await decideAction(pastView, HOST_ID);
    console.log(`\n  book.anchorFor("${STREAM_NAME}", "${ACTED_AT_OFFSET}") -> revision ${recordedInstantRevision(anchor)}`);
    console.log(`  belief.asOfRecorded(anchor): ${HOST_ID} is "${reconstructedStatus}"`);
    console.log(`  re-running decideAction() against that historical view: ${reconstructedDecision}`);

    if (reconstructedStatus !== statusWhenActed || reconstructedDecision !== decisionTaken) {
      throw new Error(
        `time travel did not reproduce the original decision: reconstructed ` +
          `(status="${reconstructedStatus}", decision=${reconstructedDecision}) vs. actually acted on ` +
          `(status="${statusWhenActed}", decision=${decisionTaken})`,
      );
    }
    console.log(`\n  >>> REPRODUCED: the agent's decision (${decisionTaken}) is explained by belief that no`);
    console.log(`      longer exists anywhere except through time travel. <<<`);

    // ----------------------------------------------------------
    // (d) The scrubber: what did belief say at every offset?
    // ----------------------------------------------------------
    section("(d) Per-offset timeline — scrubbing through every anchor");
    // Under `mockShapeSource` every change carries its own offset, so this is
    // per-CHANGE time travel. Under a real Electric shape a whole catch-up
    // batch shares one offset — `durableStreamSource` cannot give finer
    // granularity than the source's own offsets, so a live deployment scrubs
    // by batch, not by row. That is a documented limitation, not a bug.
    console.log();
    for (const change of CHANGES) {
      const offsetAnchor = await book.anchorFor(STREAM_NAME, change.offset);
      const status = offsetAnchor === undefined ? "(not checkpointed)" : await statusOf(belief.asOfRecorded(offsetAnchor), HOST_ID);
      const revision = offsetAnchor === undefined ? "—" : recordedInstantRevision(offsetAnchor);
      const marker = change.offset === ACTED_AT_OFFSET ? "  <-- agent acted here" : "";
      console.log(`    @${change.offset} (rev ${revision}) belief: ${HOST_ID} = "${status}"${marker}`);
    }

    console.log("\n" + RULE);
    console.log(" A decision is only as explainable as the belief it was made against is");
    console.log(" recoverable. Recorded-time anchors make that recovery exact.");
    console.log(RULE + "\n");
  } finally {
    await Promise.allSettled(stores.map((store) => store.close()));
  }
}

runAsMain(import.meta.url, main);
