/**
 * Demo — one worker, N agent streams, pull-wake: let the server say who has
 * work.
 *
 * The other demos point a consumer at streams they already know. Operating a
 * fleet is a different problem: with a dozen (or a thousand) agent streams,
 * polling each one on a timer wastes most of every sweep on streams that have
 * nothing new. `ensureSubscription` / `consumeSubscribed` invert that — the
 * server tracks which linked streams have pending work, and a materializer
 * worker drains only those.
 *
 * The story:
 *   1. A fleet of a dozen agent streams; only a few carry work.
 *   2. `ensureSubscription` is idempotent — re-running it confirms, not
 *      recreates.
 *   3. One worker drains exactly the streams the server reports pending. The
 *      idle ones are never even opened.
 *   4. `nextWake` drives a loop instead of a timer: new work lands on a
 *      couple of streams, the worker wakes again, and drains only those.
 *   5. The operational payoff, proved rather than asserted in prose:
 *        - a projector that throws releases the lease immediately, so a
 *          second worker picks the work up in milliseconds, not
 *          `leaseTtlMs`;
 *        - a lease already held by another worker is `SubscriptionClaimedError`;
 *        - an ack/release under a claim the server has moved past is
 *          `SubscriptionFencedError`.
 *
 * Runs against the in-process Durable Streams stand-in the tests use — see
 * the closing note for exactly what that does and does not prove.
 *
 * Run with:  pnpm tsx examples/swarm-materializer.ts
 */
import { defineGraph, defineNode } from "@nicia-ai/typegraph";
import { z } from "zod";

import {
  ackSubscription,
  checkpointGraph,
  claimSubscription,
  consumeSubscribed,
  durableStreamSource,
  ensureSubscription,
  releaseSubscription,
  SubscriptionClaimedError,
  SubscriptionFencedError,
  typeGraphCheckpoints,
  type PendingStream,
  type Projector,
} from "../src";
import { startDurableStreamsServer } from "../test/support/durable-streams-server";
import { assertEqual, expectRejection, newStore, RULE, runAsMain, section } from "./_support";

// ============================================================
// The fleet
// ============================================================

const AGENT_COUNT = 12;
const AGENT_PATHS: readonly string[] = Array.from(
  { length: AGENT_COUNT },
  (_, i) => `agents/agent-${String(i + 1).padStart(2, "0")}`,
);

/** Streams that already have work when the fleet comes up. */
const BUSY_AT_START: readonly string[] = ["agents/agent-03", "agents/agent-07", "agents/agent-11"];
/** Streams that get NEW work after the first drain — what `nextWake` is for. */
const WAKES_LATER: readonly string[] = ["agents/agent-01", "agents/agent-08"];
/** Held back for the failure-mode demos, so its lease events are isolated. */
const POISON_STREAM = "agents/agent-12";

const SUBSCRIPTION_ID = "swarm-materializer";
// One glob, not a list: the fleet is named by shape, so a stream created after
// the subscription still wakes the materializer. `*` matches one path segment.
const FLEET_PATTERN = "agents/*";
// Deliberately generous: long enough that "picked up almost immediately" and
// "picked up because the lease finally expired" cannot be confused.
const LEASE_TTL_MS = 30_000;
// The bound the handoff assertion checks elapsed time against — comfortably
// below LEASE_TTL_MS for an in-process run, with no dependence on timing luck.
const HANDOFF_BOUND_MS = 5_000;

// ============================================================
// Belief graph — one node per work item a stream reports
// ============================================================

const WorkItem = defineNode("WorkItem", {
  schema: z.object({ agent: z.string(), note: z.string() }),
});
const swarmGraph = defineGraph({
  id: "swarm_materializer_belief",
  nodes: { WorkItem: { type: WorkItem } },
  edges: {},
});
type WorkMessage = Readonly<{ key: string; agent: string; note: string }>;

const project: Projector<typeof swarmGraph, WorkMessage> = async (tx, change) => {
  await tx.nodes.WorkItem.upsertById(change.key, { agent: change.value.agent, note: change.value.note });
};

class PoisonedMessageError extends Error {
  constructor(stream: string) {
    super(`projector poisoned by a message on ${stream}`);
    this.name = "PoisonedMessageError";
  }
}

const poisonedProject: Projector<typeof swarmGraph, WorkMessage> = async (_tx, change) => {
  throw new PoisonedMessageError(change.value.agent);
};

// ============================================================
// Reporting helpers
// ============================================================

function sortedSet(paths: Iterable<string>): string[] {
  return [...new Set(paths)].sort();
}

// ============================================================
// The demo
// ============================================================

async function main(): Promise<void> {
  const server = await startDurableStreamsServer();
  // Every subscription call below targets this one fleet-wide subscription.
  const subscription = { rootUrl: server.url, id: SUBSCRIPTION_ID };
  const opened: { close: () => Promise<void> }[] = [];

  // Every stream the latest drain opened a source for. This is what proves an
  // idle stream was never even READ, not merely absent from the result.
  const openedByLastDrain = new Set<string>();
  const sourceFor = (stream: PendingStream) => {
    openedByLastDrain.add(stream.path);
    return durableStreamSource<WorkMessage, WorkMessage>({
      url: server.streamUrl(stream.path),
      name: stream.path,
      toChange: (item) => ({ shape: "work", key: item.key, operation: "insert", value: item }),
    });
  };

  try {
    console.log(RULE);
    console.log(" Pull-wake subscriptions — one worker, a dozen agent streams");
    console.log(RULE);

    section("1. FLEET — a dozen agent streams, work on three of them");
    for (const path of AGENT_PATHS) server.createStream(path);
    for (const path of BUSY_AT_START) {
      server.append(path, [
        { key: `${path}/w1`, agent: path, note: "first item" },
        { key: `${path}/w2`, agent: path, note: "second item" },
      ]);
    }
    console.log(`  streams:     ${AGENT_PATHS.length}`);
    console.log(`  with work:   ${BUSY_AT_START.join(", ")}`);
    console.log(`  idle:        ${AGENT_PATHS.filter((path) => !BUSY_AT_START.includes(path)).length} streams`);

    const cursorStore = await newStore(checkpointGraph, false);
    const belief = await newStore(swarmGraph, true);
    opened.push(cursorStore, belief);
    const checkpoints = typeGraphCheckpoints(cursorStore);
    const drain = (worker: string, projector = project) => {
      openedByLastDrain.clear();
      return consumeSubscribed({ subscription, worker, sourceFor, store: belief, checkpoints, project: projector });
    };

    section("2. SUBSCRIBE — link the whole fleet, confirm re-running is a no-op");
    const fleetSubscription = { ...subscription, pattern: FLEET_PATTERN, leaseTtlMs: LEASE_TTL_MS };
    const first = await ensureSubscription(fleetSubscription);
    const second = await ensureSubscription(fleetSubscription);
    console.log(`  first call:  created=${first.created}`);
    console.log(`  second call: created=${second.created}  (idempotent re-confirmation)`);
    assertEqual(first.created, true, "ensureSubscription first call created");
    assertEqual(second.created, false, "ensureSubscription second call created");

    section("3. DRAIN — the worker reads only what the server says is pending");
    const round1 = await drain("materializer-1");
    const drained1 = round1.streams.map((s) => s.path);
    const skipped1 = AGENT_PATHS.filter((path) => !drained1.includes(path));
    for (const s of round1.streams) console.log(`  drained  ${s.path}  (${s.processed} items)`);
    console.log(`  skipped  ${skipped1.length} idle streams: ${skipped1.join(", ")}`);

    assertEqual(sortedSet(drained1), sortedSet(BUSY_AT_START), "round 1 drained streams");
    assertEqual(sortedSet(openedByLastDrain), sortedSet(BUSY_AT_START), "round 1 streams actually opened");
    assertEqual(round1.nextWake, false, "round 1 nextWake");
    console.log(`  nextWake: ${round1.nextWake}  (no work left queued)`);

    section("4. WAKE AGAIN — new work lands, only those streams get drained");
    for (const path of WAKES_LATER) {
      server.append(path, [{ key: `${path}/w1`, agent: path, note: "arrived after round 1" }]);
    }
    console.log(`  new work on: ${WAKES_LATER.join(", ")}`);

    const round2 = await drain("materializer-1");
    const drained2 = round2.streams.map((s) => s.path);
    for (const s of round2.streams) console.log(`  drained  ${s.path}  (${s.processed} items)`);

    assertEqual(sortedSet(drained2), sortedSet(WAKES_LATER), "round 2 drained streams");
    assertEqual(sortedSet(openedByLastDrain), sortedSet(WAKES_LATER), "round 2 streams actually opened");
    console.log("  → the already-drained streams from round 1 were not re-read");

    section("5a. LEASE HANDOFF — a poisoned message releases the lease, not holds it");
    server.append(POISON_STREAM, [{ key: `${POISON_STREAM}/poison`, agent: POISON_STREAM, note: "BOOM" }]);

    const before = Date.now();
    const poisoned = await expectRejection(drain("materializer-1", poisonedProject), PoisonedMessageError);
    console.log(`  worker-1 failed: ${poisoned.message}`);

    // The claim was released on the throw, not held for LEASE_TTL_MS — a
    // second worker can claim it right away and finish the work properly.
    const recovered = await drain("materializer-2");
    const elapsedMs = Date.now() - before;
    console.log(`  worker-2 claimed and finished ${POISON_STREAM} after ${elapsedMs}ms`);
    console.log(`  lease TTL is ${LEASE_TTL_MS}ms — handoff bound checked against ${HANDOFF_BOUND_MS}ms`);

    assertEqual(recovered.streams.map((s) => s.path), [POISON_STREAM], "recovery drain target");
    if (elapsedMs >= HANDOFF_BOUND_MS) {
      throw new Error(
        `lease handoff took ${elapsedMs}ms, expected well under ${HANDOFF_BOUND_MS}ms (TTL is ${LEASE_TTL_MS}ms) — ` +
          "the throw may be holding the claim instead of releasing it",
      );
    }

    section("5b. CONTENTION & FENCING — two ways a stale or busy claim is refused");

    // A lease another worker currently holds: SubscriptionClaimedError.
    const held = await claimSubscription(subscription, "materializer-3");
    const blocked = await expectRejection(claimSubscription(subscription, "materializer-4"), SubscriptionClaimedError);
    console.log(`  worker-4 blocked: ${blocked.message}`);
    await releaseSubscription(subscription, held);

    // A claim the server has already moved past: SubscriptionFencedError.
    const stale = await claimSubscription(subscription, "materializer-5");
    await releaseSubscription(subscription, stale);
    const current = await claimSubscription(subscription, "materializer-6");
    const fenced = await expectRejection(ackSubscription(subscription, stale, [], true), SubscriptionFencedError);
    console.log(`  worker-5's stale claim fenced: ${fenced.message}`);
    await releaseSubscription(subscription, current);

    section("What this demo does and does not prove");
    console.log("  Proved: pull-wake lets one worker cover a fleet without polling idle");
    console.log("  streams; ensureSubscription is idempotent; a throw inside consumeSubscribed");
    console.log("  releases the lease for immediate handoff instead of sitting on it; a busy");
    console.log("  lease is SubscriptionClaimedError; a stale one is SubscriptionFencedError.");
    console.log("");
    console.log("  NOT proved: this ran against the in-process Durable Streams stand-in this");
    console.log("  package's tests use — a faithful implementation of the protocol slice this");
    console.log("  library speaks, not a hosted server. As the README's Limitations section");
    console.log("  states, subscription support is written to the protocol spec and has not");
    console.log("  been exercised against a real deployment's auth, retention, or CDN behaviour.");
  } finally {
    await Promise.allSettled(opened.map((store) => store.close()));
    await server.close();
  }
}

runAsMain(import.meta.url, main);
