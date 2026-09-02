/**
 * Demo — one worker, N agent streams, pull-wake: let the server say who has
 * work.
 *
 * Every other demo in this directory points a consumer at ONE known stream.
 * Operating a fleet is a different problem: with a dozen (or a thousand)
 * agent streams, polling each one on a timer means most of every sweep is
 * wasted asking streams that have nothing new. `ensureSubscription` /
 * `consumeSubscribed` invert that — the server tracks which linked streams
 * have pending work, and a materializer worker drains only those.
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
  type CheckpointBook,
  type PendingStream,
  type Projector,
} from "../src";
import { startDurableStreamsServer } from "../test/support/durable-streams-server";
import { type DemoHistoryStore, newStore, runAsMain } from "./_support";

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
type SwarmStore = DemoHistoryStore<typeof swarmGraph>;

type WorkMessage = Readonly<{ key: string; agent: string; note: string }>;

const project: Projector<typeof swarmGraph, WorkMessage> = async (tx, change) => {
  await tx.nodes.WorkItem.upsertById(change.key, { agent: change.value.agent, note: change.value.note });
};

// ============================================================
// Reporting helpers
// ============================================================

const rule = "━".repeat(74);
function banner(label: string): void {
  console.log("\n" + rule);
  console.log(` ${label}`);
  console.log(rule);
}

function assertEqual<T>(actual: T, expected: T, what: string): void {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Error(`${what}: expected ${e}, got ${a}`);
}

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

  // Every stream the worker opens a source for, this round — reset before
  // each `consumeSubscribed` call. This is what proves an idle stream was
  // never even READ, not merely that it was absent from the result.
  const readThisRound = new Set<string>();
  const sourceFor = (stream: PendingStream) => {
    readThisRound.add(stream.path);
    return durableStreamSource<WorkMessage, WorkMessage>({
      url: server.streamUrl(stream.path),
      name: stream.path,
      toChange: (item) => ({ shape: "work", key: item.key, operation: "insert", value: item }),
    });
  };

  try {
    console.log(rule);
    console.log(" Pull-wake subscriptions — one worker, a dozen agent streams");
    console.log(rule);

    // ---- 1. Stand up the fleet; only a few streams carry work ----
    banner("1. FLEET — a dozen agent streams, work on three of them");
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
    const checkpoints: CheckpointBook = typeGraphCheckpoints(cursorStore);
    const belief: SwarmStore = await newStore(swarmGraph, true);
    opened.push(cursorStore, belief);

    // ---- 2. ensureSubscription is idempotent ----
    banner("2. SUBSCRIBE — link the whole fleet, confirm re-running is a no-op");
    // One glob, not a list: the fleet is named by shape, so a stream created
    // after this call still wakes the materializer. `*` matches exactly one
    // path segment.
    const first = await ensureSubscription({ ...subscription, pattern: "agents/*", leaseTtlMs: LEASE_TTL_MS });
    const second = await ensureSubscription({ ...subscription, pattern: "agents/*", leaseTtlMs: LEASE_TTL_MS });
    console.log(`  first call:  created=${first.created}`);
    console.log(`  second call: created=${second.created}  (idempotent re-confirmation)`);
    if (!first.created) throw new Error("ensureSubscription: expected the first call to create the subscription");
    if (second.created) throw new Error("ensureSubscription: expected the second call to just confirm, not recreate");

    // ---- 3. One worker drains exactly the pending streams ----
    banner("3. DRAIN — the worker reads only what the server says is pending");
    readThisRound.clear();
    const round1 = await consumeSubscribed({
      subscription,
      worker: "materializer-1",
      sourceFor,
      store: belief,
      checkpoints,
      project,
    });
    const drained1 = round1.streams.map((s) => s.path);
    const skipped1 = AGENT_PATHS.filter((path) => !drained1.includes(path));
    for (const s of round1.streams) console.log(`  drained  ${s.path}  (${s.processed} items)`);
    console.log(`  skipped  ${skipped1.length} idle streams: ${skipped1.join(", ")}`);

    assertEqual(sortedSet(drained1), sortedSet(BUSY_AT_START), "round 1 drained streams");
    assertEqual(sortedSet(readThisRound), sortedSet(BUSY_AT_START), "round 1 streams actually opened");
    if (skipped1.some((path) => readThisRound.has(path))) {
      throw new Error("round 1: an idle stream was opened — pull-wake stopped filtering");
    }
    console.log(`  nextWake: ${round1.nextWake}  (no work left queued)`);

    // ---- 4. nextWake drives the loop, not a timer ----
    banner("4. WAKE AGAIN — new work lands, only those streams get drained");
    for (const path of WAKES_LATER) {
      server.append(path, [{ key: `${path}/w1`, agent: path, note: "arrived after round 1" }]);
    }
    console.log(`  new work on: ${WAKES_LATER.join(", ")}`);

    readThisRound.clear();
    const round2 = await consumeSubscribed({
      subscription,
      worker: "materializer-1",
      sourceFor,
      store: belief,
      checkpoints,
      project,
    });
    const drained2 = round2.streams.map((s) => s.path);
    for (const s of round2.streams) console.log(`  drained  ${s.path}  (${s.processed} items)`);

    assertEqual(sortedSet(drained2), sortedSet(WAKES_LATER), "round 2 drained streams");
    assertEqual(sortedSet(readThisRound), sortedSet(WAKES_LATER), "round 2 streams actually opened");
    console.log("  → the already-drained streams from round 1 were not re-read");

    // ---- 5a. Lease handoff on failure ----
    banner("5a. LEASE HANDOFF — a poisoned message releases the lease, not holds it");
    server.append(POISON_STREAM, [{ key: `${POISON_STREAM}/poison`, agent: POISON_STREAM, note: "BOOM" }]);
    const poisonProject: Projector<typeof swarmGraph, WorkMessage> = async () => {
      throw new Error(`projector poisoned by a message on ${POISON_STREAM}`);
    };

    const before = Date.now();
    try {
      await consumeSubscribed({
        subscription,
        worker: "materializer-1",
        sourceFor,
        store: belief,
        checkpoints,
        project: poisonProject,
      });
      throw new Error("expected the poisoned projector to throw, but consumeSubscribed succeeded");
    } catch (error) {
      if (!(error instanceof Error) || !error.message.includes("poisoned")) throw error;
      console.log(`  worker-1 crashed: ${error.message}`);
    }

    // The claim was released on the throw, not held for LEASE_TTL_MS — a
    // second worker can claim it right away and finish the work properly.
    const recovered = await consumeSubscribed({
      subscription,
      worker: "materializer-2",
      sourceFor,
      store: belief,
      checkpoints,
      project,
    });
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

    // ---- 5b. Contention and fencing ----
    banner("5b. CONTENTION & FENCING — two ways a stale or busy claim is refused");

    // A lease another worker currently holds: SubscriptionClaimedError.
    const held = await claimSubscription(subscription, "materializer-3");
    try {
      await claimSubscription(subscription, "materializer-4");
      throw new Error("expected claiming an already-held lease to throw SubscriptionClaimedError");
    } catch (error) {
      if (!(error instanceof SubscriptionClaimedError)) throw error;
      console.log(`  worker-4 blocked: ${error.message}`);
    }
    await releaseSubscription(subscription, held);

    // A claim the server has already moved past: SubscriptionFencedError.
    const stale = await claimSubscription(subscription, "materializer-5");
    await releaseSubscription(subscription, stale);
    const current = await claimSubscription(subscription, "materializer-6");
    try {
      await ackSubscription(subscription, stale, [], true);
      throw new Error("expected an ack under a stale claim to throw SubscriptionFencedError");
    } catch (error) {
      if (!(error instanceof SubscriptionFencedError)) throw error;
      console.log(`  worker-5's stale claim fenced: ${error.message}`);
    }
    await releaseSubscription(subscription, current);

    // ---- Closing ----
    banner("What this demo does and does not prove");
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
    await Promise.all(opened.map((store) => store.close()));
    await server.close();
  }
}

runAsMain(import.meta.url, main);
