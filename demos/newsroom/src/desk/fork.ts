/**
 * desk/fork.ts — fork the newsroom's shared belief at the exact point ash's
 * and brook's coverage of the story splits, then let a THIRD, hypothetical
 * account run forward from there.
 *
 * HONESTY NOTE, up front: this forks a GRAPH, not a LOG. `examples/fork-merge.ts`
 * forks a Durable Streams log — `forkStream`/`forkPointFor` against a running
 * server — and that is the right primitive when reporters read from a live,
 * server-hosted stream. This package's reporters read from checked-in
 * fixtures (`reporters/replay.ts`) or a one-shot agent call
 * (`reporters/live.ts`), neither of which is a durable log with a server to
 * fork; there is nothing there to branch. What CAN be forked honestly — and
 * is, below — is the BELIEF: `@nicia-ai/typegraph/graph-merge`'s `branch()`,
 * the same store-level primitive `ingestionBranch` (and so `desk/editor.ts`)
 * builds on. `consume`'s `stopAfter` still does the "freeze exactly here"
 * work a log-fork's offset would; `branch()` clones the store's CURRENT
 * state, so calling it immediately after `stopAfter` stops IS the fork point.
 *
 * The split: both ash and brook read the Q1 budget filing and file the same
 * award claim before parting ways on the contract's value ($41M vs $38M —
 * see `fixtures/dispatches.ts`'s header comment). This forks right after
 * that shared beat and lets a THIRD account — one no reporter actually
 * filed — run forward: a leaked memo putting the value at $45M. The real
 * "trunk" (ash's actual claim) and the hypothetical "what-if" branch are
 * independent stores from that point on; nothing written to one is visible
 * from the other, which the demo asserts rather than states.
 */
import { asBranchId, branch, unwrap } from "@nicia-ai/typegraph/graph-merge";
import { asNodeId, type Store } from "@nicia-ai/typegraph";
import { checkpointGraph, consume, graphProjector, typeGraphCheckpoints } from "@nicia-ai/agent-stream-graph";

import { BURNED_SOURCE } from "../../fixtures/dispatches.js";
import { type DeskHistoryStore, makeBackend, newStore } from "../backend.js";
import { decodeReporterEvent, type ClaimEvent } from "../decode.js";
import { newsroomGraph, type NewsroomGraph } from "../graph.js";
import { replaySource } from "../reporters/replay.js";

const project = graphProjector(newsroomGraph, decodeReporterEvent);

/** How many of ash's fixture events make up the shared prefix — see the file header. */
const SHARED_PREFIX_LENGTH = 3;

const WHAT_IF_CLAIM: ClaimEvent = {
  type: "claim",
  id: "claim-value-what-if",
  text: "A leaked internal memo puts the fleet modernisation contract at $45M — above either filed figure",
  predicate: "contractValue",
  value: "$45M",
  confidence: "single-source",
  subject: { id: "subject-halcyon", name: "Halcyon Transit Systems", handle: "@halcyon", role: "vendor" },
  sources: [BURNED_SOURCE.id],
  rule: "leaked internal memo, unconfirmed",
  validFrom: "2026-03-20T00:00:00.000Z",
};

export type ForkResult = Readonly<{
  /** The checkpoint offset both futures share up to. */
  forkOffset: string;
  /** What ash actually filed, continued for real past the fork point. */
  trunk: DeskHistoryStore<NewsroomGraph>;
  /** A hypothetical account nobody filed, run forward from the same starting point. */
  whatIf: Store<NewsroomGraph>;
  trunkValue: string | undefined;
  whatIfValue: string | undefined;
}>;

async function claimValue(store: Store<NewsroomGraph>, claimId: string): Promise<string | undefined> {
  const row = await store.nodes.Claim.getById(asNodeId(claimId));
  return row?.value;
}

/**
 * Fork the belief right where ash's and brook's coverage splits, and run a
 * hypothetical third account forward on the branch. Owns its own checkpoint
 * book and stores — deliberately a SEPARATE book from `desk/materialize.ts`'s,
 * even though it replays the same "reporter-ash" stream name: that stream is
 * already checkpointed to its end by the time this runs, and a shared book
 * would make this `consume` resume from there — i.e. read nothing — into a
 * store nothing has ever been written to. A fresh, standalone book is what
 * "owns its own stores" actually requires.
 */
export async function forkAtDivergence(): Promise<ForkResult> {
  const source = replaySource("reporter-ash");
  const trunk = await newStore(newsroomGraph, true);
  const cursor = await newStore(checkpointGraph, false);
  const checkpoints = typeGraphCheckpoints(cursor);

  // Freeze right after the shared beat (wire, tip, the award claim) — before
  // ash's and brook's belief part ways on the contract's value.
  const prefix = await consume({ source, store: trunk, checkpoints, project, stopAfter: SHARED_PREFIX_LENGTH });
  if (prefix.lastOffset === undefined) {
    throw new Error("forkAtDivergence: consumed the shared prefix but checkpointed nothing");
  }
  const forkOffset = prefix.lastOffset;

  // `branch()` clones `trunk`'s CURRENT state — which, because consumption
  // stopped right there, IS the pre-divergence belief.
  const whatIfBranch = unwrap(await branch(trunk, makeBackend, { id: asBranchId("ash-what-if") }));

  // The what-if: inject a THIRD version of events nobody actually filed.
  await whatIfBranch.store.transaction((tx) =>
    project(tx, { offset: "what-if-1", shape: "reporter-event", key: WHAT_IF_CLAIM.id, operation: "insert", value: WHAT_IF_CLAIM }),
  );

  // Continue the TRUNK forward with what ash actually filed.
  await consume({ source, store: trunk, checkpoints, project });

  const trunkValue = await claimValue(trunk, "claim-value");
  const whatIfValue = await claimValue(whatIfBranch.store, WHAT_IF_CLAIM.id);

  // The isolation assertion: the hypothetical must never leak into the trunk.
  const leaked = await claimValue(trunk, WHAT_IF_CLAIM.id);
  if (leaked !== undefined) {
    throw new Error("forkAtDivergence: the what-if claim leaked into the trunk store — branches are not isolated");
  }

  await cursor.close();

  return { forkOffset, trunk, whatIf: whatIfBranch.store, trunkValue, whatIfValue };
}
