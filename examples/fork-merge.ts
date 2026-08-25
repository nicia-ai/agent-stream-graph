/**
 * Demo — fork a durable stream at a checkpoint, let the branches diverge, then
 * MERGE the beliefs they built.
 *
 * Every fork-capable agent system in the field stops at fork → diff → a human
 * picks a winner. This is the other half: Durable Streams forks the log at an
 * exact message, each fork materializes its own bitemporal belief graph, and
 * TypeGraph's `mergeIncremental` reconciles the diverged state — entity
 * resolution included, conflicts flagged rather than silently resolved.
 *
 * The join between the two halves is the cursor. `durableStreamSource` addresses
 * a message the way the protocol does — an offset plus a count of messages past
 * it — which is exactly what `Stream-Fork-Offset` / `Stream-Fork-Sub-Offset`
 * take. So "fork the log where this graph was last anchored" is
 * `forkPointFor(cursor)`, with nothing to re-derive.
 *
 * Runs against the in-process stand-in the tests use, so it needs no
 * infrastructure. Against a real endpoint the library calls here — `consume`,
 * `forkStream`, `forkPointFor`, `mergeIncremental` — are unchanged; what would
 * change is the setup, since `createStream`/`append` are the stand-in's own
 * helpers standing in for whatever writes the streams in your system.
 *
 * Run with:  pnpm demo:fork-merge
 */
import { searchable, type RecordedInstant, defineGraph, defineNode } from "@nicia-ai/typegraph";
import {
  asBranchId,
  ingestionBranch,
  isOk,
  mergeIncremental,
  type PropertyConflict,
  unwrap,
} from "@nicia-ai/typegraph/graph-merge";
import { exportGraphStream, importGraphStream } from "@nicia-ai/typegraph/interchange";
import { z } from "zod";

import {
  checkpointGraph,
  consume,
  durableStreamSource,
  forkPointFor,
  forkStream,
  typeGraphCheckpoints,
  type CheckpointBook,
  type Projector,
  type ShapeSource,
} from "../src";
import { startDurableStreamsServer } from "../test/support/durable-streams-server";
import { type DemoStore, makeBackend, newStore, runAsMain } from "./_support";

// ============================================================
// What the agents are researching
// ============================================================

const Finding = defineNode("Finding", {
  schema: z.object({
    claim: searchable({ language: "english" }),
    topic: z.string(),
    confidence: z.string(),
  }),
});

const researchGraph = defineGraph({
  id: "fork_merge_research",
  nodes: {
    Finding: {
      type: Finding,
      unique: [{ name: "finding_topic", fields: ["topic"], scope: "kind", collation: "caseInsensitive" }],
    },
  },
  edges: {},
});
type ResearchStore = DemoStore<typeof researchGraph>;

type Note = Readonly<{ key: string; claim: string; topic: string; confidence: string }>;

const BASE_STREAM = "agents/researcher";
const CAUTIOUS_STREAM = "agents/researcher-cautious";
const BOLD_STREAM = "agents/researcher-bold";

/** The prefix both branches share — the research done before the fork. */
const SHARED: readonly Note[] = [
  { key: "n1", claim: "Latency regressions trace to the cache layer", topic: "latency", confidence: "medium" },
  { key: "n2", claim: "Retries amplify load under partial failure", topic: "retries", confidence: "high" },
];

/** After the fork, the same evidence gets read two ways. */
const CAUTIOUS_ONLY: readonly Note[] = [
  { key: "n3", claim: "Cache eviction is a contributing factor, not the cause", topic: "latency", confidence: "low" },
];
const BOLD_ONLY: readonly Note[] = [
  { key: "n4", claim: "Cache eviction is the root cause", topic: "latency", confidence: "high" },
  { key: "n5", claim: "Connection pool saturation is a separate fault", topic: "pooling", confidence: "medium" },
];

// The TOPIC is the entity; a message is an observation about it. Keying the
// upsert on the topic is what makes a later note REVISE an earlier belief within
// a branch — and what leaves the two branches holding contradictory beliefs
// about the same entity for the merge to reconcile.
const project: Projector<typeof researchGraph, Note> = async (tx, change) => {
  await tx.nodes.Finding.upsertById(change.value.topic, {
    claim: change.value.claim,
    topic: change.value.topic,
    confidence: change.value.confidence,
  });
};

// ============================================================
// Materialize one stream into its own belief graph
// ============================================================

async function materialize(
  source: ShapeSource<Note>,
  belief: ResearchStore,
  checkpoints: CheckpointBook,
): Promise<string> {
  const result = await consume({ source, store: belief, checkpoints, project });
  if (result.lastOffset === undefined) {
    throw new Error(`materialize(${source.name}): consumed ${result.processed} changes but checkpointed nothing`);
  }
  return result.lastOffset;
}

// ============================================================
// Merge one branch's belief into the canonical graph
// ============================================================

async function mergeBeliefInto(
  forkPoint: ResearchStore,
  canonical: ResearchStore,
  branchName: string,
  belief: ResearchStore,
): Promise<readonly PropertyConflict<typeof researchGraph>[]> {
  const branchId = asBranchId(branchName);
  // An INGESTION branch defers the fork point's node uniqueness to the resolved
  // write set, so a branch finding that ALIASES one canonical already holds —
  // same unique `topic`, different id — reaches merge resolution instead of
  // being rejected at staging.
  const staged = unwrap(await ingestionBranch(forkPoint, makeBackend, { id: branchId }));
  try {
    await importGraphStream(staged, exportGraphStream(belief, { includeTemporal: true }), {
      onConflict: "update",
    });
    const result = await mergeIncremental({
      forkPoint,
      target: canonical,
      branches: [staged],
      options: {
        resolve: { Finding: { similarity: { kind: "fulltext", fields: ["claim"] }, threshold: 0.9 } },
        onPropertyConflict: "flag",
        onBasePropertyConflict: "flag",
        branchOrder: [branchId],
      },
    });
    if (!isOk(result)) throw result.error;
    return result.data.conflicts;
  } finally {
    await staged.close();
  }
}

// ============================================================
// Reporting
// ============================================================

type ResearchView = { query: ResearchStore["query"] };

async function findings(view: ResearchView): Promise<readonly Readonly<{ topic: string; claim: string; confidence: string }>[]> {
  return view
    .query()
    .from("Finding", "f")
    .select((context) => ({ topic: context.f.topic, claim: context.f.claim, confidence: context.f.confidence }))
    .execute();
}

async function report(label: string, view: ResearchView): Promise<void> {
  const rows = [...(await findings(view))].sort((left, right) => left.topic.localeCompare(right.topic));
  console.log(`\n${label}`);
  for (const row of rows) {
    console.log(`  ${row.topic.padEnd(8)} ${row.confidence.padEnd(7)} ${row.claim}`);
  }
}

// ============================================================
// The demo
// ============================================================

async function main(): Promise<void> {
  const server = await startDurableStreamsServer();
  // Every store opened here, in open order, so the `finally` closes them all —
  // the checkpoint store's graph differs from the research stores', so the list
  // names only what closing needs.
  const opened: { close: () => Promise<void> }[] = [];

  const sourceFor = (path: string): ShapeSource<Note> =>
    durableStreamSource<Note, Note>({
      url: server.streamUrl(path),
      name: path,
      toChange: (note) => ({ shape: "finding", key: note.key, operation: "insert", value: note }),
    });

  try {
    // ---- 1. Shared research, materialized into a belief graph ----
    server.createStream(BASE_STREAM);
    server.append(BASE_STREAM, SHARED);

    const cursorStore = await newStore(checkpointGraph, false);
    const checkpoints = typeGraphCheckpoints(cursorStore);
    const shared = await newStore(researchGraph, true);
    opened.push(cursorStore, shared);
    const forkCursor = await materialize(sourceFor(BASE_STREAM), shared, checkpoints);
    await report("SHARED PREFIX — what both branches start from", shared);
    console.log(`\n  checkpoint cursor: ${forkCursor}`);

    // ---- 2. Fork the LOG at exactly that cursor ----
    const at = forkPointFor(forkCursor);
    console.log(`  fork point:        offset=${at.offset} sub-offset=${at.subOffset}`);
    await Promise.all(
      [CAUTIOUS_STREAM, BOLD_STREAM].map((path) =>
        forkStream({ url: server.streamUrl(path), sourcePath: BASE_STREAM, at }),
      ),
    );
    server.append(CAUTIOUS_STREAM, CAUTIOUS_ONLY);
    server.append(BOLD_STREAM, BOLD_ONLY);

    // ---- 3. Each fork builds its own belief ----
    const cautious = await newStore(researchGraph, true);
    const bold = await newStore(researchGraph, true);
    opened.push(cautious, bold);
    // Sequential deliberately: the two branches have their own belief stores but
    // share one checkpoint book, and concurrent consumers would interleave
    // transactions on it.
    await materialize(sourceFor(CAUTIOUS_STREAM), cautious, checkpoints);
    await materialize(sourceFor(BOLD_STREAM), bold, checkpoints);
    await report("BRANCH A (cautious) — inherits the prefix, then diverges", cautious);
    await report("BRANCH B (bold) — same prefix, opposite conclusion", bold);

    // ---- 4. Reconcile the diverged beliefs ----
    const forkPoint = await newStore(researchGraph, false);
    const canonical = await newStore(researchGraph, true);
    opened.push(forkPoint, canonical);
    const conflictsA = await mergeBeliefInto(forkPoint, canonical, "cautious", cautious);
    const conflictsB = await mergeBeliefInto(forkPoint, canonical, "bold", bold);
    await report("CANONICAL — both branches merged, entity-resolved", canonical);

    const conflicts = [...conflictsA, ...conflictsB];
    console.log(
      conflicts.length === 0
        ? "\n  no property conflicts"
        : `\n  ${conflicts.length} property conflict(s) flagged for review — the branches disagreed, and the` +
            " merge said so rather than silently picking a winner",
    );

    // ---- 5. Time-travel a branch back to the fork ----
    // A fork inherits its source's offsets, so the SAME cursor string addresses
    // the same message on the branch as on the trunk. Branch A's own belief can
    // therefore be rewound to the fork point without tracking a second cursor.
    const anchorAtFork: RecordedInstant | undefined = await checkpoints.anchorFor(CAUTIOUS_STREAM, forkCursor);
    if (anchorAtFork === undefined) {
      throw new Error(`no anchor for ${forkCursor} on ${CAUTIOUS_STREAM}`);
    }
    await report("BRANCH A, its own belief rewound to the fork point", cautious.asOfRecorded(anchorAtFork));
  } finally {
    await Promise.all(opened.map((store) => store.close()));
    await server.close();
  }
}

runAsMain(import.meta.url, main);
