/**
 * Demo — an agent's memory is an append-only log; the graph is a fold over it.
 *
 * `examples/agents.ts` shows the READ side: a decoder turning someone else's
 * rows into graph events. This is the WRITE side — an agent authoring events
 * of its own with `graphEmitter`, directly, with no foreign row in sight.
 *
 * The events are plain JSON. That is not a detail, it is the whole point: a
 * `GraphEvent` can be printed, logged, shipped over a wire, and replayed
 * without a class, a brand, or a symbol surviving the trip. Once the events
 * exist, the graph is nothing more than `consume()` folding them into a
 * store — fold the same log twice and you get the same graph twice.
 *
 * Run with:  pnpm tsx examples/emit.ts
 */
import { isDeepStrictEqual } from "node:util";

import { defineEdge, defineGraph, defineNode } from "@nicia-ai/typegraph";
import { z } from "zod";

import {
  applyGraphEvents,
  checkpointGraph,
  consume,
  graphEmitter,
  type GraphEmitter,
  type GraphEvent,
  mockShapeSource,
  type Projector,
  type ShapeChange,
  typeGraphCheckpoints,
} from "../src";
import { type DemoHistoryStore, newStore, RULE, runAsMain, section } from "./_support";

// ============================================================
// A small graph: an agent reading a codebase
// ============================================================

const File = defineNode("File", { schema: z.object({ path: z.string(), language: z.string() }) });
// The kind is "Symbol"; the binding is `SymbolNode` so it does not shadow the global `Symbol`.
const SymbolNode = defineNode("Symbol", { schema: z.object({ name: z.string(), symbolKind: z.string() }) });
const definedIn = defineEdge("definedIn", { schema: z.object({}) });

const codeGraph = defineGraph({
  id: "agent_code_memory",
  nodes: { File: { type: File }, Symbol: { type: SymbolNode } },
  edges: { definedIn: { type: definedIn, from: [SymbolNode], to: [File] } },
});

type CodeEvent = GraphEvent<typeof codeGraph>;
type CodeBelief = DemoHistoryStore<typeof codeGraph>;
/** One log entry: every event a single observation produced, applied as one batch. */
type LogEntry = ShapeChange<readonly CodeEvent[]>;

// ============================================================
// The agent: observations in, graph events out
// ============================================================
//
// `remember` is the agent's whole vocabulary — everything it knows how to say
// about the world reduces to these four cases. It is PURE: no store, no I/O,
// unit-testable with nothing running. Each observation is self-contained, the
// way a re-scanned file or an Electric row is: the agent doesn't track a
// diff, it just says what it currently sees.

type Observation =
  | Readonly<{ type: "file-seen"; id: string; path: string; language: string }>
  | Readonly<{ type: "symbol-seen"; id: string; file: string; name: string; symbolKind: string }>
  | Readonly<{ type: "symbol-moved"; id: string; from: string; to: string }>
  | Readonly<{ type: "symbol-removed"; id: string; file: string }>;

function remember(emit: GraphEmitter<typeof codeGraph>, obs: Observation): readonly CodeEvent[] {
  switch (obs.type) {
    case "file-seen":
      return [emit.nodes.File.upsert(obs.id, { path: obs.path, language: obs.language })];
    case "symbol-seen":
      // Kind comes from the OBJECT PATH: `emit.nodes.Symbl.upsert(...)` (typo)
      // is a compile error — "Property 'Symbl' does not exist" — not a string
      // that silently fails once it reaches the store.
      return [
        emit.nodes.Symbol.upsert(obs.id, { name: obs.name, symbolKind: obs.symbolKind }),
        emit.edges.definedIn.upsert({ kind: "Symbol", id: obs.id }, { kind: "File", id: obs.file }),
      ];
    case "symbol-moved":
      // The old relationship ends, the new one begins. Order within the array
      // doesn't matter: applyGraphEvents applies every upsert in a batch
      // before any remove.
      return [
        emit.edges.definedIn.remove({ kind: "Symbol", id: obs.id }, { kind: "File", id: obs.from }),
        emit.edges.definedIn.upsert({ kind: "Symbol", id: obs.id }, { kind: "File", id: obs.to }),
      ];
    case "symbol-removed":
      // A node can't be removed out from under an edge that still points at
      // it, yet the node comes first here on purpose: applyGraphEvents removes
      // edges before the nodes they hang off, so the agent never has to care.
      return [
        emit.nodes.Symbol.remove(obs.id),
        emit.edges.definedIn.remove({ kind: "Symbol", id: obs.id }, { kind: "File", id: obs.file }),
      ];
  }
}

// The agent's actual memory: what it saw, in order, while reading the repo.
const OBSERVATIONS: readonly Observation[] = [
  { type: "file-seen", id: "f1", path: "src/graph.ts", language: "typescript" },
  { type: "symbol-seen", id: "s1", file: "f1", name: "parseGraph", symbolKind: "function" },
  { type: "file-seen", id: "f2", path: "src/utils.ts", language: "typescript" },
  { type: "symbol-seen", id: "s2", file: "f2", name: "clamp", symbolKind: "function" },
  // Re-scanned utils.ts after a rename — same id, new name, same file.
  { type: "symbol-seen", id: "s2", file: "f2", name: "clampValue", symbolKind: "function" },
  { type: "symbol-moved", id: "s2", from: "f2", to: "f1" }, // clampValue moved into graph.ts
  { type: "symbol-removed", id: "s1", file: "f1" }, // parseGraph got deleted
];

// What folding OBSERVATIONS must produce: parseGraph removed, clampValue moved
// into graph.ts, and utils.ts still present though nothing is defined in it.
const EXPECTED_GRAPH: Snapshot = {
  files: [
    { id: "f1", path: "src/graph.ts", language: "typescript" },
    { id: "f2", path: "src/utils.ts", language: "typescript" },
  ],
  symbols: [{ id: "s2", name: "clampValue", symbolKind: "function", file: "src/graph.ts" }],
};

// ============================================================
// The demo
// ============================================================

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" An agent's memory is an append-only log; the graph is a fold over it");
  console.log(RULE);

  section("(a) Authoring events — graphEmitter(graph), not a store in sight");
  const emit = graphEmitter(codeGraph);
  const batches = OBSERVATIONS.map((obs) => remember(emit, obs));
  const events = batches.flat();
  console.log(`\n  ${events.length} events authored from ${OBSERVATIONS.length} observations`);
  console.log("\n  the first event, exactly as it would sit in a log file:");
  console.log("    " + JSON.stringify(events[0]));

  section("(b) Plain JSON — no brands, no symbols, no hidden fields");
  const wire: readonly (readonly CodeEvent[])[] = JSON.parse(JSON.stringify(batches));
  // Deep equality, not a second stringify: it also compares prototypes and
  // symbol keys, which JSON.stringify would drop on both sides and never notice.
  const survivedRoundTrip = isDeepStrictEqual(wire, batches);
  console.log(`\n  round-tripped ${events.length} events through JSON.stringify -> JSON.parse`);
  console.log(`  deep-equal to the originals after the round trip: ${survivedRoundTrip}`);
  if (!survivedRoundTrip) throw new Error("round trip changed the events — a brand or hidden field leaked into GraphEvent");

  const log = toLog(wire);
  console.log("\n  the full log, one append per observation:");
  for (const entry of log) {
    console.log(`    @${entry.offset}`);
    for (const event of entry.value) console.log(`      ${JSON.stringify(event)}`);
  }

  const belief = await newStore(codeGraph, true);
  const replica = await newStore(codeGraph, true);
  try {
    section("(c) Folding the log — consume() + applyGraphEvents, nothing else");
    await fold(belief, "code-reader-agent", log);

    section("(d) The log and the graph are two views of the same thing");
    const graph = await snapshot(belief);
    console.log("\n  resulting graph:");
    for (const file of graph.files) console.log(`    file   ${file.id} ${file.path} (${file.language})`);
    for (const symbol of graph.symbols) {
      console.log(`    symbol ${symbol.id} ${symbol.name} (${symbol.symbolKind}) — defined in ${symbol.file}`);
    }
    console.log("\n  → parseGraph is gone (removed), clampValue now lives in graph.ts (moved),");
    console.log("    and utils.ts is still there even though nothing is defined in it anymore.");
    assertGraph("the fold", graph, EXPECTED_GRAPH);

    section("(e) The fold is deterministic and idempotent");
    await fold(replica, "code-reader-agent-replica", log);
    assertGraph("the same log folded into an independent store", await snapshot(replica), graph);
    console.log("  same log, independent store: identical graph");
    await fold(belief, "code-reader-agent-redelivery", log);
    assertGraph("the whole log re-delivered onto the first store", await snapshot(belief), graph);
    console.log("  same log re-delivered onto the first store: identical graph");
  } finally {
    await Promise.allSettled([belief.close(), replica.close()]);
  }

  console.log("\n" + RULE);
  console.log(" The graph never held anything the log didn't say first.");
  console.log(RULE + "\n");
}

// ============================================================
// Log, fold, and snapshot
// ============================================================

// Every entry is an INSERT onto the log, even one whose events remove things:
// the log never updates or deletes an entry it already wrote, it only ever
// appends the next thing the agent saw.
function toLog(batches: readonly (readonly CodeEvent[])[]): readonly LogEntry[] {
  return batches.map((events, index) => {
    const position = String(index + 1).padStart(3, "0");
    return { offset: position, shape: "observation", key: position, operation: "insert", value: events };
  });
}

const project: Projector<typeof codeGraph, readonly CodeEvent[]> = async (tx, entry) => {
  await applyGraphEvents(tx, entry.value);
};

/** Fold the whole log into `belief`, as a stream with a fresh cursor. */
async function fold(belief: CodeBelief, streamName: string, log: readonly LogEntry[]): Promise<void> {
  const cursor = await newStore(checkpointGraph);
  try {
    const source = mockShapeSource(streamName, log);
    const result = await consume({ source, store: belief, checkpoints: typeGraphCheckpoints(cursor), project });
    console.log(`\n  ${streamName}: folded ${result.processed} log entries, cursor at ${result.lastOffset}`);
  } finally {
    await cursor.close();
  }
}

type Snapshot = Readonly<{
  files: readonly Readonly<{ id: string; path: string; language: string }>[];
  symbols: readonly Readonly<{ id: string; name: string; symbolKind: string; file: string }>[];
}>;

async function snapshot(belief: CodeBelief): Promise<Snapshot> {
  const files = await belief
    .query()
    .from("File", "f")
    .orderBy((c) => c.f.id)
    .select((c) => ({ id: c.f.id, path: c.f.path, language: c.f.language }))
    .execute();
  const symbols = await belief
    .query()
    .from("Symbol", "s")
    .traverse("definedIn", "d")
    .to("File", "f")
    .orderBy((c) => c.s.id)
    .select((c) => ({ id: c.s.id, name: c.s.name, symbolKind: c.s.symbolKind, file: c.f.path }))
    .execute();
  return { files, symbols };
}

function assertGraph(what: string, actual: Snapshot, expected: Snapshot): void {
  if (!isDeepStrictEqual(actual, expected)) {
    throw new Error(`${what} produced an unexpected graph:\n  expected: ${JSON.stringify(expected)}\n  actual:   ${JSON.stringify(actual)}`);
  }
}

// ============================================================
// Output
// ============================================================

runAsMain(import.meta.url, main);
