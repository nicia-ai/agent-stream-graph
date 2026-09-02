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
import { defineEdge, defineGraph, defineNode } from "@nicia-ai/typegraph";
import { z } from "zod";

import { newStore, runAsMain } from "./_support";

// ============================================================
// A small graph: an agent reading a codebase
// ============================================================

const File = defineNode("File", { schema: z.object({ path: z.string(), language: z.string() }) });
// `SymbolNode` rather than `Symbol` — the kind name below is "Symbol"; the
// binding just must not shadow the global `Symbol`.
const SymbolNode = defineNode("Symbol", { schema: z.object({ name: z.string(), symbolKind: z.string() }) });
const definedIn = defineEdge("definedIn", { schema: z.object({}) });

const codeGraph = defineGraph({
  id: "agent_code_memory",
  nodes: { File: { type: File }, Symbol: { type: SymbolNode } },
  edges: { definedIn: { type: definedIn, from: [SymbolNode], to: [File] } },
});

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

function remember(emit: GraphEmitter<typeof codeGraph>, obs: Observation): readonly GraphEvent<typeof codeGraph>[] {
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
      // The old relationship ends, the new one begins — order within the
      // returned array doesn't matter; applyGraphEvents applies every upsert
      // in a batch before any remove.
      return [
        emit.edges.definedIn.remove({ kind: "Symbol", id: obs.id }, { kind: "File", id: obs.from }),
        emit.edges.definedIn.upsert({ kind: "Symbol", id: obs.id }, { kind: "File", id: obs.to }),
      ];
    case "symbol-removed":
      // A node can't be removed out from under an edge that still points at
      // it. Both events land in this one batch — applyGraphEvents is what
      // puts them in the right order (edges before the nodes they hang off).
      return [
        emit.edges.definedIn.remove({ kind: "Symbol", id: obs.id }, { kind: "File", id: obs.file }),
        emit.nodes.Symbol.remove(obs.id),
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

// ============================================================
// Reporting: query the graph the log produced
// ============================================================

type FileRow = Readonly<{ id: string; path: string; language: string }>;
type SymbolRow = Readonly<{ id: string; name: string; symbolKind: string; file: string }>;
type CodeView = { query: Awaited<ReturnType<typeof newStore<typeof codeGraph>>>["query"] };

async function snapshot(view: CodeView): Promise<Readonly<{ files: readonly FileRow[]; symbols: readonly SymbolRow[] }>> {
  const files = await view.query().from("File", "f").select((c) => ({ id: c.f.id, path: c.f.path, language: c.f.language })).execute();
  const symbols = await view
    .query()
    .from("Symbol", "s")
    .traverse("definedIn", "d")
    .to("File", "f")
    .select((c) => ({ id: c.s.id, name: c.s.name, symbolKind: c.s.symbolKind, file: c.f.path }))
    .execute();
  return {
    files: [...files].sort((a, b) => a.id.localeCompare(b.id)),
    symbols: [...symbols].sort((a, b) => a.id.localeCompare(b.id)),
  };
}

const RULE = "━".repeat(74);
function section(title: string): void {
  console.log("\n" + RULE);
  console.log(` ${title}`);
  console.log(RULE);
}

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" An agent's memory is an append-only log; the graph is a fold over it");
  console.log(RULE);

  section("(a) Authoring events — graphEmitter(graph), not a store in sight");
  const emit = graphEmitter(codeGraph);
  const events: readonly GraphEvent<typeof codeGraph>[] = OBSERVATIONS.flatMap((obs) => remember(emit, obs));
  console.log(`\n  ${events.length} events authored from ${OBSERVATIONS.length} observations`);
  const firstEvent = events[0];
  if (firstEvent === undefined) throw new Error("remember() produced no events — nothing to demonstrate");
  console.log("\n  the first event, exactly as it would sit in a log file:");
  console.log("    " + JSON.stringify(firstEvent));

  section("(b) Plain JSON — no brands, no symbols, no hidden fields");
  // JSON.parse's return type is `any`, so this line is the proof: nothing
  // downstream needs a cast to keep treating the result as GraphEvent<typeof codeGraph>.
  const wire: readonly GraphEvent<typeof codeGraph>[] = JSON.parse(JSON.stringify(events));
  const survivedRoundTrip = wire.length === events.length && JSON.stringify(wire) === JSON.stringify(events);
  console.log(`\n  round-tripped ${wire.length} events through JSON.stringify -> JSON.parse`);
  console.log(`  byte-identical after the round trip: ${survivedRoundTrip}`);
  if (!survivedRoundTrip) throw new Error("round trip changed the events — a brand or hidden field leaked into GraphEvent");

  // The wire log, as an append-only stream: every entry is an INSERT onto the
  // log even when the entry's own payload is a node.remove — the log never
  // updates or deletes a row it already wrote, it only ever appends the next
  // thing the agent saw.
  const log: readonly ShapeChange<GraphEvent<typeof codeGraph>>[] = wire.map((event, i) => ({
    offset: String(i + 1).padStart(3, "0"),
    shape: "event",
    key: String(i + 1),
    operation: "insert",
    value: event,
  }));
  console.log("\n  the full log:");
  for (const change of log) console.log(`    @${change.offset} ${change.value.op} ${JSON.stringify(change.value)}`);

  section("(c) Folding the log — consume() + applyGraphEvents, nothing else");
  const project: Projector<typeof codeGraph, GraphEvent<typeof codeGraph>> = async (tx, change) => {
    await applyGraphEvents(tx, [change.value]);
  };
  async function foldInto(agentName: string): Promise<CodeView> {
    const belief = await newStore(codeGraph, true);
    const book = typeGraphCheckpoints(await newStore(checkpointGraph));
    const source = mockShapeSource(agentName, log);
    const result = await consume({ source, store: belief, checkpoints: book, project });
    console.log(`\n  ${agentName}: folded ${result.processed} events, cursor at ${result.lastOffset}`);
    return belief;
  }
  const graph1 = await foldInto("code-reader-agent");

  section("(d) The log and the graph are two views of the same thing");
  const snap1 = await snapshot(graph1);
  console.log("\n  resulting graph:");
  for (const file of snap1.files) console.log(`    file   ${file.id} ${file.path} (${file.language})`);
  for (const symbol of snap1.symbols) console.log(`    symbol ${symbol.id} ${symbol.name} (${symbol.symbolKind}) — defined in ${symbol.file}`);
  console.log("\n  → parseGraph is gone (removed), clampValue now lives in graph.ts (moved),");
  console.log("    and utils.ts is still there even though nothing is defined in it anymore.");

  section("(e) The fold is deterministic and idempotent");
  const snap2 = await snapshot(await foldInto("code-reader-agent-replay"));
  const serialized1 = JSON.stringify(snap1);
  const serialized2 = JSON.stringify(snap2);
  const identical = serialized1 === serialized2;
  console.log(`\n  same log, independent store: graphs identical = ${identical}`);
  if (!identical) throw new Error(`fold is not deterministic:\n  first:  ${serialized1}\n  second: ${serialized2}`);

  console.log("\n" + RULE);
  console.log(" The graph never held anything the log didn't say first.");
  console.log(RULE + "\n");
}

runAsMain(import.meta.url, main);
