/**
 * Demo — three agent frameworks, one knowledge graph.
 *
 * Vercel AI SDK, LangGraph, and a generic JSONL tool-call log each describe
 * the SAME run — call `searchDocs`, then call `readFile` — in three
 * completely different event shapes. Each has its own decoder in this
 * package; none of the three knows the other two exist.
 *
 * This demo decodes all three fixtures with `graphEmitter(agentGraph)`,
 * the SAME emitter, and checks what comes out. It is a claim about the
 * DECODE layer specifically, not about a running database: a `Decoder` is
 * pure (no store, no I/O — see `graph.ts`), so "these three converge" is
 * checkable by comparing decoded event arrays, with nothing standing up.
 * That is also why this package carries no database dependency at all —
 * unlike `integrations/claude-agent` or `integrations/mcp-memory`, which
 * exercise the store side of the library, this one only has to prove the
 * decoder side, and a decoder has no store to stand up.
 *
 * Run with:  pnpm demo
 */
import { type Decoder, graphEmitter, type GraphEvent, type ShapeChange } from "@nicia-ai/agent-stream-graph";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

import { agentGraph } from "./graph.js";
import { decodeGenericJsonl, type ToolLogLine } from "./generic-jsonl.js";
import { decodeLangGraph, type LangGraphEvent } from "./langgraph.js";
import { decodeVercelAiSdk, type VercelStreamEvent } from "./vercel-ai-sdk.js";
import { parseToolLog } from "./parse-jsonl.js";

import { LANGGRAPH_CHANGES } from "../fixtures/langgraph.js";
import { VERCEL_CHANGES } from "../fixtures/vercel-ai-sdk.js";

const GENERIC_JSONL_FIXTURE = new URL("../fixtures/generic-jsonl.jsonl", import.meta.url);

const emit = graphEmitter(agentGraph);

function decodeAll<V>(changes: readonly ShapeChange<V>[], decode: Decoder<typeof agentGraph, V>): GraphEvent<typeof agentGraph>[] {
  return changes.flatMap((change) => decode(change, emit));
}

// ============================================================
// Canonicalizing: strip what is SUPPOSED to differ between sources
// ============================================================
//
// Each decoder mints its own Run id and its own run-scoped ToolCall id, so a
// byte comparison of raw events would fail even when the three sources agree
// on everything that matters. Canonicalizing drops the Run node itself (its
// `source` field is precisely the one thing that must differ) and the
// `madeCall` edge that names it, then renames each ToolCall id to the tool
// name it belongs to — stable across sources because this fixture calls each
// tool exactly once per run. Resource events are left untouched: their ids
// are already framework-independent (see `resourceRefFromInput`), so an
// identical Resource event surviving canonicalization unchanged, from all
// three sources, is itself part of what this demo is checking.

type Canonical = Readonly<{ op: string; kind: string; id?: string; from?: string; to?: string; props?: unknown }>;

function canonicalize(events: readonly GraphEvent<typeof agentGraph>[]): readonly Canonical[] {
  const toolNameById = new Map<string, string>();
  for (const event of events) {
    if (event.op === "node.upsert" && event.kind === "ToolCall") toolNameById.set(event.id, event.props.name);
  }
  const rename = (kind: string, id: string): string => (kind === "ToolCall" ? (toolNameById.get(id) ?? id) : id);

  const canonical: Canonical[] = [];
  for (const event of events) {
    if (event.kind === "Run" || event.kind === "madeCall") continue; // the run-linking half — expected to differ by source
    switch (event.op) {
      case "node.upsert":
        canonical.push({ op: event.op, kind: event.kind, id: rename(event.kind, event.id), props: event.props });
        break;
      case "node.remove":
        canonical.push({ op: event.op, kind: event.kind, id: rename(event.kind, event.id) });
        break;
      case "edge.upsert":
        canonical.push({
          op: event.op,
          kind: event.kind,
          from: rename(event.from.kind, event.from.id),
          to: rename(event.to.kind, event.to.id),
          props: event.props,
        });
        break;
      case "edge.remove":
        canonical.push({
          op: event.op,
          kind: event.kind,
          from: rename(event.from.kind, event.from.id),
          to: rename(event.to.kind, event.to.id),
        });
        break;
    }
  }
  return [...canonical].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
}

// ============================================================
// Reporting
// ============================================================

const RULE = "━".repeat(74);
function section(title: string): void {
  console.log("\n" + RULE);
  console.log(` ${title}`);
  console.log(RULE);
}

function printBatch(source: string, events: readonly GraphEvent<typeof agentGraph>[]): void {
  console.log(`\n  ${source}  (${events.length} graph events)`);
  for (const event of events) console.log(`    ${JSON.stringify(event)}`);
}

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" Three agent frameworks, one knowledge graph");
  console.log(RULE);

  section("(a) Decoding — one fixture per framework, the SAME emitter");

  const batches: Readonly<Record<string, readonly GraphEvent<typeof agentGraph>[]>> = {
    "vercel-ai-sdk": decodeAll<VercelStreamEvent>(VERCEL_CHANGES, decodeVercelAiSdk),
    langgraph: decodeAll<LangGraphEvent>(LANGGRAPH_CHANGES, decodeLangGraph),
    "generic-jsonl": decodeAll<ToolLogLine>(parseToolLog(readFileSync(GENERIC_JSONL_FIXTURE, "utf8")), decodeGenericJsonl),
  };
  for (const [source, events] of Object.entries(batches)) printBatch(source, events);

  section("(b) Each source names its OWN Run, in its own vocabulary");
  for (const [source, events] of Object.entries(batches)) {
    const run = events.find((event) => event.op === "node.upsert" && event.kind === "Run");
    if (run === undefined || run.op !== "node.upsert") throw new Error(`${source}: decoded no Run node`);
    console.log(`\n  ${source}: Run(${run.id}) { source: "${run.props.source as string}" }`);
  }
  console.log("\n  → three runs, three ids, three decoders that have never heard of each other.");

  // Leading with the check that carries no asterisk: no ids renamed, nothing
  // dropped, nothing normalized. See (d) below for the check that does.
  section("(c) The Resource nodes — compared with ZERO normalization");
  // resourceRefFromInput derives a Resource's id from the tool INPUT alone —
  // no run id, no framework name, nothing source-specific baked in. So unlike
  // ToolCall below, the three sources' Resource.upsert events are asserted
  // byte-identical exactly as decoded — nothing stripped, nothing renamed.
  const resourceEvents = Object.entries(batches).map(
    ([source, events]) =>
      [source, events.filter((event) => event.op === "node.upsert" && event.kind === "Resource")] as const,
  );
  const [firstResourceSource, firstResources] = resourceEvents[0] ?? [undefined, []];
  if (firstResourceSource === undefined) throw new Error("no sources decoded — nothing to compare");
  for (const [source, resources] of resourceEvents) {
    const identical = JSON.stringify(resources) === JSON.stringify(firstResources);
    console.log(`\n  ${source}: ${resources.length} Resource events, byte-identical to ${firstResourceSource} = ${identical}`);
    for (const resource of resources) console.log(`    ${JSON.stringify(resource)}`);
    if (!identical) {
      throw new Error(`${source} decoded a different Resource than ${firstResourceSource} for the same tool input — entity resolution broke by construction`);
    }
  }

  // The weaker half of the claim: comparing ToolCall events requires
  // normalizing exactly two things first — see canonicalize()'s own comment
  // and the README for what and why. Everything past that (name, status,
  // input, output, the touched edges) is compared as decoded, unchanged.
  section("(d) The ToolCall events — after normalizing exactly two things");

  const canonical = Object.fromEntries(Object.entries(batches).map(([source, events]) => [source, canonicalize(events)]));
  const [firstSource, ...restSources] = Object.keys(canonical);
  if (firstSource === undefined) throw new Error("no sources decoded — nothing to compare");
  const reference = JSON.stringify(canonical[firstSource]);

  for (const source of restSources) {
    const serialized = JSON.stringify(canonical[source]);
    const identical = serialized === reference;
    console.log(`\n  ${firstSource} vs ${source}: canonical events identical = ${identical}`);
    if (!identical) {
      throw new Error(
        `decoders diverged — ${firstSource} and ${source} decoded the same scenario into different graph events:\n` +
          `  ${firstSource}: ${reference}\n  ${source}:       ${serialized}`,
      );
    }
  }

  console.log("\n" + RULE);
  console.log(" Vercel AI SDK, LangGraph, and a plain JSONL log all decode to the");
  console.log(" same Run -> ToolCall -> Resource shape. Adopting a fourth framework");
  console.log(" costs one more file this size — see README.md.");
  console.log(RULE + "\n");
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exit(1);
  });
}
