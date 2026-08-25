/**
 * Flagship demo - Deep Survey shared state -> canonical TypeGraph knowledge.
 *
 * Electric's Deep Survey example has explorers writing wiki rows and xrefs into
 * shared state. This demo keeps that shape, then projects the rows into a
 * semantic graph where repeated mentions collapse to canonical concepts with
 * source attribution.
 *
 * Run with: pnpm demo
 */
import {
  createStoreWithSchema,
  defineEdge,
  defineGraph,
  defineNode,
  searchable,
  type TransactionContext,
} from "@nicia-ai/typegraph";
import {
  asBranchId,
  branch,
  isOk,
  mergeIncremental,
  openProvenanceStore,
  readProvenance,
  unwrap,
  type BranchId,
} from "@nicia-ai/typegraph/graph-merge";
import { z } from "zod";

import {
  checkpointGraph,
  consume,
  mockShapeSource,
  typeGraphCheckpoints,
  type Projector,
  type ShapeChange,
} from "../src";
import {
  type DemoHistoryStore,
  type DemoStore,
  makeBackend,
  newStore,
  runAsMain,
} from "./_support";

export type WikiEntry = Readonly<{
  key: string;
  title: string;
  body: string;
  author: string;
  improved: boolean;
  offset: string;
}>;

export type Xref = Readonly<{
  key: string;
  a: string;
  b: string;
  offset: string;
}>;

type ConceptDef = Readonly<{
  id: string;
  name: string;
  kind: "system" | "primitive" | "library" | "pattern";
  aliases: readonly string[];
}>;

type ConceptMention = Readonly<{
  concept: ConceptDef;
  alias: string;
  entry: WikiEntry;
}>;

const WIKI: readonly WikiEntry[] = [
  {
    offset: "001",
    key: "explorer-agent-runtime",
    title: "Electric Agents Runtime",
    author: "explorer-runtime",
    improved: false,
    body:
      "Electric Agents runs durable agent entities. Durable Streams records the agent history, " +
      "while TanStack DB lets the UI subscribe to the live shared state.",
  },
  {
    offset: "002",
    key: "explorer-shared-state",
    title: "Shared State and Live UI",
    author: "explorer-state",
    improved: false,
    body:
      "The Electric AX Agents demo uses shared state so explorers write wiki entries. " +
      "Electric Agents and TanStack DB make updates visible to every participant.",
  },
  {
    offset: "003",
    key: "explorer-deep-survey",
    title: "Deep Survey Knowledge Graph",
    author: "explorer-survey",
    improved: false,
    body:
      "Deep Survey creates a growing knowledge graph from wiki entries and cross references. " +
      "Durable Streams backs the explorer histories while Electric Agents orchestrates the swarm.",
  },
];

const XREFS: readonly Xref[] = [
  { offset: "004", key: "runtime--shared-state", a: "explorer-agent-runtime", b: "explorer-shared-state" },
  { offset: "005", key: "shared-state--deep-survey", a: "explorer-shared-state", b: "explorer-deep-survey" },
];

const CONCEPTS: readonly ConceptDef[] = [
  {
    id: "electric-agents",
    name: "Electric Agents",
    kind: "system",
    aliases: ["Electric Agents", "Electric AX Agents", "agent entities"],
  },
  {
    id: "durable-streams",
    name: "Durable Streams",
    kind: "primitive",
    aliases: ["Durable Streams", "agent history", "explorer histories"],
  },
  {
    id: "tanstack-db",
    name: "TanStack DB",
    kind: "library",
    aliases: ["TanStack DB", "live UI"],
  },
  {
    id: "shared-state",
    name: "Shared State",
    kind: "primitive",
    aliases: ["shared state", "shared DB"],
  },
  {
    id: "deep-survey",
    name: "Deep Survey",
    kind: "system",
    aliases: ["Deep Survey", "growing knowledge graph"],
  },
  {
    id: "cross-references",
    name: "Cross References",
    kind: "pattern",
    aliases: ["cross references", "xrefs"],
  },
];

const WikiPage = defineNode("WikiPage", {
  schema: z.object({
    key: z.string(),
    title: z.string(),
    author: z.string(),
    offset: z.string(),
  }),
});

const Concept = defineNode("Concept", {
  schema: z.object({
    name: searchable({ language: "english" }),
    category: z.enum(["system", "primitive", "library", "pattern"]),
  }),
});

const mentions = defineEdge("mentions", {
  schema: z.object({
    alias: z.string(),
    offset: z.string(),
  }),
});

const deepSurveyGraph = defineGraph({
  id: "deep_survey_convergence",
  nodes: {
    WikiPage: {
      type: WikiPage,
      unique: [{ name: "wiki_key", fields: ["key"], scope: "kind", collation: "caseInsensitive" }],
    },
    Concept: {
      type: Concept,
      unique: [{ name: "concept_name", fields: ["name"], scope: "kind", collation: "caseInsensitive" }],
    },
  },
  edges: { mentions: { type: mentions, from: [WikiPage], to: [Concept] } },
});

type DeepSurveyStore = DemoStore<typeof deepSurveyGraph>;

type MentionRow = Readonly<{
  wikiKey: string;
  title: string;
  author: string;
  conceptId: string;
  conceptName: string;
  conceptKind: string;
}>;

// EXTRACTION: substring alias matching — a wiki entry mentions a concept when
// any of its aliases appears (case-insensitive) in the title or body. This is
// deliberately simple for the demo. The MERGE step below then uses TypeGraph's
// fulltext similarity (`searchable` + `threshold: 0.9`) to collapse
// entry-scoped concept nodes into canonical ones. Extraction (find mentions)
// and resolution (merge entities) are separate concerns with different methods.
function includesAlias(haystack: string, alias: string): boolean {
  return haystack.toLowerCase().includes(alias.toLowerCase());
}

function extractMentions(entry: WikiEntry): readonly ConceptMention[] {
  const text = `${entry.title}\n${entry.body}`;
  return CONCEPTS.flatMap((concept) => {
    const alias = concept.aliases.find((candidate) => includesAlias(text, candidate));
    return alias === undefined ? [] : [{ concept, alias, entry }];
  });
}

function conceptNodeId(entry: WikiEntry, concept: ConceptDef): string {
  return `${entry.key}::concept::${concept.id}`;
}

function branchIdFor(entry: WikiEntry): BranchId {
  return asBranchId(`${entry.author}/${entry.key}`);
}

function mentionKey(mention: ConceptMention): string {
  return `${mention.entry.key} -> ${mention.concept.name}`;
}

// The one projection both code paths share: the merge-branch builder below and
// the durable `consume()` pipeline. Takes the transaction-scoped write surface
// so it works as a `Projector` and inside an explicit `store.transaction(...)`.
// The concept-id strategy is the only thing that differs: the merge branches
// keep ENTRY-SCOPED concept ids (so the merge resolves the duplicates), while
// the single-store consumer uses CANONICAL ids (so same-name mentions converge
// by idempotent upsert without colliding on the unique `name` constraint).
async function projectEntry(
  tx: TransactionContext<typeof deepSurveyGraph>,
  entry: WikiEntry,
  conceptIdOf: (entry: WikiEntry, concept: ConceptDef) => string,
): Promise<void> {
  await tx.nodes.WikiPage.upsertById(entry.key, {
    key: entry.key,
    title: entry.title,
    author: entry.author,
    offset: entry.offset,
  });

  for (const mention of extractMentions(entry)) {
    const conceptId = conceptIdOf(entry, mention.concept);
    await tx.nodes.Concept.upsertById(conceptId, {
      name: mention.concept.name,
      category: mention.concept.kind,
    });
    await tx.edges.mentions.getOrCreateByEndpoints(
      { kind: "WikiPage", id: entry.key } as const,
      { kind: "Concept", id: conceptId } as const,
      { alias: mention.alias, offset: entry.offset },
    );
  }
}

async function buildExplorerBranch(forkPoint: DeepSurveyStore, entry: WikiEntry) {
  const branchId = branchIdFor(entry);
  const explorerBranch = unwrap(await branch(forkPoint, makeBackend, { id: branchId }));
  await explorerBranch.store.transaction((tx) => projectEntry(tx, entry, conceptNodeId));
  return explorerBranch;
}

// Materialize the wiki shape log through the package's own durable consumer:
// resume-safe `consume()` over a `ShapeSource`, checkpointed offset↔anchor
// bookkeeping, and replay of the belief by offset. This is the library the demo
// is named for — the branch/merge convergence below builds on the same
// projection, here with canonical concept ids so mentions converge in one store.
async function projectViaDurableConsumer(input: DeepSurveyConvergenceInput): Promise<void> {
  const belief = await newStore(deepSurveyGraph, true);
  const [cursor] = await createStoreWithSchema(checkpointGraph, await makeBackend());
  const book = typeGraphCheckpoints(cursor);

  try {
    const changes: readonly ShapeChange<WikiEntry>[] = input.wiki.map((entry) => ({
      offset: entry.offset,
      shape: "wiki",
      key: entry.key,
      operation: "insert",
      value: entry,
    }));
    const source = mockShapeSource<WikiEntry>("deep-survey", changes);
    const canonicalConceptId = (_entry: WikiEntry, concept: ConceptDef): string => concept.id;
    const project: Projector<typeof deepSurveyGraph, WikiEntry> = (tx, change) =>
      projectEntry(tx, change.value, canonicalConceptId);

    console.log("\n  Durable consumer — consume → crash → resume (replayable by offset)");
    const crashed = await consume({ source, store: belief, checkpoints: book, project, stopAfter: 1 });
    console.log(`    consumed ${crashed.processed}/${input.wiki.length} wiki rows, then "crashed" — cursor at ${await book.lastOffset("deep-survey")}`);
    const resumed = await consume({ source, store: belief, checkpoints: book, project });
    console.log(`    resumed without duplication: +${resumed.processed} rows — cursor at ${await book.lastOffset("deep-survey")}`);

    const conceptsAt = async (offset: string): Promise<number> => {
      const anchor = await book.anchorFor("deep-survey", offset);
      const view = anchor === undefined ? belief : belief.asOfRecorded(anchor);
      const rows = await view.query().from("Concept", "c").select((ctx) => ({ id: ctx.c.id })).execute();
      return rows.length;
    };
    const firstOffset = input.wiki[0]!.offset;
    const lastOffset = input.wiki[input.wiki.length - 1]!.offset;
    console.log(`    belief replayed by offset: @${firstOffset} held ${await conceptsAt(firstOffset)} concept(s); @${lastOffset}, ${await conceptsAt(lastOffset)}`);
  } finally {
    await Promise.allSettled([belief.close(), cursor.close()]);
  }
}

async function mentionRows(store: DeepSurveyStore): Promise<readonly MentionRow[]> {
  return store
    .query()
    .from("WikiPage", "w")
    .traverse("mentions", "m")
    .to("Concept", "c")
    .select((ctx) => ({
      wikiKey: ctx.w.key,
      title: ctx.w.title,
      author: ctx.w.author,
      conceptId: ctx.c.id,
      conceptName: ctx.c.name,
      conceptKind: ctx.c.category,
    }))
    .execute();
}

export type DeepSurveyConvergenceInput = Readonly<{
  title: string;
  wiki: readonly WikiEntry[];
  xrefs: readonly Xref[];
}>;

function sourceFor(input: DeepSurveyConvergenceInput, branchId: BranchId | string, sourceId: string): string | undefined {
  const entry = input.wiki.find((candidate) => branchIdFor(candidate) === branchId);
  if (entry === undefined) return undefined;
  const mention = extractMentions(entry).find((candidate) => conceptNodeId(entry, candidate.concept) === sourceId);
  if (mention === undefined) return undefined;
  return `${entry.author}/${entry.key} @${entry.offset} via "${mention.alias}"`;
}

function printSharedState(input: DeepSurveyConvergenceInput): void {
  console.log("\n  Deep Survey shared state rows");
  console.log(`    wiki rows: ${input.wiki.length}`);
  for (const entry of input.wiki) {
    console.log(`      @${entry.offset} ${entry.key} by ${entry.author}: ${entry.title}`);
  }
  console.log(`    xref rows: ${input.xrefs.length}`);
  for (const xref of input.xrefs) {
    console.log(`      @${xref.offset} ${xref.a} <-> ${xref.b}`);
  }
}

function printEntryScopedMentions(input: DeepSurveyConvergenceInput): void {
  const mentionsByEntry = input.wiki.map((entry) => ({ entry, mentions: extractMentions(entry) }));
  const allMentions = mentionsByEntry.flatMap((item) => item.mentions);
  console.log("\n  Shared state alone has entry-scoped mentions");
  console.log(`    ${allMentions.length} mentions across ${input.wiki.length} wiki rows`);
  for (const item of mentionsByEntry) {
    console.log(`      ${item.entry.key}`);
    for (const mention of item.mentions) {
      console.log(`        ${mentionKey(mention)} as "${mention.alias}"`);
    }
  }
}

async function printCanonicalGraph(canonical: DeepSurveyStore): Promise<void> {
  const rows = await mentionRows(canonical);
  const byConcept = new Map<string, MentionRow[]>();
  for (const row of rows) {
    const existing = byConcept.get(row.conceptName) ?? [];
    existing.push(row);
    byConcept.set(row.conceptName, existing);
  }

  console.log("\n  TypeGraph semantic projection");
  console.log(`    canonical concepts: ${byConcept.size}`);
  for (const [conceptName, conceptRows] of [...byConcept.entries()].sort(([left], [right]) => left.localeCompare(right))) {
    const sample = conceptRows[0]!;
    console.log(`      ${conceptName} (${sample.conceptKind})`);
    for (const row of [...conceptRows].sort((left, right) => left.wikiKey.localeCompare(right.wikiKey))) {
      console.log(`        mentioned by ${row.author}/${row.wikiKey}: ${row.title}`);
    }
  }
}

async function printProvenance(
  input: DeepSurveyConvergenceInput,
  canonical: DemoHistoryStore<typeof deepSurveyGraph>,
): Promise<void> {
  const provenanceStore = await openProvenanceStore(canonical);
  const conceptRows = await canonical
    .query()
    .from("Concept", "c")
    .select((ctx) => ({ id: ctx.c.id, name: ctx.c.name, kind: ctx.c.category }))
    .execute();

  console.log("\n  Canonical concept provenance");
  for (const concept of [...conceptRows].sort((left, right) => left.name.localeCompare(right.name))) {
    const provenance = await readProvenance(provenanceStore, { canonicalId: concept.id, role: "node" });
    const sources = provenance
      .map((row) => sourceFor(input, row.branchId, row.sourceId))
      .filter((source): source is string => source !== undefined)
      .sort((left, right) => left.localeCompare(right));

    console.log(`    ${concept.name}`);
    for (const source of sources) {
      console.log(`      ${source}`);
    }
  }
}

export async function runDeepSurveyConvergence(input: DeepSurveyConvergenceInput): Promise<void> {
  const rule = "=".repeat(78);
  console.log(rule);
  console.log(` ${input.title}`);
  console.log(rule);

  printSharedState(input);
  printEntryScopedMentions(input);
  await projectViaDurableConsumer(input);

  const forkPoint = await newStore(deepSurveyGraph, false);
  const canonical = await newStore(deepSurveyGraph, true);
  // A cleanup list, nothing more: `branch()` hands back portable `Store`s while
  // canonical/forkPoint are adapter stores, and closing needs neither surface.
  const stores: Pick<DeepSurveyStore, "close">[] = [canonical, forkPoint];

  try {
    const branches = await Promise.all(input.wiki.map((entry) => buildExplorerBranch(forkPoint, entry)));
    // Each branch holds its own backend handle; close them with the rest.
    stores.push(...branches.map((explorerBranch) => explorerBranch.store));
    const branchOrder = branches.map((entry) => entry.id);

    const result = await mergeIncremental({
      forkPoint,
      target: canonical,
      branches,
      options: {
        resolve: {
          Concept: { similarity: { kind: "fulltext", fields: ["name"] }, threshold: 0.9 },
        },
        branchOrder,
        onPropertyConflict: "flag",
        onBasePropertyConflict: "flag",
        persistProvenance: true,
      },
    });
    if (!isOk(result)) throw result.error;

    console.log("\n  Merge-as-projection");
    console.log(`    branch inputs: ${branches.length} wiki-entry projections`);
    console.log(`    merged nodes: ${result.data.merged.nodes}`);
    console.log(`    entity resolutions: ${result.data.resolutions.length}`);
    for (const resolution of result.data.resolutions) {
      console.log(`      ${resolution.kind}: ${resolution.memberIds.length} entry-scoped nodes -> ${resolution.canonicalId}`);
    }

    await printCanonicalGraph(canonical);
    await printProvenance(input, canonical);

    console.log("\n  Punchline");
    console.log("    Electric converges the live shared rows.");
    console.log("    TypeGraph converges the semantic entities those rows talk about, without losing source attribution.");
    console.log(rule + "\n");
  } finally {
    await Promise.allSettled(stores.map((store) => store.close()));
  }
}

export async function main(): Promise<void> {
  await runDeepSurveyConvergence({
    title: "Deep Survey convergence: shared state rows -> canonical knowledge graph",
    wiki: WIKI,
    xrefs: XREFS,
  });
}

runAsMain(import.meta.url, main);
