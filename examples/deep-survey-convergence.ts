/**
 * Flagship demo — Deep Survey shared state -> canonical TypeGraph knowledge.
 *
 * Electric's Deep Survey example has explorers writing `wiki` rows and `xrefs`
 * into shared state. Each wiki row names concepts in its own words, so shared
 * state alone holds one mention per row, never one entity per concept. Wiki rows
 * become pages that mention concepts; xrefs become cross-reference edges between
 * pages. This demo projects those rows two ways:
 *
 *   1. through this package's durable consumer — consume, crash, resume without
 *      duplication, and replay the belief as it stood at any offset;
 *   2. as one TypeGraph branch per explorer, merged so the per-row mentions
 *      resolve to canonical concepts that keep their source attribution.
 *
 * Every claim the output makes is asserted, so a regression fails the run.
 *
 * Run with: pnpm demo
 */
import {
  defineEdge,
  defineGraph,
  defineNode,
  expr,
  searchable,
  type TransactionContext,
} from "@nicia-ai/typegraph";
import {
  asBranchId,
  branch,
  type BranchId,
  type GraphBranch,
  mergeIncremental,
  type MergeReport,
  openProvenanceStore,
  readProvenance,
  unwrap,
} from "@nicia-ai/typegraph/graph-merge";
import { z } from "zod";

import {
  checkpointGraph,
  compareOffsets,
  consume,
  mockShapeSource,
  typeGraphCheckpoints,
  type Projector,
  type ShapeChange,
} from "../src";
import { type DemoHistoryStore, type DemoStore, makeBackend, newStore, RULE, runAsMain } from "./_support";

export type WikiEntry = Readonly<{
  key: string;
  title: string;
  body: string;
  author: string;
  offset: string;
}>;

export type Xref = Readonly<{
  key: string;
  a: string;
  b: string;
  offset: string;
}>;

export type DeepSurveyConvergenceInput = Readonly<{
  title: string;
  wiki: readonly WikiEntry[];
  xrefs: readonly Xref[];
}>;

const CONCEPT_CATEGORIES = ["system", "primitive", "library", "pattern"] as const;

type ConceptDef = Readonly<{
  id: string;
  name: string;
  category: (typeof CONCEPT_CATEGORIES)[number];
  aliases: readonly string[];
}>;

type ConceptMention = Readonly<{ concept: ConceptDef; alias: string }>;

const WIKI: readonly WikiEntry[] = [
  {
    offset: "001",
    key: "explorer-agent-runtime",
    title: "Electric Agents Runtime",
    author: "explorer-runtime",
    body:
      "Electric Agents runs durable agent entities. Durable Streams records the agent history, " +
      "while TanStack DB lets the UI subscribe to the live shared state.",
  },
  {
    offset: "002",
    key: "explorer-shared-state",
    title: "Shared State and Live UI",
    author: "explorer-state",
    body:
      "The Electric AX Agents demo uses shared state so explorers write wiki entries. " +
      "Electric Agents and TanStack DB make updates visible to every participant.",
  },
  {
    offset: "003",
    key: "explorer-deep-survey",
    title: "Deep Survey Knowledge Graph",
    author: "explorer-survey",
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
    category: "system",
    aliases: ["Electric Agents", "Electric AX Agents", "agent entities"],
  },
  {
    id: "durable-streams",
    name: "Durable Streams",
    category: "primitive",
    aliases: ["Durable Streams", "agent history", "explorer histories"],
  },
  { id: "tanstack-db", name: "TanStack DB", category: "library", aliases: ["TanStack DB", "live UI"] },
  { id: "shared-state", name: "Shared State", category: "primitive", aliases: ["shared state", "shared DB"] },
  {
    id: "deep-survey",
    name: "Deep Survey",
    category: "system",
    aliases: ["Deep Survey", "growing knowledge graph"],
  },
  { id: "cross-references", name: "Cross References", category: "pattern", aliases: ["cross references", "xrefs"] },
];

const STREAM_NAME = "deep-survey";
const WIKI_SHAPE = "wiki";
const XREF_SHAPE = "xref";
/** Rows the durable consumer applies before the simulated crash. */
const ROWS_BEFORE_CRASH = 1;
/** Fulltext similarity at or above which two concept nodes are one entity. */
const CONCEPT_MATCH_THRESHOLD = 0.9;
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
    category: z.enum(CONCEPT_CATEGORIES),
  }),
});

const mentions = defineEdge("mentions", {
  schema: z.object({
    alias: z.string(),
    offset: z.string(),
  }),
});

const crossReferences = defineEdge("crossReferences", {
  schema: z.object({
    key: z.string(),
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
  edges: {
    mentions: { type: mentions, from: [WikiPage], to: [Concept] },
    crossReferences: { type: crossReferences, from: [WikiPage], to: [WikiPage] },
  },
});

type DeepSurveyStore = DemoStore<typeof deepSurveyGraph>;
type ConceptIdStrategy = (entry: WikiEntry, concept: ConceptDef) => string;

// ============================================================
// The demo, top to bottom
// ============================================================

export async function main(): Promise<void> {
  await runDeepSurveyConvergence({
    title: "Deep Survey convergence: shared state rows -> canonical knowledge graph",
    wiki: WIKI,
    xrefs: XREFS,
  });
}

export async function runDeepSurveyConvergence(input: DeepSurveyConvergenceInput): Promise<void> {
  console.log(`${RULE}\n ${input.title}\n${RULE}`);

  const { linked, dangling } = partitionXrefs(input);
  printSharedState(input, dangling);
  const survey: DeepSurveyConvergenceInput = { ...input, xrefs: linked };

  printEntryScopedMentions(survey);
  await projectViaDurableConsumer(survey);
  await convergeViaMerge(survey);

  console.log("\n  Punchline");
  console.log("    Electric converges the live shared rows.");
  console.log("    TypeGraph converges the semantic entities those rows talk about, without losing source attribution.");
  console.log(`${RULE}\n`);
}

// ============================================================
// Extraction and projection
// ============================================================

// EXTRACTION finds mentions: a row mentions a concept when any alias appears
// in its title or body. It is deliberately naive. RESOLUTION — deciding that
// two mentions are one entity — is the merge's job, and uses TypeGraph's
// fulltext similarity instead (see `convergeViaMerge`).
function extractMentions(entry: WikiEntry): readonly ConceptMention[] {
  const text = `${entry.title}\n${entry.body}`.toLowerCase();
  return CONCEPTS.flatMap((concept) => {
    const alias = concept.aliases.find((candidate) => text.includes(candidate.toLowerCase()));
    return alias === undefined ? [] : [{ concept, alias }];
  });
}

function distinctConceptCount(entries: readonly WikiEntry[]): number {
  return new Set(entries.flatMap(extractMentions).map((mention) => mention.concept.id)).size;
}

/** One concept node per (row, concept): duplicates the merge has to resolve. */
const entryScopedConceptId: ConceptIdStrategy = (entry, concept) => `${entry.key}::concept::${concept.id}`;

/** One concept node per concept: same-name mentions converge by idempotent upsert. */
const canonicalConceptId: ConceptIdStrategy = (_entry, concept) => concept.id;

// The projection both paths share; only the concept-id strategy differs.
// It writes through the transaction it is handed, so it serves as a
// `Projector` body and inside an explicit `store.transaction(...)` alike.
async function projectEntry(
  tx: TransactionContext<typeof deepSurveyGraph>,
  entry: WikiEntry,
  conceptIdOf: ConceptIdStrategy,
): Promise<void> {
  await tx.nodes.WikiPage.upsertById(entry.key, {
    key: entry.key,
    title: entry.title,
    author: entry.author,
    offset: entry.offset,
  });

  for (const mention of extractMentions(entry)) {
    const conceptId = conceptIdOf(entry, mention.concept);
    await tx.nodes.Concept.upsertById(conceptId, { name: mention.concept.name, category: mention.concept.category });
    // `ifExists: "update"` makes this an upsert: the default ("return") keeps an
    // existing edge's props, so a revised entry's new alias would be dropped.
    await tx.edges.mentions.getOrCreateByEndpoints(
      { kind: "WikiPage", id: entry.key },
      { kind: "Concept", id: conceptId },
      { alias: mention.alias, offset: entry.offset },
      { ifExists: "update" },
    );
  }
}

/**
 * An xref can only become an edge between two pages that exist. Live shared
 * state can hold one naming a page it does not (yet) have; projecting it would
 * fail the transaction on every re-delivery, so it is reported and left out.
 */
function partitionXrefs(input: DeepSurveyConvergenceInput): Readonly<{ linked: readonly Xref[]; dangling: readonly Xref[] }> {
  const pageKeys = new Set(input.wiki.map((entry) => entry.key));
  const joinsPages = (xref: Xref): boolean => pageKeys.has(xref.a) && pageKeys.has(xref.b);
  return { linked: input.xrefs.filter(joinsPages), dangling: input.xrefs.filter((xref) => !joinsPages(xref)) };
}

function isXref(row: WikiEntry | Xref): row is Xref {
  return "a" in row;
}

// Xrefs are symmetric in Deep Survey; the edge keeps the row's own a -> b order.
async function projectXref(tx: TransactionContext<typeof deepSurveyGraph>, xref: Xref): Promise<void> {
  await tx.edges.crossReferences.getOrCreateByEndpoints(
    { kind: "WikiPage", id: xref.a },
    { kind: "WikiPage", id: xref.b },
    { key: xref.key, offset: xref.offset },
    { ifExists: "update" },
  );
}

// ============================================================
// Path 1: the durable consumer — crash, resume, replay by offset
// ============================================================

async function projectViaDurableConsumer(input: DeepSurveyConvergenceInput): Promise<void> {
  const belief = await newStore(deepSurveyGraph, true);
  const cursor = await newStore(checkpointGraph);
  const book = typeGraphCheckpoints(cursor);

  try {
    // Xrefs follow the wiki rows in the stream, so every page an xref joins is
    // already projected when it arrives.
    const rows: readonly (WikiEntry | Xref)[] = [...input.wiki, ...input.xrefs];
    const changes: readonly ShapeChange<WikiEntry | Xref>[] = rows.map((row) => ({
      offset: row.offset,
      shape: isXref(row) ? XREF_SHAPE : WIKI_SHAPE,
      key: row.key,
      operation: "insert",
      value: row,
    }));
    const source = mockShapeSource(STREAM_NAME, changes);
    const project: Projector<typeof deepSurveyGraph, WikiEntry | Xref> = (tx, change) =>
      isXref(change.value) ? projectXref(tx, change.value) : projectEntry(tx, change.value, canonicalConceptId);

    console.log("\n  Durable consumer — consume → crash → resume (replayable by offset)");
    const crashed = await consume({ source, store: belief, checkpoints: book, project, stopAfter: ROWS_BEFORE_CRASH });
    console.log(`    consumed ${crashed.processed}/${rows.length} rows, then "crashed" — cursor at ${crashed.lastOffset}`);
    const resumed = await consume({ source, store: belief, checkpoints: book, project });
    console.log(`    resumed without duplication: +${resumed.processed} rows — cursor at ${resumed.lastOffset}`);
    if (crashed.processed + resumed.processed !== rows.length) {
      throw new Error(`crash + resume applied ${crashed.processed} + ${resumed.processed} rows; expected ${rows.length} in total`);
    }

    // Replay the belief as it stood when a row was consumed, and check it holds
    // exactly the concepts and cross references of that row and its predecessors.
    const replayAt = async (offset: string): Promise<string> => {
      const anchor = await book.anchorFor(STREAM_NAME, offset);
      if (anchor === undefined) throw new Error(`no checkpoint anchor recorded for ${STREAM_NAME}@${offset}`);
      const view = belief.asOfRecorded(anchor);
      const concepts = await view.query().from("Concept", "c").count();
      const xrefs = await view.query().from("WikiPage", "a").traverse("crossReferences", "x").to("WikiPage", "b").count();

      const consumedSoFar = (row: WikiEntry | Xref): boolean => compareOffsets(row.offset, offset) <= 0;
      const expected = { concepts: distinctConceptCount(input.wiki.filter(consumedSoFar)), xrefs: input.xrefs.filter(consumedSoFar).length };
      if (concepts !== expected.concepts || xrefs !== expected.xrefs) {
        throw new Error(
          `replay @${offset} held ${concepts} concept(s) and ${xrefs} cross reference(s); ` +
            `the rows up to it hold ${expected.concepts} and ${expected.xrefs}`,
        );
      }
      return `@${offset}: ${concepts} concept(s), ${xrefs} cross reference(s)`;
    };
    const replayOffsets = [...new Set([rows[0]!.offset, input.wiki.at(-1)!.offset, rows.at(-1)!.offset])];
    console.log("    belief replayed by offset:");
    for (const offset of replayOffsets) console.log(`      ${await replayAt(offset)}`);
  } finally {
    await Promise.allSettled([belief.close(), cursor.close()]);
  }
}

// ============================================================
// Path 2: one branch per explorer, merged into canonical concepts
// ============================================================

function branchIdFor(entry: WikiEntry): BranchId {
  return asBranchId(`${entry.author}/${entry.key}`);
}

async function buildExplorerBranch(
  forkPoint: DeepSurveyStore,
  entry: WikiEntry,
): Promise<GraphBranch<typeof deepSurveyGraph>> {
  const explorerBranch = unwrap(await branch(forkPoint, makeBackend, { id: branchIdFor(entry) }));
  await explorerBranch.store.transaction((tx) => projectEntry(tx, entry, entryScopedConceptId));
  return explorerBranch;
}

async function convergeViaMerge(input: DeepSurveyConvergenceInput): Promise<void> {
  const forkPoint = await newStore(deepSurveyGraph);
  const canonical = await newStore(deepSurveyGraph, true);
  // `branch()` hands back portable `Store`s, the others are adapter stores;
  // cleanup needs only `close`, so the list is typed to just that.
  const stores: Pick<DeepSurveyStore, "close">[] = [canonical, forkPoint];

  try {
    const branches: GraphBranch<typeof deepSurveyGraph>[] = [];
    for (const entry of input.wiki) {
      const explorerBranch = await buildExplorerBranch(forkPoint, entry);
      stores.push(explorerBranch.store);
      branches.push(explorerBranch);
    }

    const report = unwrap(
      await mergeIncremental({
        forkPoint,
        target: canonical,
        branches,
        options: {
          resolve: {
            Concept: { similarity: { kind: "fulltext", fields: ["name"] }, threshold: CONCEPT_MATCH_THRESHOLD },
          },
          branchOrder: branches.map((explorerBranch) => explorerBranch.id),
          persistProvenance: true,
        },
      }),
    );

    // Xrefs join pages, not concepts, so there is nothing for the merge to
    // resolve: they apply straight to canonical once the pages exist there.
    await canonical.transaction(async (tx) => {
      for (const xref of input.xrefs) await projectXref(tx, xref);
    });

    printMergeReport(report, branches.length);
    await printCanonicalGraph(input, canonical);
    await printCrossReferences(input, canonical);
    await printProvenance(input, canonical);
  } finally {
    await Promise.allSettled(stores.map((store) => store.close()));
  }
}

// ============================================================
// Reporting
// ============================================================

function printSharedState(input: DeepSurveyConvergenceInput, dangling: readonly Xref[]): void {
  console.log("\n  Deep Survey shared state rows");
  console.log(`    wiki rows: ${input.wiki.length}`);
  for (const entry of input.wiki) {
    console.log(`      @${entry.offset} ${entry.key} by ${entry.author}: ${entry.title}`);
  }
  console.log(`    xref rows: ${input.xrefs.length}`);
  for (const xref of input.xrefs) {
    const skipped = dangling.includes(xref) ? "  (skipped: names a page shared state does not hold)" : "";
    console.log(`      @${xref.offset} ${xref.a} <-> ${xref.b}${skipped}`);
  }
}

function printEntryScopedMentions(input: DeepSurveyConvergenceInput): void {
  const mentionCount = input.wiki.flatMap(extractMentions).length;
  console.log("\n  Shared state alone has entry-scoped mentions");
  console.log(`    ${mentionCount} mentions across ${input.wiki.length} wiki rows`);
  for (const entry of input.wiki) {
    console.log(`      ${entry.key}`);
    for (const mention of extractMentions(entry)) {
      console.log(`        -> ${mention.concept.name} as "${mention.alias}"`);
    }
  }
}

function printMergeReport(report: MergeReport<typeof deepSurveyGraph>, branchCount: number): void {
  console.log("\n  Merge-as-projection");
  console.log(`    branch inputs: ${branchCount} wiki-entry projections`);
  console.log(`    merged nodes: ${report.merged.nodes}`);
  console.log(`    entity resolutions: ${report.resolutions.length}`);
  for (const resolution of report.resolutions) {
    console.log(`      ${resolution.kind}: ${resolution.memberIds.length} entry-scoped nodes -> ${resolution.canonicalId}`);
  }
}

/**
 * One row per canonical concept, each carrying its mentioning wiki pages as an
 * ordered array. `expr.collect` groups and orders in SQL, so nothing here
 * rebuilds a Map or re-sorts what arrives.
 */
async function conceptMentions(store: DeepSurveyStore) {
  return store
    .query()
    .from("WikiPage", "w")
    .traverse("mentions", "m")
    .to("Concept", "c")
    .groupBy((ctx) => [ctx.c.name, ctx.c.category])
    .project((ctx) => ({
      name: ctx.c.name,
      category: ctx.c.category,
      mentions: expr.collect(
        { wikiKey: ctx.w.key, title: ctx.w.title, author: ctx.w.author },
        { orderBy: [{ expression: ctx.w.key }] },
      ),
    }))
    .orderBy((ctx) => ctx.c.name)
    .execute();
}

async function printCanonicalGraph(input: DeepSurveyConvergenceInput, canonical: DeepSurveyStore): Promise<void> {
  const concepts = await conceptMentions(canonical);

  console.log("\n  TypeGraph semantic projection");
  console.log(`    canonical concepts: ${concepts.length}`);
  for (const concept of concepts) {
    console.log(`      ${concept.name} (${concept.category})`);
    for (const mention of concept.mentions) {
      console.log(`        mentioned by ${mention.author}/${mention.wikiKey}: ${mention.title}`);
    }
  }

  const expected = distinctConceptCount(input.wiki);
  if (concepts.length !== expected) {
    throw new Error(`merge left ${concepts.length} canonical concepts; the wiki rows mention ${expected} distinct ones`);
  }
}

async function printCrossReferences(input: DeepSurveyConvergenceInput, canonical: DeepSurveyStore): Promise<void> {
  const links = await canonical
    .query()
    .from("WikiPage", "a")
    .traverse("crossReferences", "x")
    .to("WikiPage", "b")
    .select((ctx) => ({ from: ctx.a.title, to: ctx.b.title }))
    .orderBy("a", "key")
    .execute();

  console.log("\n  Cross references between canonical pages");
  for (const link of links) console.log(`    ${link.from} <-> ${link.to}`);

  if (links.length !== input.xrefs.length) {
    throw new Error(`canonical holds ${links.length} cross reference(s); shared state links ${input.xrefs.length} page pair(s)`);
  }
}

/** Describe the explorer row behind one entry-scoped concept node, or `undefined` if none produced it. */
function describeSource(input: DeepSurveyConvergenceInput, branchId: string, sourceId: string): string | undefined {
  const entry = input.wiki.find((candidate) => branchIdFor(candidate) === branchId);
  if (entry === undefined) return undefined;
  const mention = extractMentions(entry).find((candidate) => entryScopedConceptId(entry, candidate.concept) === sourceId);
  if (mention === undefined) return undefined;
  return `${entry.author}/${entry.key} @${entry.offset} via "${mention.alias}"`;
}

async function printProvenance(
  input: DeepSurveyConvergenceInput,
  canonical: DemoHistoryStore<typeof deepSurveyGraph>,
): Promise<void> {
  // Shares the canonical store's backend, so closing `canonical` closes it too.
  const provenanceStore = await openProvenanceStore(canonical);
  const concepts = await canonical
    .query()
    .from("Concept", "c")
    .orderBy((ctx) => ctx.c.name)
    .select((ctx) => ({ id: ctx.c.id, name: ctx.c.name }))
    .execute();

  console.log("\n  Canonical concept provenance");
  let attributed = 0;
  for (const concept of concepts) {
    const provenance = await readProvenance(provenanceStore, { canonicalId: concept.id, role: "node" });
    const sources = provenance
      .map((row) => describeSource(input, row.branchId, row.sourceId))
      .filter((source): source is string => source !== undefined)
      .sort((left, right) => left.localeCompare(right));
    attributed += sources.length;

    console.log(`    ${concept.name}`);
    for (const source of sources) console.log(`      ${source}`);
  }

  const mentionCount = input.wiki.flatMap(extractMentions).length;
  if (attributed !== mentionCount) {
    throw new Error(`provenance attributes ${attributed} mention(s) to a source row; the wiki rows hold ${mentionCount}`);
  }
}

runAsMain(import.meta.url, main);
