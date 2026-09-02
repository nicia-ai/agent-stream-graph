/**
 * Demo — a non-MCP driver that calls the same three tool functions
 * `server.ts` exposes over MCP, so the tools' correctness can be checked
 * (`pnpm demo`, exit 0) without an MCP client in the loop.
 *
 * The story:
 *   (a) two sources observe the same person under different spellings —
 *       `recall()` resolves both to one entity, aliases and all.
 *   (b) a title correction lands mid-stream — `believedAt()` reconstructs
 *       what was believed before it did.
 *   (c) two sources independently verify that person's identity — retracting
 *       one leaves the fact standing; retracting both kills it. `whySoFar()`
 *       narrates the provenance chain at each step.
 *   (d) the store is closed and reopened from the same files — memory
 *       survives the restart, including the retraction from (c).
 *
 * Every claim below is an assertion, not just a printed line: if the
 * library breaks, this script throws instead of reporting success.
 */
import { existsSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { createRetractionCapability } from "@nicia-ai/typegraph/provenance";

import { BACKGROUND_SCAN_SOURCE, ID_CHECK_SOURCE, JANE_EMAIL, STREAM_LINKEDIN } from "./fixtures.js";
import { retractionConfig, VERIFIED_PREDICATE } from "./graph.js";
import { openMemoryStore } from "./store.js";
import { believedAt, recall, whySoFar, type ResolvedEntity } from "./tools.js";

const DATA_DIR = join(dirname(fileURLToPath(import.meta.url)), "..", ".data", "demo");
const RULE = "━".repeat(74);

function requirePerson(entity: ResolvedEntity, context: string): asserts entity is Extract<ResolvedEntity, { kind: "Person" }> {
  if (entity.kind !== "Person") throw new Error(`${context}: expected a Person, got ${entity.kind}`);
}

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" MCP memory — entity-resolved, time-travelable, source-justified belief");
  console.log(RULE);

  // A clean slate every run: this demo asserts a specific before/after
  // narrative, so it cannot tolerate replaying stale state from a previous
  // invocation the way an idempotent `consume()` replay could.
  if (existsSync(DATA_DIR)) rmSync(DATA_DIR, { recursive: true, force: true });

  let memory = await openMemoryStore({ dataDir: DATA_DIR });
  if (!memory.seeded) throw new Error("demo setup is broken: a fresh data dir should always seed");

  try {
    // ----------------------------------------------------------
    console.log("\n" + RULE);
    console.log(" (a) recall — entity resolution across differently-spelled sightings");
    console.log(RULE);

    const byFullName = await recall(memory.store, "Jane Doe");
    const byAbbreviation = await recall(memory.store, "J. Doe");
    const byEmail = await recall(memory.store, JANE_EMAIL);
    if (!byFullName.found || !byAbbreviation.found || !byEmail.found) {
      throw new Error("demo setup is broken: all three handles should resolve");
    }
    requirePerson(byFullName.entity, "recall(Jane Doe)");
    requirePerson(byAbbreviation.entity, "recall(J. Doe)");
    requirePerson(byEmail.entity, "recall(email)");

    console.log(`\n  recall("Jane Doe")  -> id=${byFullName.entity.id}  aliases=[${byFullName.entity.aliases.join(", ")}]`);
    console.log(`  recall("J. Doe")    -> id=${byAbbreviation.entity.id}  aliases=[${byAbbreviation.entity.aliases.join(", ")}]`);
    console.log(`  recall("${JANE_EMAIL}") -> id=${byEmail.entity.id}`);
    console.log(`  employer: ${byFullName.employer ?? "(unknown)"}   verified: ${byFullName.verified}`);

    if (byFullName.entity.id !== byAbbreviation.entity.id || byFullName.entity.id !== byEmail.entity.id) {
      throw new Error("entity resolution failed: three handles for the same person resolved to different entities");
    }
    if (!byFullName.entity.aliases.includes("Jane Doe") || !byFullName.entity.aliases.includes("J. Doe")) {
      throw new Error(`entity resolution failed: aliases missing an observed spelling: [${byFullName.entity.aliases.join(", ")}]`);
    }
    console.log("\n  >>> Two sources, two spellings, one entity. <<<");

    // ----------------------------------------------------------
    console.log("\n" + RULE);
    console.log(" (b) believedAt — time travel across a mid-stream correction");
    console.log(RULE);

    const beforeCorrection = await believedAt(memory.store, memory.book, STREAM_LINKEDIN, "001");
    const afterCorrection = await believedAt(memory.store, memory.book, STREAM_LINKEDIN, "002");
    if (!beforeCorrection.found || !afterCorrection.found) {
      throw new Error("demo setup is broken: both linkedin-scrape offsets should have checkpoints");
    }
    const titleBefore = beforeCorrection.people.find((p) => p.email === JANE_EMAIL)?.title;
    const titleAfter = afterCorrection.people.find((p) => p.email === JANE_EMAIL)?.title;
    console.log(`\n  believedAt("${STREAM_LINKEDIN}", "001") rev ${beforeCorrection.revision}: title = "${titleBefore}"`);
    console.log(`  believedAt("${STREAM_LINKEDIN}", "002") rev ${afterCorrection.revision}: title = "${titleAfter}"`);

    if (titleBefore === undefined || titleAfter === undefined || titleBefore === titleAfter) {
      throw new Error(`time travel did not observe a correction: before="${titleBefore}" after="${titleAfter}"`);
    }
    if (byFullName.entity.title !== titleAfter) {
      throw new Error(`current belief ("${byFullName.entity.title}") should match the post-correction title ("${titleAfter}")`);
    }
    console.log("\n  >>> The pre-correction belief is still reconstructable by offset, even though <<<");
    console.log("      current belief has moved on.");

    // ----------------------------------------------------------
    console.log("\n" + RULE);
    console.log(" (c) whySoFar — provenance, then retraction");
    console.log(RULE);

    const initial = await whySoFar(memory.store, JANE_EMAIL, VERIFIED_PREDICATE);
    if (!initial.found) throw new Error("demo setup is broken: the verified fact should exist");
    console.log(`\n  whySoFar(jane, "${VERIFIED_PREDICATE}") -> currentlyHeld=${initial.currentlyHeld}`);
    for (const s of initial.supportedBy) console.log(`    supported by ${s.label} (retracted=${s.retracted})`);
    if (!initial.currentlyHeld || initial.supportedBy.length !== 2) {
      throw new Error(`demo setup is broken: expected 2 live sources, got ${JSON.stringify(initial.supportedBy)}`);
    }

    const provenance = createRetractionCapability(memory.store, retractionConfig);

    console.log(`\n  retract(${ID_CHECK_SOURCE}) — one of two sources withdrawn.`);
    await provenance.retract({ kind: "Source", id: ID_CHECK_SOURCE });
    const afterFirstRetraction = await whySoFar(memory.store, JANE_EMAIL, VERIFIED_PREDICATE);
    if (!afterFirstRetraction.found) throw new Error("fact vanished from the graph, not just from currency");
    console.log(`    currentlyHeld=${afterFirstRetraction.currentlyHeld} (should still hold — background-scan remains)`);
    for (const s of afterFirstRetraction.supportedBy) console.log(`    ${s.sourceId}: retracted=${s.retracted}`);
    if (!afterFirstRetraction.currentlyHeld) {
      throw new Error("fact died after retracting only one of two independent sources — it should have survived");
    }

    console.log(`\n  retract(${BACKGROUND_SCAN_SOURCE}) — the last remaining source withdrawn.`);
    await provenance.retract({ kind: "Source", id: BACKGROUND_SCAN_SOURCE });
    const afterSecondRetraction = await whySoFar(memory.store, JANE_EMAIL, VERIFIED_PREDICATE);
    if (!afterSecondRetraction.found) throw new Error("fact vanished from the graph, not just from currency");
    console.log(`    currentlyHeld=${afterSecondRetraction.currentlyHeld} (should no longer hold — no support left)`);
    for (const s of afterSecondRetraction.supportedBy) console.log(`    ${s.sourceId}: retracted=${s.retracted}`);
    if (afterSecondRetraction.currentlyHeld) {
      throw new Error("fact survived after every source that justified it was retracted");
    }
    console.log("\n  >>> A fact is only as strong as its live support. Losing the last source <<<");
    console.log("      un-derives it — its two sources are still on record, both now retracted.");

    // ----------------------------------------------------------
    console.log("\n" + RULE);
    console.log(" (d) restart — memory persists, including the retraction above");
    console.log(RULE);

    await memory.close();
    memory = await openMemoryStore({ dataDir: DATA_DIR });
    if (memory.seeded) {
      throw new Error("re-opening an already-seeded store should not seed again — persistence is broken");
    }

    const afterRestart = await recall(memory.store, "J. Doe");
    const provenanceAfterRestart = await whySoFar(memory.store, JANE_EMAIL, VERIFIED_PREDICATE);
    if (!afterRestart.found) throw new Error("persistence is broken: the entity did not survive a restart");
    requirePerson(afterRestart.entity, "recall(J. Doe) after restart");
    if (!afterRestart.entity.aliases.includes("Jane Doe")) {
      throw new Error("persistence is broken: an alias was lost across restart");
    }
    if (!provenanceAfterRestart.found || provenanceAfterRestart.currentlyHeld) {
      throw new Error("persistence is broken: the retraction from (c) did not survive the restart");
    }
    console.log(`\n  reopened from ${DATA_DIR}`);
    console.log(`  recall("J. Doe")   -> id=${afterRestart.entity.id}  aliases=[${afterRestart.entity.aliases.join(", ")}]`);
    console.log(`  whySoFar(verified) -> currentlyHeld=${provenanceAfterRestart.currentlyHeld}  (retraction survived the restart)`);
    console.log("\n  >>> Nothing above was re-derived from fixtures — it was read back from the <<<");
    console.log("      SQLite files the first run wrote.");

    console.log("\n" + RULE);
    console.log(" Entity resolution, time travel, and retraction-aware provenance —");
    console.log(" durable across restarts because the belief graph is, not because this");
    console.log(" script remembered anything.");
    console.log(RULE + "\n");
  } finally {
    await memory.close();
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
