/**
 * Demo — crash-resume: kill the materializer with a real `SIGKILL` mid-stream,
 * then prove the resumed run converges to exactly the same graph a clean,
 * uninterrupted run would have produced.
 *
 * Every "crash-safe, idempotent resume" claim in this space gets demonstrated
 * with `stopAfter` — a scripted stop, not a crash. This demo instead forks a
 * REAL child process (`examples/crash-resume-worker.ts`, run through tsx) and
 * sends it a REAL `SIGKILL`: no signal handler, no `finally`, no chance to
 * flush. The belief store and the checkpoint store are therefore file-backed
 * SQLite databases (`createLocalSqliteBackend({ path })`) in a temp directory —
 * an in-memory store would die with the process and prove nothing.
 *
 * The worker is killed at a controlled, non-flaky point: it drains most of the
 * stream normally, then manually commits ONE more change directly to the
 * belief store WITHOUT checkpointing it — the worst-case at-least-once crash
 * window ("the belief moved past the durable cursor"). It signals the parent
 * over a stdout JSON line and hangs; the parent kills it the instant that
 * signal arrives, so the demo never races the kill against work in flight.
 *
 * After the kill, this file resumes IN-PROCESS against the same on-disk
 * stores: `consume()` re-delivers the primed change (absorbed as a no-op by
 * `coalesceUnchangedUpserts`, not duplicated) and drains the rest. The result
 * is asserted, field for field, against a clean run over the same stream.
 *
 * Run with:  pnpm tsx examples/crash-resume.ts
 */
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import { recordedInstantRevision } from "@nicia-ai/typegraph";

import { checkpointGraph, consume, mockShapeSource, typeGraphCheckpoints } from "../src";
import { newStore, runAsMain } from "./_support";
import {
  CHANGES,
  observationGraph,
  openFileBackedStores,
  PRIME_INDEX,
  project,
  rows,
  serializeRows,
  STREAM_NAME,
  type WorkerMessage,
} from "./crash-resume-worker";

const HARD_TIMEOUT_MS = 30_000;

const WORKER_PATH = fileURLToPath(new URL("./crash-resume-worker.ts", import.meta.url));
const TSX_CLI = createRequire(import.meta.url).resolve("tsx/cli");

type PrimedMessage = Extract<WorkerMessage, { type: "primed" }>;

/**
 * Spawn the worker through tsx, listen to its stdout protocol, and resolve the
 * instant it reports the crash window is primed. Rejects if the worker dies
 * (or errors) before that — a real bug there should fail loud, not hang the
 * demo waiting for a signal that will never come.
 */
async function primeForCrash(child: ChildProcess): Promise<PrimedMessage> {
  if (child.stdout === null) {
    throw new Error("primeForCrash(): worker was not spawned with a piped stdout");
  }
  const rl = createInterface({ input: child.stdout });
  try {
    return await new Promise<PrimedMessage>((resolve, reject) => {
      rl.on("line", (line) => {
        const message = JSON.parse(line) as WorkerMessage;
        switch (message.type) {
          case "ready-normal":
            console.log(`\n  worker: drained ${message.processed} changes normally — durable cursor at "${message.checkpoint ?? "(none)"}"`);
            break;
          case "primed":
            console.log(`  worker: committed change @${message.offset} to belief, but did NOT checkpoint it`);
            console.log(`          (durable cursor still at "${message.checkpoint ?? "(none)"}" — this is the crash window)`);
            resolve(message);
            break;
        }
      });
      child.once("exit", (code, signal) => {
        reject(new Error(`worker exited early (code=${code ?? "null"}, signal=${signal ?? "null"}) before priming the crash window`));
      });
      child.once("error", reject);
    });
  } finally {
    rl.close();
  }
}

/** Kill `worker` and wait for its exit event, reporting the exact code/signal. */
function killAndWaitForExit(worker: ChildProcess, signal: NodeJS.Signals): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
  return new Promise((resolve) => {
    worker.once("exit", (code, exitSignal) => resolve({ code, signal: exitSignal }));
    worker.kill(signal);
  });
}

const RULE = "━".repeat(74);
function section(title: string): void {
  console.log("\n" + RULE);
  console.log(` ${title}`);
  console.log(RULE);
}

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" Crash-resume — a real SIGKILL mid-stream, then a byte-identical replay");
  console.log(RULE);

  const dir = await mkdtemp(join(tmpdir(), "crash-resume-"));
  const beliefDbPath = join(dir, "belief.db");
  const checkpointDbPath = join(dir, "checkpoints.db");

  let child: ChildProcess | undefined;
  const stores: { close: () => Promise<void> }[] = [];

  const timer = setTimeout(() => {
    console.error("\n  crash-resume: hard timeout exceeded — force-killing the worker and aborting");
    child?.kill("SIGKILL");
    process.exit(1);
  }, HARD_TIMEOUT_MS);
  // A hard-timeout setTimeout would otherwise keep the process alive on its
  // own; unref it so a normal successful run exits the instant main() returns.
  timer.unref();

  try {
    // ----------------------------------------------------------
    // (a) A real child process, file-backed stores, driven to the crash window
    // ----------------------------------------------------------
    section("(a) Spawn a real materializer process against file-backed stores");
    console.log(`\n  belief db:     ${beliefDbPath}`);
    console.log(`  checkpoint db: ${checkpointDbPath}`);

    child = spawn(process.execPath, [TSX_CLI, WORKER_PATH, beliefDbPath, checkpointDbPath], {
      stdio: ["ignore", "pipe", "inherit"],
    });
    const primed = await primeForCrash(child);

    console.log("\n  >>> killing the materializer with SIGKILL now — no cleanup, no finally block <<<");
    const exit = await killAndWaitForExit(child, "SIGKILL");
    if (exit.signal !== "SIGKILL") {
      throw new Error(`crash-resume: expected the worker to die by SIGKILL; it exited with code=${exit.code ?? "null"} signal=${exit.signal ?? "null"}`);
    }
    console.log(`  worker is dead (signal ${exit.signal}) — durable cursor frozen at "${primed.checkpoint ?? "(none)"}"`);

    // ----------------------------------------------------------
    // (b) Resume in-process — drain what the crash left behind
    // ----------------------------------------------------------
    section("(b) Resume in-process against the SAME on-disk stores");

    const { belief: resumedBelief, cursor: resumedCursor, book: resumedBook } = await openFileBackedStores(beliefDbPath, checkpointDbPath);
    stores.push(resumedBelief, resumedCursor);

    const revisionAtPrime = primed.revision;
    const anchorAfterReopen = await resumedBelief.recordedNow();
    const revisionAfterReopen = anchorAfterReopen === undefined ? -1 : recordedInstantRevision(anchorAfterReopen);
    if (revisionAfterReopen !== revisionAtPrime) {
      throw new Error(`crash-resume: reopening the file-backed belief store should read back revision ${revisionAtPrime}, got ${revisionAfterReopen}`);
    }
    console.log(`\n  reopened the SAME on-disk stores after the kill — recorded revision still ${revisionAfterReopen} (nothing lost, nothing extra)`);

    const resumedSource = mockShapeSource(STREAM_NAME, CHANGES);
    const resumed = await consume({ source: resumedSource, store: resumedBelief, checkpoints: resumedBook, project });

    const expectedRedelivered = CHANGES.length - PRIME_INDEX;
    const lastChange = CHANGES.at(-1);
    if (lastChange === undefined) throw new Error("crash-resume: CHANGES must be non-empty");
    if (resumed.processed !== expectedRedelivered) {
      throw new Error(`crash-resume: expected resume to process ${expectedRedelivered} changes, processed ${resumed.processed}`);
    }
    if (resumed.fromOffset !== primed.checkpoint) {
      throw new Error(`crash-resume: expected resume to start from "${primed.checkpoint ?? "(none)"}", started from "${resumed.fromOffset ?? "(none)"}"`);
    }
    if (resumed.lastOffset !== lastChange.offset) {
      throw new Error(`crash-resume: expected resume to finish at "${lastChange.offset}", finished at "${resumed.lastOffset ?? "(none)"}"`);
    }
    console.log(`  resumed: processed ${resumed.processed} changes (cursor "${resumed.fromOffset ?? "(none)"}" → "${resumed.lastOffset ?? "(none)"}")`);
    console.log(`    → change @${primed.offset} was RE-DELIVERED (it was already in belief); everything after it is new`);

    const anchorAfterResume = await resumedBelief.recordedNow();
    const revisionAfterResume = anchorAfterResume === undefined ? -1 : recordedInstantRevision(anchorAfterResume);
    const revisionDelta = revisionAfterResume - revisionAfterReopen;
    const expectedDelta = expectedRedelivered - 1; // the redelivered change coalesces to a no-op
    if (revisionDelta !== expectedDelta) {
      throw new Error(`crash-resume: expected the recorded clock to advance by ${expectedDelta}, advanced by ${revisionDelta}`);
    }
    console.log(`  recorded clock advanced by ${revisionDelta}, not ${expectedRedelivered} — the redelivered change was ABSORBED, not reapplied as new history`);

    const resumedRows = await rows(resumedBelief);
    if (resumedRows.length !== CHANGES.length) {
      throw new Error(`crash-resume: expected ${CHANGES.length} rows after resume, found ${resumedRows.length} — a duplicate or a loss`);
    }
    console.log(`  belief row count: ${resumedRows.length} (matches ${CHANGES.length} distinct keys — no duplicates)`);

    // ----------------------------------------------------------
    // (c) Compare against a clean, uninterrupted run
    // ----------------------------------------------------------
    section("(c) Compare against a clean, uninterrupted run of the same stream");

    const cleanBelief = await newStore(observationGraph, true);
    const cleanCursorStore = await newStore(checkpointGraph);
    stores.push(cleanBelief, cleanCursorStore);
    const cleanBook = typeGraphCheckpoints(cleanCursorStore);
    const cleanSource = mockShapeSource(STREAM_NAME, CHANGES);
    await consume({ source: cleanSource, store: cleanBelief, checkpoints: cleanBook, project });
    const cleanRows = await rows(cleanBelief);

    const resumedSerialized = serializeRows(resumedRows);
    const cleanSerialized = serializeRows(cleanRows);
    if (resumedSerialized !== cleanSerialized) {
      throw new Error(
        `crash-resume: resumed graph diverged from a clean, uninterrupted run.\n  resumed: ${resumedSerialized}\n  clean:   ${cleanSerialized}`,
      );
    }
    console.log(`\n  resumed graph === clean graph (${cleanRows.length} rows, byte-identical sorted serialization)`);

    console.log("\n" + RULE);
    console.log(" A real SIGKILL, a durable cursor left behind, and a resume that lands");
    console.log(" on exactly the graph an uninterrupted run would have produced.");
    console.log(RULE);
    console.log("\n  Contrast with `pnpm demo:exactly-once`: there, the projection and the");
    console.log("  cursor advance in ONE transaction, so a crash leaves nothing to re-deliver");
    console.log("  at all. Here, belief and cursor are two separate commits — the gap between");
    console.log("  them is real, and idempotent projection plus coalescing is what closes it.");
    console.log(RULE + "\n");
  } finally {
    clearTimeout(timer);
    if (child !== undefined && child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
    }
    await Promise.allSettled(stores.map((store) => store.close()));
    await rm(dir, { recursive: true, force: true });
  }
}

runAsMain(import.meta.url, main);
