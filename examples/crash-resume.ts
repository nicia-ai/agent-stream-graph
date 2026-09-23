/**
 * Demo — crash-resume: kill the materializer with a real `SIGKILL` mid-stream,
 * then prove the resumed run converges to exactly the same graph a clean,
 * uninterrupted run would have produced.
 *
 * Crash-safety is usually demonstrated with `stopAfter` — a scripted stop, not
 * a crash. This demo spawns a REAL child process
 * (`examples/crash-resume-worker.ts`) and sends it a REAL `SIGKILL`: no signal
 * handler, no `finally`, no chance to flush. Both stores are therefore
 * file-backed SQLite databases in a temp directory — an in-memory store would
 * die with the process and prove nothing.
 *
 * The kill lands at a controlled point, never racing work in flight: the worker
 * drains most of the stream normally, then commits ONE more change to the
 * belief store WITHOUT checkpointing it — the worst-case at-least-once crash
 * window, "the belief moved past the durable cursor" — reports that, and parks.
 *
 * This file then resumes IN-PROCESS against the same on-disk stores:
 * `consume()` re-delivers the primed change (absorbed as a no-op by
 * `coalesceUnchangedUpserts`, not duplicated) and drains the rest. The result
 * is asserted against a clean run over the same stream.
 *
 * Run with:  pnpm tsx examples/crash-resume.ts
 */
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import { checkpointGraph, consume, mockShapeSource, typeGraphCheckpoints } from "../src";
import { assertEqual, newStore, RULE, runAsMain, section } from "./_support";
import {
  CHANGES,
  currentRevision,
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
// Run the worker under tsx's loader rather than the `tsx` CLI: the CLI runs the
// script in a grandchild process, so a SIGKILL sent to it would kill the
// wrapper and orphan the actual worker.
const TSX_LOADER = import.meta.resolve("tsx");

type PrimedMessage = Extract<WorkerMessage, { type: "primed" }>;
type ExitStatus = Readonly<{ code: number | null; signal: NodeJS.Signals | null }>;

/**
 * Narrate the worker's stdout protocol until it reports the crash window is
 * primed. Throws if the worker's stdout closes first — it died before priming,
 * and waiting would only hang the demo.
 */
async function waitUntilPrimed(worker: ChildProcess): Promise<PrimedMessage> {
  if (worker.stdout === null) throw new Error("waitUntilPrimed(): worker was not spawned with a piped stdout");
  for await (const line of createInterface({ input: worker.stdout })) {
    const message = parseWorkerMessage(line);
    if (message.type === "drained") {
      console.log(`\n  worker: drained ${message.processed} changes normally — durable cursor at "${message.checkpoint ?? "(none)"}"`);
      continue;
    }
    console.log(`  worker: committed change @${message.offset} to belief, but did NOT checkpoint it`);
    console.log(`          (durable cursor still at "${message.checkpoint ?? "(none)"}" — this is the crash window)`);
    return message;
  }
  throw new Error("crash-resume: the worker exited before priming the crash window");
}

function parseWorkerMessage(line: string): WorkerMessage {
  try {
    return JSON.parse(line) as WorkerMessage;
  } catch {
    throw new Error(`crash-resume: the worker wrote a non-protocol line to stdout: ${line}`);
  }
}

function killAndWaitForExit(worker: ChildProcess, signal: NodeJS.Signals): Promise<ExitStatus> {
  if (worker.exitCode !== null || worker.signalCode !== null) {
    return Promise.resolve({ code: worker.exitCode, signal: worker.signalCode });
  }
  return new Promise((resolve) => {
    worker.once("exit", (code, exitSignal) => resolve({ code, signal: exitSignal }));
    worker.kill(signal);
  });
}

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" Crash-resume — a real SIGKILL mid-stream, then a byte-identical replay");
  console.log(RULE);

  const dir = await mkdtemp(join(tmpdir(), "crash-resume-"));
  const beliefDbPath = join(dir, "belief.db");
  const checkpointDbPath = join(dir, "checkpoints.db");

  let worker: ChildProcess | undefined;
  const stores: { close: () => Promise<void> }[] = [];

  // Deliberately NOT unref'd: a hang with nothing left on the event loop would
  // otherwise let Node exit 0 and pass the demo silently.
  const watchdog = setTimeout(() => {
    console.error(`\n  crash-resume: no result after ${HARD_TIMEOUT_MS}ms — killing the worker and aborting`);
    worker?.kill("SIGKILL");
    process.exit(1);
  }, HARD_TIMEOUT_MS);

  try {
    section("(a) Spawn a real materializer process against file-backed stores");
    console.log(`\n  belief db:     ${beliefDbPath}`);
    console.log(`  checkpoint db: ${checkpointDbPath}`);

    worker = spawn(process.execPath, ["--import", TSX_LOADER, WORKER_PATH, beliefDbPath, checkpointDbPath], {
      stdio: ["ignore", "pipe", "inherit"],
    });
    const primed = await waitUntilPrimed(worker);

    console.log("\n  >>> killing the materializer with SIGKILL now — no cleanup, no finally block <<<");
    const exit = await killAndWaitForExit(worker, "SIGKILL");
    if (exit.signal !== "SIGKILL") {
      throw new Error(`crash-resume: expected the worker to die by SIGKILL; it exited with code=${exit.code} signal=${exit.signal}`);
    }
    console.log(`  worker is dead (signal ${exit.signal}) — durable cursor frozen at "${primed.checkpoint ?? "(none)"}"`);

    section("(b) Resume in-process against the SAME on-disk stores");

    const { belief, cursor, book } = await openFileBackedStores(beliefDbPath, checkpointDbPath);
    stores.push(belief, cursor);

    const revisionAfterReopen = await currentRevision(belief);
    assertEqual(revisionAfterReopen, primed.revision, "recorded revision after reopening the killed worker's store");
    console.log(`\n  reopened the SAME on-disk stores after the kill — recorded revision still ${revisionAfterReopen} (nothing lost, nothing extra)`);

    const resumed = await consume({ source: mockShapeSource(STREAM_NAME, CHANGES), store: belief, checkpoints: book, project });
    const redelivered = CHANGES.length - PRIME_INDEX;
    assertEqual(resumed.fromOffset, primed.checkpoint, "offset the resume started from");
    assertEqual(resumed.processed, redelivered, "changes processed on resume");
    assertEqual(resumed.lastOffset, CHANGES.at(-1)?.offset, "offset the resume finished at");
    console.log(`  resumed: processed ${resumed.processed} changes (cursor "${resumed.fromOffset}" → "${resumed.lastOffset}")`);
    console.log(`    → change @${primed.offset} was RE-DELIVERED (it was already in belief); everything after it is new`);

    // The re-delivered change coalesces to a no-op, so it allocates no revision.
    const revisionDelta = (await currentRevision(belief)) - revisionAfterReopen;
    assertEqual(revisionDelta, redelivered - 1, "recorded revisions allocated by the resume");
    console.log(`  recorded clock advanced by ${revisionDelta}, not ${redelivered} — the redelivered change was ABSORBED, not reapplied as new history`);

    const resumedRows = await rows(belief);
    assertEqual(resumedRows.length, CHANGES.length, "belief rows after resume (a mismatch is a duplicate or a loss)");
    console.log(`  belief row count: ${resumedRows.length} (matches ${CHANGES.length} distinct keys — no duplicates)`);

    section("(c) Compare against a clean, uninterrupted run of the same stream");

    const cleanBelief = await newStore(observationGraph, true);
    const cleanCursor = await newStore(checkpointGraph);
    stores.push(cleanBelief, cleanCursor);
    await consume({
      source: mockShapeSource(STREAM_NAME, CHANGES),
      store: cleanBelief,
      checkpoints: typeGraphCheckpoints(cleanCursor),
      project,
    });
    const cleanRows = await rows(cleanBelief);
    assertEqual(serializeRows(resumedRows), serializeRows(cleanRows), "resumed graph vs a clean, uninterrupted run");
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
    clearTimeout(watchdog);
    if (worker !== undefined && worker.exitCode === null && worker.signalCode === null) worker.kill("SIGKILL");
    await Promise.allSettled(stores.map((store) => store.close()));
    await rm(dir, { recursive: true, force: true });
  }
}

runAsMain(import.meta.url, main);
