// Runs every self-contained demo in `examples/` and fails if one does not exit 0.
//
// The demos are teaching artifacts, which is exactly why they rot: nothing else
// in CI reads them, so a library change that breaks a narrative breaks it
// silently. They are deterministic and offline by construction — no network, no
// service, no API key — so running the whole set is cheap enough to do on every
// push, and that is what keeps the showcase honest.
//
// Each demo asserts its own invariants internally (a demo that would still
// print success if the library broke is worthless), so a non-zero exit here
// means a real regression, not just a changed line of output.

import { spawn } from "node:child_process";
import { readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const EXAMPLES = join(ROOT, "examples");

/**
 * Files in `examples/` that are not demo entry points.
 *
 * `_support.ts` is shared boilerplate (underscore-prefixed by convention),
 * `crash-resume-worker.ts` is a child process its parent spawns, and
 * `deep-survey-live.ts` needs a running Electric Deep Survey swarm — it is the
 * one demo that cannot be self-contained, by design.
 */
const NOT_ENTRY_POINTS = new Set(["_support.ts", "crash-resume-worker.ts", "deep-survey-live.ts"]);

/** Generous enough for the process-spawning demos, short enough to fail a hang. */
const TIMEOUT_MS = 180_000;

const IS_WINDOWS = process.platform === "win32";

/** The demo currently running, so an interrupted run can take it down too. */
let running;

/**
 * SIGKILL the demo AND everything it spawned. Killing only the direct child
 * would orphan descendants (crash-resume's worker inherits our stderr), and an
 * orphan holding our pipes open keeps `close` from ever firing, so a hung demo
 * would hang the run despite the timeout. Each demo leads its own process group
 * (`detached`), which a negative pid signals as a whole.
 */
function killDemo(child) {
  try {
    if (IS_WINDOWS) child.kill("SIGKILL");
    else process.kill(-child.pid, "SIGKILL");
  } catch {
    // Already exited.
  }
}

function runDemo(file) {
  return new Promise((resolve) => {
    const started = Date.now();
    // `node --import tsx` rather than `npx tsx`: no wrapper processes between
    // us and the demo, and no chance of npx reaching the network for a package.
    const child = spawn(process.execPath, ["--import", "tsx", join(EXAMPLES, file)], {
      cwd: ROOT,
      stdio: ["ignore", "pipe", "pipe"],
      detached: !IS_WINDOWS,
    });
    running = child;
    let output = "";
    let timedOut = false;
    child.stdout.on("data", (chunk) => (output += chunk));
    child.stderr.on("data", (chunk) => (output += chunk));
    // A spawn failure emits `error` and then `close`, so recording it is enough.
    child.on("error", (error) => (output += `\n[runner] failed to spawn: ${error.message}\n`));

    const timer = setTimeout(() => {
      timedOut = true;
      killDemo(child);
    }, TIMEOUT_MS);
    child.on("close", (code, signal) => {
      clearTimeout(timer);
      running = undefined;
      resolve({ file, code, signal, timedOut, output, ms: Date.now() - started });
    });
  });
}

function describeExit({ code, signal, timedOut }) {
  if (timedOut) return `timed out after ${TIMEOUT_MS / 1000}s`;
  return signal === null ? `exit ${code}` : `signal ${signal}`;
}

for (const interrupt of ["SIGINT", "SIGTERM"]) {
  process.on(interrupt, () => {
    if (running !== undefined) killDemo(running);
    process.exit(1);
  });
}

const demos = readdirSync(EXAMPLES)
  .filter((file) => file.endsWith(".ts") && !NOT_ENTRY_POINTS.has(file))
  .sort();

console.log(`Running ${demos.length} demos from examples/\n`);

const failures = [];
for (const file of demos) {
  const result = await runDemo(file);
  const ok = result.code === 0;
  if (!ok) failures.push(result);
  const status = ok ? "ok  " : "FAIL";
  console.log(`  ${status} ${file.padEnd(32)} ${String(result.ms).padStart(6)}ms  (${describeExit(result)})`);
}

if (failures.length > 0) {
  for (const failure of failures) {
    console.error(`\n=== ${failure.file} failed (${describeExit(failure)}) ===\n${failure.output}`);
  }
  console.error(`\n${failures.length} of ${demos.length} demos failed.`);
  process.exit(1);
}

console.log(`\nAll ${demos.length} demos passed.`);
