// Release tarball export smoke test.
//
// Runs against the *packed* @nicia-ai/agent-stream-graph tarball installed into
// a throwaway sandbox (see .github/workflows/ci.yml). It imports every public
// subpath declared in the package's `exports` map and asserts a representative
// named export resolves.
//
// This catches packaging regressions a type check cannot, and one of them was
// real: `tsc` emits relative specifiers verbatim, so extensionless imports in
// `src/` shipped as extensionless imports in `dist/`, which Node's ESM resolver
// refuses. The whole package was unimportable while `pnpm typecheck`, `pnpm
// test`, and every demo stayed green — because those reach the code through
// vitest and tsx, which resolve extensionless specifiers that Node does not.
//
// Runs under plain `node` (no tsx/TypeScript) because the sandbox holds only
// the installed tarball plus its peer dependencies. That is the point: it must
// exercise the same resolver a consumer's Node does.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

// Resolution goes through `import.meta.resolve`, never `createRequire`: this
// package is ESM-only, so its `exports` map declares no `require` condition and
// CJS resolution of it fails by design. Using the ESM resolver is also the
// point — it is the one a consumer's `import` actually runs.
const resolves = (specifier) => {
  try {
    return import.meta.resolve(specifier);
  } catch {
    return undefined;
  }
};

const PACKAGE_NAME = "@nicia-ai/agent-stream-graph";

// One representative export per public subpath. Keep this in lockstep with the
// `exports` map in package.json — the manifest cross-check below fails the
// build if a subpath is added to `exports` without a matching entry here.
const PUBLIC_SUBPATHS = [{ subpath: "", expectedExport: "consume" }];

// The root entrypoint re-exports every module in `src/`. Naming one value from
// each keeps a module that fails to resolve — the extensionless-import failure
// mode — from hiding behind its neighbours, since a single missing specifier
// takes down the whole barrel.
const ROOT_EXPORTS = [
  "checkpointGraph",
  "typeGraphCheckpoints",
  "typeGraphAdoptingCheckpoints",
  "consume",
  "DEFAULT_MAX_BATCH_SIZE",
  "InvalidMaxBatchSizeError",
  "ProjectorRecordedNothingError",
  "durableStateSource",
  "isStateChangeEvent",
  "StateResetError",
  "durableStreamSource",
  "DurableStreamRetentionError",
  "forkStream",
  "forkPointFor",
  "StreamForkError",
  "applyGraphEvents",
  "graphEmitter",
  "graphProjector",
  "OP_NODE_UPSERT",
  "OP_NODE_REMOVE",
  "OP_EDGE_UPSERT",
  "OP_EDGE_REMOVE",
  "compareOffsets",
  "composeOffset",
  "parseCompositeOffset",
  "STREAM_START",
  "electricShapeSource",
  "mockShapeSource",
  "ElectricControlError",
  "ElectricMustRefetchError",
  "ensureSubscription",
  "claimSubscription",
  "ackSubscription",
  "releaseSubscription",
  "deleteSubscription",
  "consumeSubscribed",
  "SubscriptionClaimedError",
  "SubscriptionFencedError",
  "SubscriptionRequestError",
];

function findPackageManifest(startPath, packageName) {
  let currentDir = dirname(startPath);
  while (currentDir !== dirname(currentDir)) {
    try {
      const manifest = JSON.parse(readFileSync(join(currentDir, "package.json"), "utf8"));
      if (manifest.name === packageName) return manifest;
    } catch {
      // No package.json here (or unreadable) — keep walking toward the root.
    }
    currentDir = dirname(currentDir);
  }
  throw new Error(`Could not locate package.json for ${packageName}`);
}

function toSubpath(exportsKey) {
  return exportsKey === "." ? "" : exportsKey.replace(/^\.\//, "/");
}

const failures = [];

// Guard against the export surface growing past this test: every subpath the
// installed package declares must be covered by PUBLIC_SUBPATHS.
const entrypoint = resolves(PACKAGE_NAME);
if (entrypoint === undefined) {
  console.error(`Tarball export smoke test failed:\n  cannot resolve "${PACKAGE_NAME}" from the sandbox`);
  process.exit(1);
}
const manifest = findPackageManifest(fileURLToPath(entrypoint), PACKAGE_NAME);
const covered = new Set(PUBLIC_SUBPATHS.map((entry) => entry.subpath));
const uncovered = Object.keys(manifest.exports ?? {})
  .map(toSubpath)
  .filter((subpath) => !covered.has(subpath));
if (uncovered.length > 0) {
  failures.push(`exports map has subpaths not covered by smoke test: ${uncovered.join(", ")}`);
}

let rootModule;
for (const { subpath, expectedExport } of PUBLIC_SUBPATHS) {
  const specifier = `${PACKAGE_NAME}${subpath}`;
  try {
    const imported = await import(specifier);
    if (subpath === "") rootModule = imported;
    if (typeof imported[expectedExport] === "undefined") {
      failures.push(`ESM ${specifier}: missing export "${expectedExport}"`);
    }
  } catch (error) {
    failures.push(`ESM ${specifier}: ${error.message}`);
  }
}

if (rootModule !== undefined) {
  const missing = ROOT_EXPORTS.filter((name) => typeof rootModule[name] === "undefined");
  if (missing.length > 0) failures.push(`root entrypoint is missing exports: ${missing.join(", ")}`);
}

// The adapters lazy-import their optional peers, so the module graph must load
// with neither installed — that is what makes them optional rather than
// required. A top-level import of one would surface here as a load failure.
if (rootModule !== undefined) {
  for (const name of ["@electric-sql/client", "@durable-streams/client"]) {
    if (resolves(name) !== undefined) {
      failures.push(`optional peer "${name}" is installed in the sandbox — the smoke no longer proves lazy loading`);
    }
  }
}

if (failures.length > 0) {
  console.error(`Tarball export smoke test failed:\n  ${failures.join("\n  ")}`);
  process.exit(1);
}

console.log(
  `Tarball export smoke test passed: ${PUBLIC_SUBPATHS.length} subpath(s), ${ROOT_EXPORTS.length} root exports, under plain Node.`,
);
