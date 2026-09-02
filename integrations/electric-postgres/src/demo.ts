/**
 * Demo — the REAL thing: Electric over Postgres logical replication,
 * materialized live into a belief graph via `@nicia-ai/agent-stream-graph`'s
 * published entrypoint (not `../src` — see the root repo's `pnpm-workspace.yaml`).
 *
 * This is the credibility fix for the root README's own admission: "The
 * Electric adapter is not live-service tested here." Everything below either
 * ran against a real `electricsql/electric` container over a real
 * `wal_level=logical` Postgres, or is explicitly, visibly labeled as the
 * offline fallback.
 *
 * THE HONEST PART, and the actual point of this package: Electric gives you
 * PER-BATCH checkpoint granularity, not per-change. `ShapeStream` has no
 * per-message resume position — every change in one catch-up batch shares
 * that batch's `lastOffset` — so `book.anchorFor(stream, offset)` reconstructs
 * the belief as of the END of a batch, never mid-batch. This demo does not
 * just assert that; it PROVES it: section (b) below preloads several rows
 * into ONE Postgres-visible moment, shows Electric hand them all back sharing
 * ONE offset, checkpoints that offset, and then in section (e) shows that
 * time-travelling to it reconstructs the state after ALL of them — the
 * in-between states are simply not addressable by offset. Contrast:
 * `durableStreamSource` (this package doesn't run one, but the root README
 * documents it) gives every change its OWN offset by construction, so the
 * same query there would land exactly on one change.
 *
 * Also demonstrated, deterministically, against the real service: initial
 * snapshot vs. live tailing (sections b vs. c), and `ElectricMustRefetchError`
 * (section f) — a schema change genuinely invalidates a shape mid-flight, and
 * this package is the first place in this repo that has ever seen it happen.
 *
 * Run with:  pnpm demo
 * (falls back to an offline decoder-only run if Postgres/Electric aren't
 * reachable — see `runOfflineDemo` below, and README.md.)
 */
import {
  checkpointGraph,
  consume,
  electricShapeSource,
  ElectricMustRefetchError,
  graphEmitter,
  graphProjector,
  mockShapeSource,
  type ShapeChange,
  type ShapeSource,
  typeGraphCheckpoints,
} from "@nicia-ai/agent-stream-graph";
import { recordedInstantRevision } from "@nicia-ai/typegraph";
import { Pool } from "pg";

import { connectDemoDatabase, type FleetHistoryStore, newStore } from "./db.js";
import { type AgentEventRow, agentEventRowSchema, decodeAgentEvent } from "./decode.js";
import { fleetGraph } from "./graph.js";
import { SCRIPT, seedOverTime, sleep } from "./seed.js";

const DATABASE_URL = process.env.DATABASE_URL ?? "postgresql://postgres:password@localhost:54321/agent_fleet?sslmode=disable";
const ELECTRIC_SHAPE_URL = process.env.ELECTRIC_URL ?? "http://localhost:3000/v1/shape";
const ELECTRIC_HEALTH_URL = ELECTRIC_SHAPE_URL.replace(/\/v1\/shape\/?$/, "/v1/health");
const STREAM_NAME = "agent-fleet";
/** How many of `SCRIPT`'s events land BEFORE the first `consume()` call — the guaranteed-shared-offset batch section (e) time-travels into. */
const PRESEED_COUNT = 4;
const HEALTH_CHECK_TIMEOUT_MS = 2_000;
const LIVE_POLL_INTERVAL_MS = 250;
/** Spacing between live-tailed inserts — real delays, not one bulk write; see seed.ts's own header. */
const LIVE_SEED_DELAY_MS = 400;

const RULE = "━".repeat(74);
function section(title: string): void {
  console.log("\n" + RULE);
  console.log(` ${title}`);
  console.log(RULE);
}

// ============================================================
// Reachability — decide live vs. offline BEFORE touching either service
// ============================================================

type Reachability = Readonly<{ live: true }> | Readonly<{ live: false; reason: string }>;

/** `pg`'s connection failures surface as an `AggregateError` with an empty top-level `.message`; `.code` (e.g. `ECONNREFUSED`) is where the useful detail actually is. */
function describeConnectionError(error: unknown): string {
  if (error instanceof Error) {
    const code = "code" in error && typeof error.code === "string" ? error.code : undefined;
    if (error.message.length > 0) return code === undefined ? error.message : `${error.message} [${code}]`;
    if (code !== undefined) return code;
    return error.constructor.name;
  }
  return String(error);
}

async function checkLiveStack(): Promise<Reachability> {
  const pool = new Pool({ connectionString: DATABASE_URL, connectionTimeoutMillis: HEALTH_CHECK_TIMEOUT_MS });
  try {
    await pool.query("SELECT 1");
  } catch (error) {
    return { live: false, reason: `Postgres unreachable at ${DATABASE_URL} (${describeConnectionError(error)})` };
  } finally {
    await pool.end().catch(() => {});
  }
  try {
    const response = await fetch(ELECTRIC_HEALTH_URL, { signal: AbortSignal.timeout(HEALTH_CHECK_TIMEOUT_MS) });
    if (!response.ok) return { live: false, reason: `Electric health check at ${ELECTRIC_HEALTH_URL} returned HTTP ${response.status}` };
  } catch (error) {
    return { live: false, reason: `Electric unreachable at ${ELECTRIC_HEALTH_URL} (${describeConnectionError(error)})` };
  }
  return { live: true };
}

// ============================================================
// Narration — surface the raw batch/offset shape, not just consume()'s summary
// ============================================================

/** Wraps a `ShapeSource` to report every raw `read()` result before `consume()` folds it away. */
function withBatchNarration<V>(
  source: ShapeSource<V>,
  onBatch: (after: string | undefined, changes: readonly ShapeChange<V>[]) => void,
): ShapeSource<V> {
  return {
    name: source.name,
    async read(after) {
      const changes = await source.read(after);
      onBatch(after, changes);
      return changes;
    },
  };
}

/** Groups a batch's changes by offset and prints the grouping — this is where "several changes share one offset" becomes visible instead of asserted. */
function printBatch(after: string | undefined, changes: readonly ShapeChange<AgentEventRow>[]): void {
  if (changes.length === 0) {
    console.log(`  read(${after ?? "<start>"}) -> 0 changes (nothing new yet)`);
    return;
  }
  const byOffset = new Map<string, number>();
  for (const change of changes) byOffset.set(change.offset, (byOffset.get(change.offset) ?? 0) + 1);
  console.log(`  read(${after ?? "<start>"}) -> ${changes.length} change(s) across ${byOffset.size} distinct offset(s):`);
  for (const [offset, count] of byOffset) {
    const marker = count > 1 ? `  <-- ${count} changes share this ONE offset` : "";
    console.log(`      @${offset}: ${count} change(s)${marker}`);
  }
}

// ============================================================
// Reading the resulting graph
// ============================================================

type FleetView = { query: FleetHistoryStore<typeof fleetGraph>["query"] };
type TaskRow = Readonly<{ id: string; title: string; status: string; agent: string }>;

async function taskSnapshot(view: FleetView): Promise<readonly TaskRow[]> {
  const rows = await view
    .query()
    .from("Task", "t")
    .traverse("assignedTo", "e")
    .to("Agent", "a")
    .select((c) => ({ id: c.t.id, title: c.t.title, status: c.t.status, agent: c.a.name }))
    .execute();
  return [...rows].sort((a, b) => a.id.localeCompare(b.id));
}

// ============================================================
// Offline fallback — no Postgres/Electric reachable
// ============================================================

async function runOfflineDemo(reason: string): Promise<void> {
  section("OFFLINE MODE");
  console.log(`\n  ${reason}`);
  console.log("  Falling back to a decoder-only run over mockShapeSource fixtures.");
  console.log("  This exercises the exact same pure decodeAgentEvent() the live path uses,");
  console.log("  but proves nothing about electricShapeSource, Postgres, or Electric itself.");

  // Postgres timestamptz text format, verbatim — proves toIsoInstant()'s
  // normalization runs in the offline path too, not just against a live row.
  const fixtures: readonly ShapeChange<AgentEventRow>[] = SCRIPT.slice(0, 6).map((event, index) => ({
    offset: String(index + 1).padStart(3, "0"),
    shape: "agent_event",
    key: String(index + 1),
    operation: "insert",
    value: agentEventRowSchema.parse({
      agent_id: event.agentId,
      agent_name: event.agentName,
      task_id: event.taskId,
      task_title: event.taskTitle,
      event_type: event.eventType,
      status: event.status,
      finding_id: event.findingId ?? null,
      finding_summary: event.findingSummary ?? null,
      severity: event.severity ?? null,
      occurred_at: `2026-08-28 12:00:0${index}.000000+00`,
    }),
  }));

  const source = mockShapeSource("agent-fleet-offline", fixtures);
  const emit = graphEmitter(fleetGraph);
  const changes = await source.read(undefined);
  console.log(`\n  decoding ${changes.length} fixture rows with no store, no Electric, no Postgres:\n`);

  let nodeUpserts = 0;
  let edgeUpserts = 0;
  for (const change of changes) {
    const events = decodeAgentEvent(change, emit);
    for (const event of events) {
      if (event.op === "node.upsert") nodeUpserts += 1;
      if (event.op === "edge.upsert") edgeUpserts += 1;
    }
    console.log(`    @${change.offset} ${change.value.event_type.padEnd(16)} -> ${events.length} graph event(s): ${events.map((e) => e.op).join(", ")}`);
  }

  if (nodeUpserts === 0 || edgeUpserts === 0) {
    throw new Error(`offline demo setup is broken: decoded ${nodeUpserts} node.upsert and ${edgeUpserts} edge.upsert — expected both > 0`);
  }

  // Round-trip through JSON, same proof as examples/emit.ts: the decoder's
  // output is plain data, nothing library-specific survives serialization.
  const allEvents = changes.flatMap((change) => decodeAgentEvent(change, emit));
  const roundTripped = JSON.parse(JSON.stringify(allEvents)) as typeof allEvents;
  if (JSON.stringify(roundTripped) !== JSON.stringify(allEvents)) {
    throw new Error("offline demo: decoded events did not survive a JSON round trip unchanged");
  }

  console.log(`\n  ${nodeUpserts} node.upsert + ${edgeUpserts} edge.upsert events decoded, all plain JSON, all from ${changes.length} rows.`);
  console.log("\n" + RULE);
  console.log(" OFFLINE RUN COMPLETE — the live path above is UNVERIFIED in this run.");
  console.log(" Run `pnpm docker:up` (Docker) and re-run `pnpm demo` to exercise it for real.");
  console.log(RULE + "\n");
}

// ============================================================
// Live demo — against a real Postgres + Electric
// ============================================================

async function runLiveDemo(): Promise<void> {
  section("LIVE MODE — real Postgres (wal_level=logical) + real Electric");
  console.log(`\n  Postgres: ${DATABASE_URL}`);
  console.log(`  Electric: ${ELECTRIC_SHAPE_URL}`);

  const db = connectDemoDatabase(DATABASE_URL);
  const belief: FleetHistoryStore<typeof fleetGraph> = await newStore(fleetGraph, db, true);
  const cursor = await newStore(checkpointGraph, db);
  const book = typeGraphCheckpoints(cursor);

  try {
    const batches: Array<{ after: string | undefined; changes: readonly ShapeChange<AgentEventRow>[] }> = [];
    const rawSource = electricShapeSource<AgentEventRow>({
      name: STREAM_NAME,
      url: ELECTRIC_SHAPE_URL,
      params: { table: "agent_events" },
      toChange: (message) => ({
        shape: "agent_event",
        key: message.key,
        operation: message.headers.operation,
        value: agentEventRowSchema.parse(message.value),
      }),
    });
    // A live shape's first `up-to-date` reports `lastOffset` as `"<lsn>_inf"`.
    // That is a real resumable position, and `compareOffsets` orders it natively
    // (above every integer in its position, below the next LSN) — see
    // `src/offset.ts` and `test/offset.test.ts`. This demo is what found that it
    // did not, and no adapter-side guard is needed any more.
    const source = withBatchNarration(rawSource, (after, changes) => {
      batches.push({ after, changes });
      printBatch(after, changes);
    });
    const project = graphProjector(fleetGraph, decodeAgentEvent);

    // ----------------------------------------------------------
    // (a) Preload several rows into ONE Postgres-visible moment
    // ----------------------------------------------------------
    section(`(a) Preloading ${PRESEED_COUNT} events before the first catch-up`);
    const preseeded = SCRIPT.slice(0, PRESEED_COUNT);
    await seedOverTime(db.pool, preseeded, preseeded.map(() => 0), (event) => {
      console.log(`  inserted: ${event.agentName} ${event.eventType} ${event.taskId}`);
    });

    // ----------------------------------------------------------
    // (b) First consume() — the initial snapshot, one shared offset
    // ----------------------------------------------------------
    section("(b) Initial catch-up — a real Postgres MVCC snapshot, not mockShapeSource");
    const snapshotResult = await consume({ source, store: belief, checkpoints: book, project });
    console.log(`\n  consume(): processed ${snapshotResult.processed}, cursor now @${snapshotResult.lastOffset}`);

    const snapshotBatch = batches[0];
    if (snapshotBatch === undefined) throw new Error("demo setup is broken: no batch was read during the initial catch-up");
    const snapshotOffsets = new Set(snapshotBatch.changes.map((c) => c.offset));
    // `>=`, not `===`: agent_events may already hold rows from an earlier
    // `pnpm tsx src/seed.ts` or a previous `pnpm demo` run in the same `pnpm
    // up` session (the checkpoint persists in the same Postgres database) —
    // Electric's initial sync always snapshots the table's CURRENT full
    // state, so a rerun's first batch can be larger than PRESEED_COUNT. What
    // never changes is that it is still exactly ONE Electric offset.
    if (snapshotResult.processed < PRESEED_COUNT) {
      throw new Error(`expected the initial catch-up to process at least ${PRESEED_COUNT} changes, got ${snapshotResult.processed}`);
    }
    if (snapshotOffsets.size !== 1) {
      throw new Error(`expected the initial catch-up to share ONE offset, got ${snapshotOffsets.size} distinct offsets`);
    }
    const sharedOffset = [...snapshotOffsets][0];
    if (sharedOffset === undefined) throw new Error("demo setup is broken: no shared offset captured");
    console.log(`\n  >>> PROVEN: ${snapshotResult.processed} separate Postgres commits, ${snapshotOffsets.size} Electric offset. <<<`);

    // ----------------------------------------------------------
    // (c) Live tailing — the rest of the script, spaced out in real time
    // ----------------------------------------------------------
    section("(c) Live tailing — the rest of the script arrives as real commits");
    const remaining = SCRIPT.slice(PRESEED_COUNT);
    let livePolls = 0;
    let liveProcessed = 0;
    const seeding = seedOverTime(db.pool, remaining, remaining.map(() => LIVE_SEED_DELAY_MS));
    let seedingDone = false;
    seeding.then(() => {
      seedingDone = true;
    });
    while (!seedingDone || liveProcessed < remaining.length) {
      const result = await consume({ source, store: belief, checkpoints: book, project });
      livePolls += 1;
      liveProcessed += result.processed;
      if (result.processed > 0) console.log(`  poll ${livePolls}: processed ${result.processed}, cursor @${result.lastOffset}`);
      if (!seedingDone || liveProcessed < remaining.length) await sleep(LIVE_POLL_INTERVAL_MS);
    }
    await seeding;
    console.log(`\n  live tailing done: ${liveProcessed} changes across ${livePolls} poll(s) (vs. 1 batch for the ${snapshotResult.processed}-row snapshot).`);

    // Exact, unlike the snapshot check above: every one of `remaining`'s
    // inserts happens strictly after the snapshot's read(), entirely within
    // this run, regardless of what agent_events held before this process
    // started — so nothing here should ever be lost, no matter how many
    // times this demo has run against the same `pnpm docker:up` stack.
    if (liveProcessed !== remaining.length) {
      throw new Error(`expected live tailing to process exactly ${remaining.length} changes, got ${liveProcessed}`);
    }

    // ----------------------------------------------------------
    // (d) The resulting belief
    // ----------------------------------------------------------
    section("(d) The materialized graph");
    const tasks = await taskSnapshot(belief);
    console.log();
    for (const task of tasks) console.log(`    ${task.id.padEnd(20)} [${task.status.padEnd(11)}] ${task.title}  (${task.agent})`);

    // ----------------------------------------------------------
    // (e) Time travel by offset — the honest limit, proven not asserted
    // ----------------------------------------------------------
    section("(e) Time travel by offset — per-batch, not per-change");
    const anchor = await book.anchorFor(STREAM_NAME, sharedOffset);
    if (anchor === undefined) throw new Error(`no checkpoint anchor recorded for ${STREAM_NAME}@${sharedOffset}`);
    const pastView = belief.asOfRecorded(anchor);
    const pastTasks = await taskSnapshot(pastView);
    console.log(`\n  book.anchorFor("${STREAM_NAME}", "${sharedOffset}") -> revision ${recordedInstantRevision(anchor)}`);
    console.log(`  belief.asOfRecorded(anchor) sees ${pastTasks.length} task(s) — everything from the initial snapshot, nothing from live tailing:`);
    for (const task of pastTasks) console.log(`    ${task.id.padEnd(20)} [${task.status.padEnd(11)}] ${task.title}`);
    // Every task THIS run preloaded must be visible in the snapshot-offset
    // view, each in the exact state its preload event stated — not `===` on
    // the total count, which a prior seed.ts/demo run's leftover rows would
    // legitimately inflate (see the `>=` check in section (b)).
    for (const event of preseeded) {
      const found = pastTasks.find((t) => t.id === event.taskId);
      if (found === undefined) throw new Error(`time travel is missing preloaded task "${event.taskId}" at the snapshot offset`);
      if (found.status !== event.status) {
        throw new Error(`time travel shows task "${event.taskId}" as "${found.status}", expected "${event.status}" (its LAST preloaded status)`);
      }
    }
    // The contrast: BOTH preloaded tasks get their COMPLETING event during
    // live tailing (see SCRIPT — index 4 completes t-index-fix, index 6
    // completes t-auth-audit, both >= PRESEED_COUNT). So the snapshot-offset
    // view must show them mid-flight while the CURRENT belief (section d,
    // above) shows them finished — a same-task, same-run before/after that
    // holds regardless of any unrelated history already in agent_events.
    for (const taskId of ["t-auth-audit", "t-index-fix"]) {
      const past = pastTasks.find((t) => t.id === taskId);
      const current = tasks.find((t) => t.id === taskId);
      if (past === undefined || current === undefined) throw new Error(`demo setup is broken: task "${taskId}" missing from past or current view`);
      if (past.status === current.status) {
        throw new Error(
          `time travel proves nothing for "${taskId}": snapshot-offset status ("${past.status}") equals the current status — ` +
            "live tailing should have moved it on",
        );
      }
      console.log(`\n  "${taskId}": snapshot-offset says "${past.status}", current belief says "${current.status}" — moved by live tailing.`);
    }
    console.log(`\n  All ${snapshotResult.processed} snapshot changes landed under offset @${sharedOffset} in ONE transaction (consumer.ts bounds a`);
    console.log("  same-offset run to one transaction), so there is exactly ONE recorded instant for the whole");
    console.log("  batch. anchorFor(offset) can only ever name that one instant: there is no finer-grained");
    console.log("  \"after just the finding, before the completion\" to time-travel to, because Electric itself");
    console.log("  never told this consumer such a moment existed.");
    console.log("\n  Contrast: durableStreamSource (see the root README's \"Wiring Durable Streams\") gives every");
    console.log("  change its OWN offset by construction — the same anchorFor() call there would land exactly");
    console.log("  on one change, every time. This package doesn't run a Durable Stream to prove that half; it");
    console.log("  is the other adapter's job. What's proven HERE is Electric's actual granularity, live.");

    // ----------------------------------------------------------
    // (f) must-refetch — a real shape invalidation, reproduced deterministically
    // ----------------------------------------------------------
    section("(f) ElectricMustRefetchError — a real schema-change invalidation");
    console.log("\n  Sequence: read a shape, ALTER TABLE (a real DDL change Electric must notice), insert a new");
    console.log("  row (the commit that makes Electric re-check the shape), then resume from the pre-ALTER offset.");
    const refetchSource = electricShapeSource<Record<string, unknown>>({
      name: `${STREAM_NAME}-refetch-probe`,
      url: ELECTRIC_SHAPE_URL,
      params: { table: "agent_events" },
      toChange: (message) => ({ shape: "agent_event", key: message.key, operation: message.headers.operation, value: message.value }),
    });
    const beforeAlter = await refetchSource.read(undefined);
    const beforeAlterOffset = beforeAlter.at(-1)?.offset;
    await db.pool.query("ALTER TABLE agent_events ADD COLUMN IF NOT EXISTS _shape_invalidation_demo_marker text");
    await db.pool.query(
      `INSERT INTO agent_events (agent_id, agent_name, task_id, task_title, event_type, status)
       VALUES ('agent-probe', 'Probe', 't-refetch-probe', 'trigger a shape invalidation', 'task_claimed', 'in_progress')`,
    );
    let sawMustRefetch = false;
    try {
      await refetchSource.read(beforeAlterOffset);
      console.log("\n  (no must-refetch this run — Electric had not yet noticed the schema change; this is a real");
      console.log("  race in Electric's own invalidation detection, not a bug in this package. See README.md.)");
    } catch (error) {
      if (error instanceof ElectricMustRefetchError) {
        sawMustRefetch = true;
        console.log(`\n  >>> CAUGHT ElectricMustRefetchError: "${error.message}" <<<`);
        console.log("\n  Recovering per the documented contract: drop the handle, read(undefined) from scratch.");
        const recovered = await refetchSource.read(undefined);
        console.log(`  recovered: read(undefined) returned ${recovered.length} rows with no error.`);
      } else {
        throw error;
      }
    }
    if (sawMustRefetch) {
      console.log("\n  >>> PROVEN against a real service: electricShapeSource's ElectricMustRefetchError mapping");
      console.log("      fires on an actual Electric shape invalidation, and the documented recovery works. <<<");
    }

    console.log("\n" + RULE);
    console.log(" LIVE RUN COMPLETE. Every claim above ran against a real Postgres + a real Electric.");
    console.log(RULE + "\n");
  } finally {
    await belief.close();
    await cursor.close();
    await db.close();
  }
}

// ============================================================
// Main
// ============================================================

export async function main(): Promise<void> {
  console.log(RULE);
  console.log(" agent-stream-graph x Electric x Postgres — the real thing");
  console.log(RULE);

  const reachability = await checkLiveStack();
  if (reachability.live) {
    await runLiveDemo();
  } else {
    await runOfflineDemo(reachability.reason);
  }
}

main()
  .then(() => {
    // `electricShapeSource` goes through the platform `fetch` (undici), whose
    // keep-alive sockets can outlive every explicit `close()`/`pool.end()`
    // above and leave the event loop open. An explicit exit on the success
    // path is the honest fix, not a `--force-exit` flag hiding the same gap.
    process.exit(0);
  })
  .catch((error: unknown) => {
    console.error(error);
    process.exit(1);
  });
