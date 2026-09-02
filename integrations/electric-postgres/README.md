# @nicia-ai/asg-electric-postgres

The real thing: [Electric](https://electric-sql.com) over Postgres logical
replication, materialized live into a bitemporal belief graph with
[`@nicia-ai/agent-stream-graph`](https://github.com/nicia-ai/agent-stream-graph)'s
published entrypoint — not a reach into `../src`, an actual `workspace:*`
install of the built package, the way a real consumer would use it.

This package exists because the root repo's README says so, plainly: *"The
Electric adapter is not live-service tested here — unit tests cover the
`@electric-sql/client` message shape, control messages, and timeout, but CI
does not run an Electric service."* Everything below either ran, verified,
against a real `electricsql/electric` container over a real
`wal_level=logical` Postgres — or is explicitly labeled as the offline
fallback that runs when one isn't reachable.

## What this proves, and what it doesn't

**Proves, against a real service (see `pnpm demo`'s section labels):**

- **(b) Initial sync is a real Postgres MVCC snapshot**, not `mockShapeSource`'s
  instant resolve — several independent commits arrive as one catch-up batch.
- **(c) Live tailing** — after catch-up, new Postgres commits arrive as
  genuinely separate batches over real wall-clock time, each its own poll.
- **(e) Checkpoint granularity is PER BATCH, not per change.** This is the
  headline limitation and this package's actual point: Electric's
  `ShapeStream` gives no per-message resume offset. Every change in one
  catch-up batch shares that batch's `lastOffset`, so
  `book.anchorFor(stream, offset)` reconstructs the belief as of the **end**
  of the batch — never mid-batch. The demo doesn't just say this, it proves
  it: several preloaded changes land under one offset, and time-travelling to
  that offset visibly shows the state *after all of them*, with no way to ask
  for "after just the third one." Contrast: `durableStreamSource` (the root
  README's "Wiring Durable Streams") gives every change its own offset by
  construction — this package doesn't run one to prove that half, that's the
  other adapter's job, but the asymmetry is real and this is where you feel it.
- **(f) `ElectricMustRefetchError` fires on a real shape invalidation.** A
  schema change (`ALTER TABLE ... ADD COLUMN`) genuinely invalidates an
  open shape; the next resume attempt gets Electric's real `must-refetch`
  control message, and `electricShapeSource` maps it correctly. This error
  path had never been seen against a real server anywhere in this repo before
  this package.

  **This beat is the one non-deterministic thing in the demo, by nature.**
  Whether Electric has noticed the DDL change by the time the demo resumes is
  Electric's own timing, not something this package controls — on a cold stack
  it fires reliably, on a warm one it often does not. The demo therefore treats
  a missing `must-refetch` as a legitimate outcome and says so in its output
  rather than failing or, worse, retrying until it gets the answer it wanted.
  When it does fire, the recovery (`read(undefined)`) is verified for real.
  Everything else in the demo is deterministic, including the offset bug below,
  which reproduces sequentially with no race.

**Does not prove:** anything about Electric's authenticated/proxied
deployment mode (`ELECTRIC_INSECURE=true` here, dev-only, on purpose),
`@tanstack/electric-db-collection`-style client consumption, Electric's
`liveSse` transport, or `durableStreamSource`'s per-message offsets (that's a
different adapter — see the root README).

## A real bug this package found, and fixed upstream

### `compareOffsets` couldn't parse Electric's own `"<lsn>_inf"` offset

`ShapeStream.lastOffset` is `"<lsn>_inf"` — literally, e.g. `"0_inf"` — on a
shape's first `up-to-date`: caught up to the live tail, but no live-replication
message has arrived yet to assign a real op-index. Captured directly off the
wire:

```
$ curl -s "http://localhost:3000/v1/shape?table=agent_events&offset=-1"
[{"key":"...","value":{...},"headers":{"relation":["public","agent_events"],"operation":"insert"}},
 {"headers":{"control":"snapshot-end", ...}}]
# electric-offset header on the response: 0_0
# ...next poll, subscribe() delivers a SEPARATE batch:
# stream.lastOffset === "0_inf"
```

`inf` is not a sentinel to reject — it is a REAL, resumable position Electric
hands back and accepts again. `src/offset.ts`'s `compareOffsets` used to
require every offset compared against a numeric-tuple offset to itself be
numeric-tuple (`/^\d+(?:_\d+)*$/` — `inf` failed that). The moment a real live
change gave the SAME shape a genuine `"<lsn>_<n>"` offset, comparing it against
the checkpointed `"0_inf"` threw:

```
compareOffsets(): incompatible offset formats — "26729520_0" and "0_inf"
mix numeric-tuple and non-numeric. A single stream must use one format
consistently.
```

That throw happened inside `checkpoint.ts`'s `advanceCheckpoint`, so it never
surfaced on the FIRST catch-up — it surfaced on the SECOND one. Which meant:
on essentially every fresh `docker compose up` + tail-in-a-loop, the very
pattern the root README recommends ("Call it in a loop to keep tailing a live
shape"), this threw on the first live change after catch-up. Reproduced
directly with the real adapter, sequentially, no timing race:

```
$ node probe.mjs
read #1: 12 changes, lastOffset "0_inf"
compareOffsets("26729520_0", "0_inf") THREW:
  compareOffsets(): incompatible offset formats — mix numeric-tuple and non-numeric.
```

**Fixed upstream, in `src/offset.ts`.** `inf` is now a parsed offset
component (`INFINITE_PART`) that sorts above every integer in its own
position and below the next LSN — the only total order consistent with what
it means: `"0_inf"` falls after `"0_9"` and before `"1_0"`. `OFFSET_PATTERN`
accepts it, `parseOffset` maps it, and comparison goes through a
`compareParts` helper. Regression-tested in `test/offset.test.ts` — ordering
cases, plus a realistic catch-up sequence (`0_inf` → `26800000_0` →
`26800000_1` → `26800080_0`) asserted monotonic, and a case proving `0_inf`
still works as a composite base. Root suite 183/183 green.

This package originally shipped a local workaround (`withElectricOffsetGuard`,
substituting a large decimal sentinel for `inf` in both directions around
`electricShapeSource`) before the fix landed. It's gone now — the root handles
`inf` natively, so `pnpm demo`'s section (e) prints Electric's real
`0_inf` offset rather than a sentinel standing in for it. That's not just
tidier, it's more instructive: the offset a reader sees in the output is the
literal thing Electric said, not this package's paraphrase of it.

### Not a bug, but a real gotcha worth documenting here: `id`'s type

Electric's default value parser turns Postgres `int8`/`bigserial` into a JS
`BigInt`, not a string — `agent_events.id` arrives as `1n`, not `"1"`. This
package doesn't use that column for anything (task/agent/finding identity all
come from denormalized text columns — see `sql/schema.sql`), so it's a
non-issue here, but it would break `JSON.stringify` on anything that passed
the raw row through un-narrowed. `decode.ts`'s `agentEventRowSchema` never
even names the column, so it's silently stripped by Zod's default behavior.

## Layout

- `docker-compose.yml` — Postgres 16 (`wal_level=logical`) + `electricsql/electric:latest`, `ELECTRIC_INSECURE=true` (dev only).
- `sql/schema.sql` — `agent_events`: a denormalized, append-only event log, the shape a real agent fleet would actually write (title/status restated on every row, not looked up from a side table — see the file's own comment for why).
- `src/graph.ts` — the belief graph: `Agent`, `Task`, `Finding` nodes; `assignedTo`, `aboutTask` edges.
- `src/decode.ts` — `decodeAgentEvent`: the PURE decoder (`agent_events` row → `GraphEvent[]`), plus the Zod schema that validates the row and normalizes Postgres's `timestamptz` text format into ISO-8601 (see below — a real wire-format gap, distinct from the `inf`-offset bug above).
- `src/db.ts` — shared Postgres wiring (one backend/pool for both the belief graph AND the library's own checkpoint graph — safe because TypeGraph's Postgres tables carry a `graph_id` column and are multi-tenant by design).
- `src/seed.ts` — writes a realistic 12-event script into Postgres, spaced out over real time (also runnable standalone: `pnpm tsx src/seed.ts`).
- `src/demo.ts` — `pnpm demo`. Self-contained: seeds its own data (4 rows before the first catch-up, 8 more live, 400ms apart) and shows every claim above happening.

### A third wire-format gap, found the same way (see `decode.ts`)

Electric hands back Postgres's native `timestamptz` text representation for
`occurred_at`, not ISO-8601:

```
"occurred_at":"2026-08-28 17:20:56.031413+00"
```

Space-separated, not `T`-separated; `+00`, not `Z`/`+00:00`. This is NOT valid
ISO-8601, and `agent-stream-graph`'s `ValidTime.validFrom` requires ISO-8601.
`decode.ts`'s `toIsoInstant` fixes it (`new Date(...).toISOString()` — V8
parses Postgres's format leniently even though it doesn't round-trip it).
This isn't a library bug — `ValidTime` correctly documents that it wants
ISO-8601 strings — it's a real integration detail anyone wiring a Postgres
timestamp column into `validFrom` will hit, so it's called out here rather
than silently handled.

## Running it

### Live (needs Docker)

```bash
pnpm docker:up        # docker compose up -d — Postgres + Electric
pnpm demo             # tsx src/demo.ts — self-contained, seeds + consumes + time-travels
pnpm docker:down      # docker compose down -v — stop and wipe the volume
```

(Not `pnpm up`/`pnpm down` — those collide with pnpm's own builtin `pnpm up`
alias for `pnpm update`, which would run a dependency update instead of
Docker Compose. Hence the `docker:` prefix.)

`pnpm demo` alone is the whole story — it seeds its own data. You do **not**
need to run `src/seed.ts` first. If you do run it first (or run `pnpm demo`
more than once against the same `pnpm docker:up` session), the demo still
works and still exits 0: the checkpoint persists in the same Postgres
database across runs, so the numbers in section (b) will be larger than the
`4` shown below (the initial snapshot picks up whatever the table currently
holds, which is exactly the point — Electric's initial sync is a snapshot of
CURRENT state, not "since your last run"), but every assertion is written to
hold regardless (see the comments in `demo.ts` around `PRESEED_COUNT`). For
the exact numbers below, start from `pnpm docker:down && pnpm docker:up`.

`src/seed.ts` is also runnable on its own, against an already-running stack,
to generate more live traffic to watch `pnpm demo`'s section (c) pick up:

```bash
pnpm tsx src/seed.ts
```

Real output from this repo (`pnpm docker:down && pnpm docker:up && pnpm demo`,
section headers only — full transcript is long; run it yourself for the rest):

```
(a) Preloading 4 events before the first catch-up
(b) Initial catch-up — a real Postgres MVCC snapshot, not mockShapeSource
  read(<start>) -> 4 change(s) across 1 distinct offset(s):
      @0_inf: 4 change(s)  <-- 4 changes share this ONE offset
  >>> PROVEN: 4 separate Postgres commits, 1 Electric offset. <<<
(c) Live tailing — the rest of the script arrives as real commits
  poll 3: processed 1, cursor @27236376_0
  ... (8 separate polls, 8 separate offsets)
(d) The materialized graph
    t-auth-audit         [done       ] Audit the auth flow for token leakage  (Ada)
    ...
(e) Time travel by offset — per-batch, not per-change
  belief.asOfRecorded(anchor) sees 2 task(s) — everything from the initial snapshot, nothing from live tailing:
    t-auth-audit         [in_progress] ...
  "t-auth-audit": snapshot-offset says "in_progress", current belief says "done" — moved by live tailing.
(f) ElectricMustRefetchError — a real schema-change invalidation
  >>> CAUGHT ElectricMustRefetchError: "..." <<<
  recovered: read(undefined) returned 13 rows with no error.
LIVE RUN COMPLETE. Every claim above ran against a real Postgres + a real Electric.
```

### Offline (no Docker / no reachable Postgres+Electric)

`pnpm demo` checks reachability first (a 2s-timeout `SELECT 1` against
Postgres, a 2s-timeout `GET /v1/health` against Electric) and falls back
automatically — same command, no flags:

```bash
pnpm demo
```

```
OFFLINE MODE
  Postgres unreachable at postgresql://...localhost:54321... (ECONNREFUSED)
  Falling back to a decoder-only run over mockShapeSource fixtures.
  ...
  9 node.upsert + 4 edge.upsert events decoded, all plain JSON, all from 6 rows.
OFFLINE RUN COMPLETE — the live path above is UNVERIFIED in this run.
```

Offline mode exercises `decodeAgentEvent` — the same pure function the live
path uses — against `mockShapeSource` fixtures written in Postgres's own
`timestamptz` text format (so `toIsoInstant`'s normalization runs identically
in both modes), and asserts on the decoded event shape and a JSON round trip.
It does **not** exercise `electricShapeSource`, Postgres, or Electric —
there's no offline-capable TypeGraph store backend
among this package's pinned dependencies (`pg`/`drizzle-orm`/Postgres only, no
SQLite/PGlite), so a full store materialization isn't possible without a
reachable service. This is why offline mode is decoder-only rather than a
smaller version of the live run.

## Verified

- `pnpm typecheck` — clean, strict settings (`exactOptionalPropertyTypes`,
  `noUncheckedIndexedAccess`, `verbatimModuleSyntax`), no `any`/`as never`/`!`.
- `pnpm demo` — exit 0 in **both** modes, run repeatedly against this repo's
  own Docker (live) and with Docker stopped (offline). The live run was
  additionally exercised: fresh (`down -v && up`), and after a prior
  `src/seed.ts` run in the same session (the `>=`/per-task assertions in
  `demo.ts` are there specifically because that second case was tested and
  found to break the naive `===` version — see the comments at
  `PRESEED_COUNT`'s use sites).
