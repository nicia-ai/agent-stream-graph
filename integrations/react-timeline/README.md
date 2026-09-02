# @nicia-ai/asg-react-timeline

The read-side: sync resolved entities, with history, into the browser with
[TanStack DB](https://tanstack.com/db), and scrub the belief graph through
recorded time.

`@nicia-ai/agent-stream-graph` materializes agent event streams into a
bitemporal TypeGraph belief graph — server-side, Node-only, TypeGraph never
runs in a browser. This package is the other half: a small React app that
syncs a **flat, entity-resolved history feed** of that graph and lets you
drag a recorded-time scrubber across it. Every panel re-renders the belief
as of the selected moment, and — the point of the whole exercise — visibly
marks what that moment could **not yet have known**, versus what's known
now.

## What it demonstrates

- **`asOfRecorded`, made visible.** `useRecordedTimeline` filters the synced
  version history down to "the newest version of each entity recorded at or
  before this instant" — the same question `store.asOfRecorded(anchor)`
  answers server-side, done client-side over synced rows.
- **Not-yet-known vs. known now.** Entities that exist in the graph today
  but hadn't been recorded yet as of the selected instant render as ghost
  chips — kind only, no content, because showing their content would leak
  information that moment couldn't have had.
- **Revised beliefs.** Entities known at the selected instant whose
  attributes have since changed show a field-level "then → now" diff.
- **Contested entities.** Entity resolution's flagged disagreements
  (`J. Doe` / `Jane Doe`, see the fixtures) show as a `disputed` badge, not
  a silently-picked winner.

## What it does NOT prove

- **It does not run TypeGraph or entity resolution.** Both are server-side.
  This app only proves that a flat, already-resolved history feed can be
  synced and scrubbed correctly in the browser — not that any particular
  backend correctly *produces* that feed. The row contract it expects is
  documented in `src/types.ts` (`BeliefVersionRow`, `RecordedAnchorRow`);
  wiring a real backend to serve rows in that shape is a real integration
  step this package does not do for you.
- **Mock mode's fixtures are hand-written, not derived from a real
  `consume()` run.** They're internally consistent (see `src/mockSync.ts`'s
  module doc) but they're illustrative data, not a golden trace.
- **No mutations.** This is a read-side viewer. The collections are synced
  read-only; nothing here writes back to Electric or Postgres.
- **Electric mode is untested against a live Electric service in this
  checkout.** The `electricCollectionOptions` wiring is verified against
  the installed package's `.d.ts` types, not against a running shape
  stream — see Verification below.

## Run it (offline fixtures — the default)

```bash
pnpm install   # from the repo root, if you haven't already
pnpm --filter @nicia-ai/asg-react-timeline dev
```

No Electric, no Postgres, no network. `src/mockSync.ts` seeds two
TanStack DB `localOnlyCollectionOptions` collections — the same
`Collection` interface the live path uses — with a small, deliberately
rich fixture: two agents disagreeing about one person, an entity that
arrives late, an attribute revision, and a valid-time window that closes
and reopens **in place** (same row, original `validFrom` preserved).

## Point it at a real Electric service

Set `VITE_ASG_MODE=electric` plus the two shape-proxy URLs, e.g. in
`integrations/react-timeline/.env.local`:

```
VITE_ASG_MODE=electric
VITE_ASG_VERSIONS_URL=http://localhost:3001/api/belief-versions
VITE_ASG_ANCHORS_URL=http://localhost:3001/api/recorded-anchors
```

Both URLs should be a small proxy route you control (never point the
browser at Electric or Postgres directly) that forwards to two Electric
shapes serving rows matching `BeliefVersionRow` and `RecordedAnchorRow` in
`src/types.ts`. `integrations/electric-postgres` in this repo has a real
Electric-over-Postgres setup (`docker compose up`) you could adapt a proxy
route from; the newsroom demo's HTTP server (`demos/newsroom`, `pnpm
serve`) is a plausible source for these two shapes too, once it exists —
this package does not depend on it and hasn't been run against it.

## Commands

```bash
pnpm typecheck   # tsc --noEmit
pnpm build       # vite build
pnpm dev         # vite dev server, mock mode by default
```

## Files

| File | What it owns |
| --- | --- |
| `src/types.ts` | The row contract (`BeliefVersionRow`, `RecordedAnchorRow`) both sync modes produce. |
| `src/recordedInstant.ts` | Parsing/ordering `RecordedInstant`'s `"r1:<revision>:<ts>"` string encoding — by revision, not timestamp text. |
| `src/collections.ts` | Builds the two TanStack DB collections, mock or Electric, from `VITE_ASG_MODE`. |
| `src/mockSync.ts` | The offline fixture source and its narrative. |
| `src/useRecordedTimeline.ts` | Owns the scrubber position and derives "known as of", "not yet known", and "revised since" from the synced rows. |
| `src/RecordedScrubber.tsx` | The scrubber control — generic over `{ id, label }` steps, no belief-graph knowledge. Reusable as-is. |
| `src/BeliefPanel.tsx` | Renders the derived belief state. |
| `src/App.tsx`, `src/main.tsx`, `index.html` | App shell. |

## Verification

Run in this checkout: `pnpm typecheck` clean, `pnpm build` succeeding, and
`pnpm dev` loaded in a headless browser confirming the app mounts with no
console errors, the fixtures render, and dragging the scrubber changes the
rendered belief (entity revisions, the not-yet-known ghost strip, and the
contested badge all changed as expected while scrubbing across the
fixture's ten recorded anchors). The Electric path (`VITE_ASG_MODE=electric`)
was NOT exercised against a live shape stream — only checked for type
correctness against the installed `@tanstack/electric-db-collection` types.
