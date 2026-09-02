/**
 * TanStack DB collections for the two shapes this package reads:
 * resolved-entity history (`BeliefVersionRow`) and the recorded-time
 * scrubber's anchor list (`RecordedAnchorRow`).
 *
 * Default is offline (`mockSync.ts`) — see the module doc there. Setting
 * `VITE_ASG_MODE=electric` switches both collections to
 * `electricCollectionOptions`, reading from two Electric shapes behind
 * proxy URLs you provide. See README.md for what those endpoints need to
 * serve.
 *
 * `mockCollections()` and `electricCollections()` are exposed as two
 * separate functions, rather than one that returns either, ON PURPOSE:
 * each collection kind carries its own `utils` shape
 * (`LocalOnlyCollectionUtils` vs `ElectricCollectionUtils`) and its own
 * key type, and unifying both into one common return type would either
 * lose that precision or fight TypeScript's generic inference for no
 * benefit — `App.tsx` picks one branch and renders one concrete subtree,
 * it never needs both at once.
 */
import { createCollection } from "@tanstack/react-db";
import { electricCollectionOptions } from "@tanstack/electric-db-collection";
import type { BeliefVersionRow, RecordedAnchorRow } from "./types";
import { mockBeliefVersions, mockRecordedAnchors } from "./mockSync";

export type SyncMode = "mock" | "electric";

export function resolveMode(): SyncMode {
  return import.meta.env.VITE_ASG_MODE === "electric" ? "electric" : "mock";
}

function requireEnv(name: "VITE_ASG_VERSIONS_URL" | "VITE_ASG_ANCHORS_URL"): string {
  const value = import.meta.env[name];
  if (value === undefined || value === "") {
    throw new Error(
      `VITE_ASG_MODE=electric requires ${name} — a proxy URL for the corresponding Electric shape. ` +
        "See README.md.",
    );
  }
  return value;
}

export function mockCollections() {
  return { versions: mockBeliefVersions(), anchors: mockRecordedAnchors() };
}

export function electricCollections() {
  const versions = createCollection(
    electricCollectionOptions<BeliefVersionRow>({
      id: "asg-belief-versions",
      getKey: (row) => row.id,
      shapeOptions: { url: requireEnv("VITE_ASG_VERSIONS_URL") },
    }),
  );
  const anchors = createCollection(
    electricCollectionOptions<RecordedAnchorRow>({
      id: "asg-recorded-anchors",
      getKey: (row) => row.id,
      shapeOptions: { url: requireEnv("VITE_ASG_ANCHORS_URL") },
    }),
  );
  return { versions, anchors };
}
