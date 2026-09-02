/**
 * Regression test: `believedAt` reconstructs a recorded-time view via
 * `RecordedStoreView.query()`, not `RecordedStoreView.nodes.X.scan()`.
 * `scan()` returns one bounded page (at most 1,000 entities, per its own
 * doc comment) — using it directly, without following `hasNextPage`, would
 * silently truncate `believedAt`'s result once a store held more Person
 * rows than fit a page. That is the worst failure mode for a time-travel
 * tool: it looks like a complete, correct answer.
 *
 * This seeds enough Person rows to exceed one page and asserts every one
 * of them comes back.
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { consume, mockShapeSource, type ShapeChange } from "@nicia-ai/agent-stream-graph";
import { afterAll, describe, expect, it } from "vitest";

import type { ObservationValue } from "../src/fixtures.js";
import { openMemoryStore, projectObservation } from "../src/store.js";
import { believedAt } from "../src/tools.js";

// `RecordedScanOptions.limit` "[d]efaults to 1,000 and cannot exceed 1,000" —
// comfortably exceed that so a page-1-only read is provably incomplete.
const BULK_PERSON_COUNT = 1_100;
const BULK_STREAM = "bulk-seed";
// One shared offset for the whole batch — the same "one offset per catch-up
// batch" shape `shape-source.ts`'s Electric adapter documents for a real
// initial sync — so `consume()` (given a matching `maxBatchSize`) writes all
// `BULK_PERSON_COUNT` rows in a single transaction instead of one per row.
// The regression this test guards against is about `believedAt`'s read path,
// not about how many transactions produced the rows it reads.
const BULK_OFFSET = "00001";

describe("believedAt pagination", () => {
  const dataDir = mkdtempSync(join(tmpdir(), "asg-mcp-memory-pagination-test-"));

  afterAll(() => {
    rmSync(dataDir, { recursive: true, force: true });
  });

  it(
    "returns every Person as of the anchor, not just the first scan page",
    async () => {
      const memory = await openMemoryStore({ dataDir });
      try {
        const bulkChanges: ShapeChange<ObservationValue>[] = Array.from({ length: BULK_PERSON_COUNT }, (_unused, i) => ({
          offset: BULK_OFFSET,
          shape: "person",
          key: `bulk-${i}`,
          operation: "insert",
          value: { personEmail: `bulk-${i}@example.test`, personName: `Bulk Person ${i}`, title: "Tester" },
        }));

        const result = await consume({
          source: mockShapeSource(BULK_STREAM, bulkChanges),
          store: memory.store,
          checkpoints: memory.book,
          project: projectObservation,
          maxBatchSize: BULK_PERSON_COUNT,
        });
        expect(result.processed).toBe(BULK_PERSON_COUNT);

        const belief = await believedAt(memory.store, memory.book, BULK_STREAM, BULK_OFFSET);
        expect(belief.found).toBe(true);
        if (!belief.found) return;

        // + 1 for "Jane Doe", seeded from the package's own fixtures.
        expect(belief.people.length).toBe(BULK_PERSON_COUNT + 1);
        expect(belief.people.length).toBeGreaterThan(1_000);
      } finally {
        await memory.close();
      }
    },
    60_000,
  );
});
