import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createRetractionCapability } from "@nicia-ai/typegraph/provenance";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { BACKGROUND_SCAN_SOURCE, ID_CHECK_SOURCE, JANE_EMAIL, STREAM_CRM, STREAM_LINKEDIN } from "../src/fixtures.js";
import { retractionConfig, VERIFIED_PREDICATE } from "../src/graph.js";
import { openMemoryStore, type OpenMemoryStoreResult } from "../src/store.js";
import { believedAt, recall, whySoFar } from "../src/tools.js";

describe("memory tools", () => {
  const dataDir = mkdtempSync(join(tmpdir(), "asg-mcp-memory-test-"));
  let memory: OpenMemoryStoreResult;

  beforeAll(async () => {
    memory = await openMemoryStore({ dataDir });
  });

  afterAll(async () => {
    await memory.close();
    rmSync(dataDir, { recursive: true, force: true });
  });

  it("seeds a fresh store from fixtures on first open", () => {
    expect(memory.seeded).toBe(true);
  });

  it("does not reseed an already-seeded store — closes and reopens the same files", async () => {
    await memory.close();
    memory = await openMemoryStore({ dataDir });
    expect(memory.seeded).toBe(false);
  });

  describe("recall", () => {
    it("resolves differently-spelled sightings of the same person to one entity", async () => {
      const byFullName = await recall(memory.store, "Jane Doe");
      const byAbbreviation = await recall(memory.store, "J. Doe");
      expect(byFullName.found).toBe(true);
      expect(byAbbreviation.found).toBe(true);
      if (!byFullName.found || !byAbbreviation.found) return;
      expect(byFullName.entity.kind).toBe("Person");
      expect(byFullName.entity.id).toBe(byAbbreviation.entity.id);
    });

    it("includes every observed alias on the resolved entity", async () => {
      const result = await recall(memory.store, JANE_EMAIL);
      expect(result.found).toBe(true);
      if (!result.found || result.entity.kind !== "Person") return;
      expect(result.entity.aliases).toEqual(expect.arrayContaining(["Jane Doe", "J. Doe"]));
    });

    it("resolves by email, name, and alias to the same id", async () => {
      const byEmail = await recall(memory.store, JANE_EMAIL);
      const byName = await recall(memory.store, "Jane Doe");
      expect(byEmail.found && byName.found).toBe(true);
      if (!byEmail.found || !byName.found) return;
      expect(byEmail.entity.id).toBe(byName.entity.id);
    });

    it("reports not found for an unknown handle", async () => {
      const result = await recall(memory.store, "nobody@example.invalid");
      expect(result.found).toBe(false);
    });
  });

  describe("believedAt", () => {
    it("reconstructs belief before a correction differently than after", async () => {
      const before = await believedAt(memory.store, memory.book, STREAM_LINKEDIN, "001");
      const after = await believedAt(memory.store, memory.book, STREAM_LINKEDIN, "002");
      expect(before.found && after.found).toBe(true);
      if (!before.found || !after.found) return;

      const titleBefore = before.people.find((p) => p.email === JANE_EMAIL)?.title;
      const titleAfter = after.people.find((p) => p.email === JANE_EMAIL)?.title;
      expect(titleBefore).toBe("VP Engineering");
      expect(titleAfter).toBe("VP Engineering & Product");
    });

    it("returns found: false for an offset with no checkpoint", async () => {
      const result = await believedAt(memory.store, memory.book, STREAM_LINKEDIN, "999");
      expect(result.found).toBe(false);
    });

    it("keys checkpoints by stream name, not by store", async () => {
      const result = await believedAt(memory.store, memory.book, STREAM_CRM, "001");
      expect(result.found).toBe(true);
    });
  });

  describe("whySoFar", () => {
    it("shows a fact losing its support as its sources are retracted", async () => {
      const initial = await whySoFar(memory.store, JANE_EMAIL, VERIFIED_PREDICATE);
      expect(initial.found).toBe(true);
      if (!initial.found) return;
      expect(initial.currentlyHeld).toBe(true);
      expect(initial.supportedBy).toHaveLength(2);
      expect(initial.supportedBy.every((s) => !s.retracted)).toBe(true);

      const provenance = createRetractionCapability(memory.store, retractionConfig);

      await provenance.retract({ kind: "Source", id: ID_CHECK_SOURCE });
      const afterOne = await whySoFar(memory.store, JANE_EMAIL, VERIFIED_PREDICATE);
      expect(afterOne.found && afterOne.currentlyHeld).toBe(true);

      await provenance.retract({ kind: "Source", id: BACKGROUND_SCAN_SOURCE });
      const afterBoth = await whySoFar(memory.store, JANE_EMAIL, VERIFIED_PREDICATE);
      expect(afterBoth.found).toBe(true);
      if (!afterBoth.found) return;
      expect(afterBoth.currentlyHeld).toBe(false);
      expect(afterBoth.supportedBy.every((s) => s.retracted)).toBe(true);

      // Restore state so this test does not leak into others in the file.
      await provenance.unRetractMany([
        { kind: "Source", id: ID_CHECK_SOURCE },
        { kind: "Source", id: BACKGROUND_SCAN_SOURCE },
      ]);
    });

    it("reports found: false for an entity that does not resolve", async () => {
      const result = await whySoFar(memory.store, "nobody@example.invalid", VERIFIED_PREDICATE);
      expect(result.found).toBe(false);
    });
  });
});
