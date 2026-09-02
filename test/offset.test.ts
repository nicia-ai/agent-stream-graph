import { describe, expect, it } from "vitest";

import { compareOffsets, composeOffset, parseCompositeOffset, STREAM_START } from "../src";

describe("compareOffsets", () => {
  it("orders numeric tuples component-wise without zero padding", () => {
    expect(compareOffsets("2_10", "2_9")).toBe(1);
    expect(compareOffsets("10_0", "9_999")).toBe(1);
    expect(compareOffsets("3_0", "3")).toBe(0);
  });

  it("treats the start sentinel as the least offset", () => {
    expect(compareOffsets(STREAM_START, "0_0")).toBe(-1);
    expect(compareOffsets("0_0", STREAM_START)).toBe(1);
  });

  // Electric reports `"<lsn>_inf"` as `ShapeStream.lastOffset` once a shape has
  // caught up but no live message has arrived yet. It is a real resumable
  // position, so a consumer tailing a shape in a loop checkpoints it and then
  // has to order the NEXT batch's ordinary offset against it. Before `inf` was
  // parsed, that second batch threw "mix numeric-tuple and non-numeric" — on
  // essentially every fresh start, which is the documented usage pattern.
  describe("Electric's `inf` operation position", () => {
    it("sorts above every integer in its own position", () => {
      expect(compareOffsets("0_inf", "0_9")).toBe(1);
      expect(compareOffsets("0_inf", "0_999999")).toBe(1);
      expect(compareOffsets("0_0", "0_inf")).toBe(-1);
      expect(compareOffsets("0_inf", "0")).toBe(1);
    });

    it("sorts below the next log sequence number", () => {
      expect(compareOffsets("0_inf", "1_0")).toBe(-1);
      expect(compareOffsets("0_inf", "26800000_0")).toBe(-1);
      expect(compareOffsets("1_0", "0_inf")).toBe(1);
    });

    it("is equal to itself and ordered against the start sentinel", () => {
      expect(compareOffsets("0_inf", "0_inf")).toBe(0);
      expect(compareOffsets(STREAM_START, "0_inf")).toBe(-1);
    });

    it("advances monotonically across a realistic catch-up sequence", () => {
      // The exact shape of a fresh `pnpm up` followed by a tail loop: the first
      // up-to-date reports `0_inf`, then real commits arrive.
      const observed = ["0_inf", "26800000_0", "26800000_1", "26800080_0"];
      for (let index = 1; index < observed.length; index += 1) {
        expect(compareOffsets(observed[index]!, observed[index - 1]!)).toBe(1);
      }
    });

    it("works as a composite base, so a durable-stream cursor can carry it", () => {
      const earlier = composeOffset("0_inf", 0);
      const later = composeOffset("0_inf", 3);
      expect(compareOffsets(earlier, later)).toBe(-1);
      expect(parseCompositeOffset(later)).toEqual({ base: "0_inf", consumed: 3 });
    });
  });

  it("still refuses a genuine format mix", () => {
    expect(() => compareOffsets("0_0", "opaque")).toThrow(/mix numeric-tuple and non-numeric/);
    expect(() => compareOffsets("0_0", "0_0,1")).toThrow(/mix composite/);
  });

  it("orders opaque offsets lexicographically", () => {
    expect(compareOffsets("aaa", "aab")).toBe(-1);
    expect(compareOffsets("b", "a")).toBe(1);
  });
});
