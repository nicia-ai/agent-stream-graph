type ParsedOffset = readonly bigint[];

const OFFSET_PATTERN = /^\d+(?:_\d+)*$/;

/**
 * Separator between a composite offset's base and its sub-offset.
 *
 * PRECONDITION: a base offset must never contain this character. That is the
 * caller's guarantee to make — {@link composeOffset} rejects a base that breaks
 * it rather than emitting a cursor that would parse back as something else.
 */
const COMPOSITE_SEPARATOR = ",";

/**
 * A position inside a stream whose server-assigned offsets address whole
 * appends rather than individual messages.
 *
 * `base` is an offset the server issued; `consumed` is how many messages after
 * that offset have already been applied. This is the protocol's own addressing
 * scheme for `application/json` streams — `Stream-Fork-Offset` plus
 * `Stream-Fork-Sub-Offset`, "the number of flattened JSON messages to inherit
 * past the anchor" — so a checkpoint taken here converts directly into a fork
 * point with no re-derivation.
 */
export type CompositeOffset = Readonly<{ base: string; consumed: number }>;

const COMPOSITE_PATTERN = new RegExp(`^([^${COMPOSITE_SEPARATOR}]*)${COMPOSITE_SEPARATOR}(\\d+)$`);

/**
 * The "beginning of stream" sentinel. It is not a token a server ever issues, so
 * it matches no offset format — but it is by definition the least position, and
 * a stream's first messages are addressed relative to it until the first real
 * offset arrives.
 *
 * Note the deliberate disagreement with `shape-source.ts`'s `SENTINEL_OFFSETS`,
 * which treats the same literal as a value that must NEVER be checkpointed. Both
 * are right: there it is a shape stream reporting it has no resumable position
 * yet, here it is a durable stream's real, seekable origin.
 */
export const STREAM_START = "-1";

function parseOffset(offset: string): ParsedOffset | undefined {
  if (!OFFSET_PATTERN.test(offset)) return undefined;
  return offset.split("_").map((part) => BigInt(part));
}

/** Render a {@link CompositeOffset} as the single opaque string a cursor stores. */
export function composeOffset(base: string, consumed: number): string {
  if (!Number.isInteger(consumed) || consumed < 0) {
    throw new RangeError(
      `composeOffset(): consumed must be a non-negative integer, received ${consumed}. ` +
        "It counts messages already applied past the base offset.",
    );
  }
  if (base.includes(COMPOSITE_SEPARATOR)) {
    throw new RangeError(
      `composeOffset(): base offset "${base}" already contains "${COMPOSITE_SEPARATOR}". ` +
        "Composite offsets are built from a server-issued base, which never contains the separator.",
    );
  }
  return `${base}${COMPOSITE_SEPARATOR}${consumed}`;
}

/** Split a composite offset, or `undefined` if this is a plain (whole-append) offset. */
export function parseCompositeOffset(offset: string): CompositeOffset | undefined {
  const match = COMPOSITE_PATTERN.exec(offset);
  if (match === null) return undefined;
  return { base: match[1]!, consumed: Number(match[2]!) };
}

/**
 * Compare stream offsets without relying on zero padding.
 *
 * - The `-1` start sentinel is the least offset, whatever the other side's
 *   format. It is what a composite offset's base is until the first real offset
 *   arrives, so this is on the ordinary path, not an edge case.
 * - Both composite (`<base>,<consumed>`) → bases compared by these same rules,
 *   then `consumed` numerically. Numeric comparison is what lets the sub-offset
 *   grow past 9 without padding, matching how numeric-tuple bases are handled.
 * - Both numeric-tuple → component-wise `BigInt` comparison (missing trailing
 *   components are treated as `0n`).
 * - Both non-numeric → lexicographic `String` comparison (consistent ordering
 *   for sources that use opaque string offsets).
 * - Formats that do not match → THROWS. A stream whose offsets switch format
 *   mid-flight has an incoherent order; failing loud is safer than silently
 *   producing a partial order.
 */
export function compareOffsets(left: string, right: string): number {
  if (left === right) return 0;
  if (left === STREAM_START) return -1;
  if (right === STREAM_START) return 1;
  const compositeLeft = parseCompositeOffset(left);
  const compositeRight = parseCompositeOffset(right);
  if (compositeLeft !== undefined && compositeRight !== undefined) {
    const byBase = compareOffsets(compositeLeft.base, compositeRight.base);
    if (byBase !== 0) return byBase;
    if (compositeLeft.consumed < compositeRight.consumed) return -1;
    if (compositeLeft.consumed > compositeRight.consumed) return 1;
    return 0;
  }
  if (compositeLeft !== undefined || compositeRight !== undefined) {
    throw new Error(
      `compareOffsets(): incompatible offset formats — "${left}" and "${right}" mix composite ` +
        `(<base>${COMPOSITE_SEPARATOR}<consumed>) and plain offsets. A single stream must use one format consistently.`,
    );
  }
  const parsedLeft = parseOffset(left);
  const parsedRight = parseOffset(right);
  if (parsedLeft !== undefined && parsedRight !== undefined) {
    const length = Math.max(parsedLeft.length, parsedRight.length);
    for (let index = 0; index < length; index += 1) {
      const leftPart = parsedLeft[index] ?? 0n;
      const rightPart = parsedRight[index] ?? 0n;
      if (leftPart < rightPart) return -1;
      if (leftPart > rightPart) return 1;
    }
    return 0;
  }
  if (parsedLeft !== undefined || parsedRight !== undefined) {
    throw new Error(
      `compareOffsets(): incompatible offset formats — "${left}" and "${right}" mix numeric-tuple and non-numeric. ` +
        "A single stream must use one format consistently.",
    );
  }
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}
