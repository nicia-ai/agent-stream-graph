/**
 * `RecordedInstant` string-encoding helpers.
 *
 * Since TypeGraph 0.40, a `RecordedInstant` prints as `"r1:<revision>:<ts>"`
 * — `r1` is the encoding version, `revision` is a monotonic integer that
 * orders commits, and `ts` is the wall-clock timestamp of that commit.
 *
 * The revision, not the timestamp, is the ordering key. Two recorded
 * instants can legitimately carry out-of-order or identical wall-clock
 * timestamps (clock skew, batched commits); the revision never lies about
 * order. Every comparison in this package goes through `compareRecorded`
 * rather than comparing the timestamp text directly.
 */

const RECORDED_INSTANT_PATTERN = /^r1:(\d+):(.+)$/;

export interface ParsedRecordedInstant {
  readonly revision: number;
  readonly timestamp: string;
}

export function parseRecordedInstant(value: string): ParsedRecordedInstant {
  const match = RECORDED_INSTANT_PATTERN.exec(value);
  if (match === null) {
    throw new Error(
      `Not a recorded-time anchor in the "r1:<revision>:<timestamp>" encoding: ${JSON.stringify(value)}`,
    );
  }
  const revisionText = match[1];
  const timestamp = match[2];
  if (revisionText === undefined || timestamp === undefined) {
    throw new Error(`Recorded-time anchor matched but did not capture both groups: ${JSON.stringify(value)}`);
  }
  return { revision: Number(revisionText), timestamp };
}

/** Orders two recorded-instant strings by revision first, wall-clock timestamp only as a tiebreaker. */
export function compareRecorded(a: string, b: string): number {
  const parsedA = parseRecordedInstant(a);
  const parsedB = parseRecordedInstant(b);
  if (parsedA.revision !== parsedB.revision) {
    return parsedA.revision - parsedB.revision;
  }
  if (parsedA.timestamp < parsedB.timestamp) return -1;
  if (parsedA.timestamp > parsedB.timestamp) return 1;
  return 0;
}

export function recordedAtOrBefore(candidate: string, anchor: string): boolean {
  return compareRecorded(candidate, anchor) <= 0;
}

export function recordedAfter(candidate: string, anchor: string): boolean {
  return compareRecorded(candidate, anchor) > 0;
}
