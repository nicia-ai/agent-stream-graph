/**
 * Owns the scrubber: the list of recorded-time anchors, which one is
 * selected, and the belief derived at that coordinate versus at "now".
 *
 * This is where "as of a recorded instant" — `store.asOfRecorded(anchor)`
 * server-side — becomes a client-side computation: filter each entity's
 * version rows down to the one whose recorded window covers the selected
 * anchor, per entity. `asOfSelected` and `asOfLatest` (an ordinary use of
 * the same filter, anchored to the newest anchor) are computed the same
 * way, on purpose — "now" is not a special case, just the anchor furthest
 * along the recorded axis.
 */
import { useCallback, useMemo, useState } from "react";
import { useLiveQuery, type Collection, type NonSingleResult, type UtilsRecord } from "@tanstack/react-db";
import type { BeliefVersionRow, RecordedAnchorRow } from "./types";
import { compareRecorded, recordedAfter, recordedAtOrBefore } from "./recordedInstant";

export interface ResolvedEntity {
  readonly entityId: string;
  readonly kind: string;
  readonly label: string;
  readonly attributes: Readonly<Record<string, unknown>>;
  readonly validFrom: string;
  readonly validTo: string | null;
  readonly recordedFrom: string;
  readonly sourceAgents: readonly string[];
  readonly contested: boolean;
  readonly contestedFields: readonly string[];
}

/** An entity known both as of the selection and now, whose belief differs between the two. */
export interface RevisedEntity {
  readonly entityId: string;
  readonly asOfSelected: ResolvedEntity;
  readonly now: ResolvedEntity;
  readonly changedFields: readonly string[];
}

export interface RecordedTimelineState {
  readonly anchors: readonly RecordedAnchorRow[];
  readonly selectedIndex: number;
  readonly selectedAnchor: RecordedAnchorRow | undefined;
  readonly latestAnchor: RecordedAnchorRow | undefined;
  readonly isAtLatest: boolean;
  readonly isLoading: boolean;
  /** Move the scrubber to an absolute anchor index (clamped to the valid range). */
  readonly select: (index: number) => void;
  /** Move the scrubber by `delta` anchors (negative = back in recorded time). */
  readonly step: (delta: number) => void;
  /** What was known, as of the selected anchor. */
  readonly knownAsOfSelected: readonly ResolvedEntity[];
  /** Entities that exist now but did not exist yet as of the selected anchor. */
  readonly notYetKnown: readonly ResolvedEntity[];
  /** Entities known at the selection whose belief has since changed. */
  readonly revisedSince: readonly RevisedEntity[];
}

function toResolvedEntity(row: BeliefVersionRow): ResolvedEntity {
  return {
    entityId: row.entityId,
    kind: row.kind,
    label: row.label,
    attributes: row.attributes,
    validFrom: row.validFrom,
    validTo: row.validTo,
    recordedFrom: row.recordedFrom,
    sourceAgents: row.sourceAgents,
    contested: row.contested,
    contestedFields: row.contestedFields,
  };
}

/** The belief graph as of `anchor`: the newest version of each entity recorded at or before it, not yet superseded by it. */
function resolveAsOf(versions: readonly BeliefVersionRow[], anchor: string): Map<string, ResolvedEntity> {
  const latestPerEntity = new Map<string, BeliefVersionRow>();
  for (const row of versions) {
    if (recordedAfter(row.recordedFrom, anchor)) continue; // not recorded yet at this point
    if (row.recordedTo !== null && recordedAtOrBefore(row.recordedTo, anchor)) continue; // superseded by this point
    const current = latestPerEntity.get(row.entityId);
    if (current === undefined || compareRecorded(row.recordedFrom, current.recordedFrom) > 0) {
      latestPerEntity.set(row.entityId, row);
    }
  }
  const resolved = new Map<string, ResolvedEntity>();
  for (const [entityId, row] of latestPerEntity) {
    resolved.set(entityId, toResolvedEntity(row));
  }
  return resolved;
}

function valuesEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b || a === null || b === null) return false;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((value, index) => valuesEqual(value, b[index]));
  }
  if (typeof a === "object" && typeof b === "object") {
    const aRecord = a as Record<string, unknown>;
    const bRecord = b as Record<string, unknown>;
    const keys = new Set([...Object.keys(aRecord), ...Object.keys(bRecord)]);
    return [...keys].every((key) => valuesEqual(aRecord[key], bRecord[key]));
  }
  return false;
}

function diffAttributeFields(
  a: Readonly<Record<string, unknown>>,
  b: Readonly<Record<string, unknown>>,
): string[] {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  return [...keys].filter((key) => !valuesEqual(a[key], b[key])).sort();
}

export interface UseRecordedTimelineOptions {
  /**
   * Chooses the starting anchor index given the anchor count, once anchors
   * have loaded. Defaults to the latest anchor (the safest generic
   * default: everything known, nothing yet to contrast). Callers with
   * fixture-specific knowledge of an interesting moment can override this.
   */
  readonly initialIndex?: (anchorCount: number) => number;
}

export function useRecordedTimeline<
  VKey extends string | number,
  VUtils extends UtilsRecord,
  AKey extends string | number,
  AUtils extends UtilsRecord,
>(
  versionsCollection: Collection<BeliefVersionRow, VKey, VUtils> & NonSingleResult,
  anchorsCollection: Collection<RecordedAnchorRow, AKey, AUtils> & NonSingleResult,
  options: UseRecordedTimelineOptions = {},
): RecordedTimelineState {
  const { data: rawAnchors, isLoading: anchorsLoading } = useLiveQuery(anchorsCollection);
  const { data: rawVersions, isLoading: versionsLoading } = useLiveQuery(versionsCollection);

  const anchors = useMemo(
    () => [...rawAnchors].sort((a, b) => compareRecorded(a.recorded, b.recorded)),
    [rawAnchors],
  );

  const [explicitIndex, setExplicitIndex] = useState<number | null>(null);

  const defaultIndex = useMemo(() => {
    if (anchors.length === 0) return 0;
    const chooser = options.initialIndex ?? ((count: number) => count - 1);
    return Math.min(Math.max(chooser(anchors.length), 0), anchors.length - 1);
  }, [anchors.length, options.initialIndex]);

  const selectedIndex =
    explicitIndex === null ? defaultIndex : Math.min(Math.max(explicitIndex, 0), Math.max(anchors.length - 1, 0));

  const select = useCallback(
    (index: number) => {
      setExplicitIndex(Math.min(Math.max(index, 0), Math.max(anchors.length - 1, 0)));
    },
    [anchors.length],
  );

  const step = useCallback(
    (delta: number) => {
      setExplicitIndex((previous) => {
        const base = previous === null ? defaultIndex : previous;
        return Math.min(Math.max(base + delta, 0), Math.max(anchors.length - 1, 0));
      });
    },
    [anchors.length, defaultIndex],
  );

  const selectedAnchor = anchors[selectedIndex];
  const latestAnchor = anchors[anchors.length - 1];
  const isAtLatest = selectedAnchor !== undefined && latestAnchor !== undefined && selectedAnchor.id === latestAnchor.id;

  const knownAsOfSelectedMap = useMemo(
    () => (selectedAnchor === undefined ? new Map<string, ResolvedEntity>() : resolveAsOf(rawVersions, selectedAnchor.recorded)),
    [rawVersions, selectedAnchor],
  );

  const knownNowMap = useMemo(
    () => (latestAnchor === undefined ? new Map<string, ResolvedEntity>() : resolveAsOf(rawVersions, latestAnchor.recorded)),
    [rawVersions, latestAnchor],
  );

  const knownAsOfSelected = useMemo(() => [...knownAsOfSelectedMap.values()], [knownAsOfSelectedMap]);

  const notYetKnown = useMemo(
    () => [...knownNowMap.values()].filter((entity) => !knownAsOfSelectedMap.has(entity.entityId)),
    [knownNowMap, knownAsOfSelectedMap],
  );

  const revisedSince = useMemo(() => {
    const revised: RevisedEntity[] = [];
    for (const [entityId, asOfSelected] of knownAsOfSelectedMap) {
      const now = knownNowMap.get(entityId);
      if (now === undefined || now.recordedFrom === asOfSelected.recordedFrom) continue;
      const changedFields = diffAttributeFields(asOfSelected.attributes, now.attributes);
      if (changedFields.length === 0 && now.validTo === asOfSelected.validTo) continue;
      revised.push({ entityId, asOfSelected, now, changedFields });
    }
    return revised.sort((a, b) => a.entityId.localeCompare(b.entityId));
  }, [knownAsOfSelectedMap, knownNowMap]);

  return {
    anchors,
    selectedIndex,
    selectedAnchor,
    latestAnchor,
    isAtLatest,
    isLoading: anchorsLoading || versionsLoading,
    select,
    step,
    knownAsOfSelected,
    notYetKnown,
    revisedSince,
  };
}
