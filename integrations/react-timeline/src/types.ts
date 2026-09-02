/**
 * The read-side row contract.
 *
 * `agent-stream-graph` materializes agent event streams into a TypeGraph
 * belief graph server-side (Node only — TypeGraph never runs in the
 * browser). This package does not talk to TypeGraph directly. Instead it
 * defines the two flat row shapes a backend is expected to expose — as
 * Electric shapes behind a proxy, or as any other row source — so the
 * browser can sync and scrub them with TanStack DB.
 *
 * Both shapes are plain JSON: every field a Postgres/Electric row can
 * actually carry. Nothing here is TypeGraph's own `NodeId` / `EdgeId` /
 * `RecordedInstant` brand — those are nominal types that only exist inside
 * a Node process holding a live TypeGraph import, and are exactly the kind
 * of thing that cannot cross a network boundary. `recordedFrom` /
 * `recordedTo` below carry a `RecordedInstant`'s STRING ENCODING
 * (`"r1:<revision>:<timestamp>"`, see recordedInstant.ts) — the same value
 * you'd get from `String(instant)` — but typed as `string`, not the branded
 * type.
 */

/** JSON-serializable attribute values, as they'd sit in a Postgres row's jsonb column. */
export type Json = string | number | boolean | null | { [key: string]: Json } | Json[];

/**
 * One recorded version of one resolved entity.
 *
 * A row is never mutated in place once superseded: a new belief produces a
 * new row with a new `id`, and the row it replaces gets its `recordedTo`
 * filled in. That is what makes "as of a recorded instant" a plain filter
 * over this table rather than something requiring a live TypeGraph store.
 */
export interface BeliefVersionRow {
  /**
   * Electric/TanStack DB's `Row<Extensions>` constraint requires an actual
   * index signature, not just known properties — a real Postgres row can
   * always carry columns beyond the ones a particular consumer names.
   */
  [column: string]: Json;
  /** Stable primary key: `${entityId}@${recordedFrom}`. */
  id: string;
  /** The canonical entity id, post entity-resolution. Stable across versions. */
  entityId: string;
  /** The resolved node kind, e.g. "Person" or "Company". */
  kind: string;
  /** Display label for the entity — already resolved, not a raw source name. */
  label: string;
  /** The resolved attributes as of this version. */
  attributes: Record<string, Json>;
  /** Application time: when this belief starts being true. ISO-8601. */
  validFrom: string;
  /** Application time: when this belief stops being true, or `null` if still open. ISO-8601 or null. */
  validTo: string | null;
  /**
   * Recorded time: when this exact version became the belief. The string
   * encoding of a `RecordedInstant` (`"r1:<revision>:<timestamp>"`).
   */
  recordedFrom: string;
  /**
   * Recorded time: when this version was superseded, or `null` if it is
   * still the current belief. Same encoding as `recordedFrom`.
   */
  recordedTo: string | null;
  /** Which agent streams contributed to this version. */
  sourceAgents: string[];
  /** Whether entity resolution is currently holding an unresolved disagreement about this entity. */
  contested: boolean;
  /** Which attribute keys are disputed, when `contested` is true. */
  contestedFields: string[];
}

/**
 * One point on the recorded-time scrubber: a moment the belief graph's
 * clock advanced, worth landing on.
 */
export interface RecordedAnchorRow {
  [column: string]: Json;
  id: string;
  /** The string encoding of the `RecordedInstant` this anchor names. */
  recorded: string;
  /** The timestamp component of `recorded`, duplicated for display/sorting convenience. ISO-8601. */
  recordedAt: string;
  /** The agent stream whose delivery advanced the clock to this instant. */
  agent: string;
  /** That agent's shape offset at this instant. */
  offset: string;
  /** Short label for the scrubber tick. */
  label: string;
  /** One-line description of what changed, for the panel header. */
  summary: string;
}
