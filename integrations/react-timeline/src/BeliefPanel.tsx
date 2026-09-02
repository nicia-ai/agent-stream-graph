/**
 * Renders the belief as of the scrubber's current position, and — this is
 * the whole point of the demo — what that view is missing compared to now:
 *
 *  - **Not yet known**: entities that exist in the graph today but hadn't
 *    been recorded yet as of the selected instant. Rendered as ghost
 *    outlines that show ONLY that something will arrive, never its
 *    attributes — showing the content would defeat the point, which is
 *    that this moment genuinely could not have known it.
 *  - **Revised since**: entities that were known at the selected instant,
 *    but whose belief has since changed — the field-level diff between
 *    "known then" and "known now".
 *  - **Contested**: entities entity resolution is still holding an
 *    unresolved disagreement about, at the selected instant.
 */
import type { RecordedAnchorRow } from "./types";
import type { ResolvedEntity, RevisedEntity } from "./useRecordedTimeline";

export interface BeliefPanelProps {
  readonly anchor: RecordedAnchorRow | undefined;
  readonly isAtLatest: boolean;
  readonly knownAsOfSelected: readonly ResolvedEntity[];
  readonly notYetKnown: readonly ResolvedEntity[];
  readonly revisedSince: readonly RevisedEntity[];
}

function formatAttributeValue(value: unknown): string {
  if (value === null) return "—";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

function formatInstant(iso: string): string {
  return new Date(iso).toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
}

function formatValidity(entity: ResolvedEntity): string {
  const from = formatInstant(entity.validFrom);
  if (entity.validTo === null) return `valid since ${from}`;
  return `valid ${from} – ${formatInstant(entity.validTo)} (closed)`;
}

function EntityCard({
  entity,
  revision,
}: {
  readonly entity: ResolvedEntity;
  readonly revision?: RevisedEntity | undefined;
}) {
  const attributeEntries = Object.entries(entity.attributes).sort(([a], [b]) => a.localeCompare(b));
  return (
    <article className={`entity-card${entity.contested ? " entity-card--contested" : ""}`}>
      <header className="entity-card-header">
        <span className="entity-kind">{entity.kind}</span>
        <h3 className="entity-label">{entity.label}</h3>
      </header>

      <dl className="entity-attributes">
        {attributeEntries.map(([key, value]) => (
          <div className="entity-attribute" key={key}>
            <dt className={entity.contestedFields.includes(key) ? "entity-attribute-contested" : undefined}>
              {key}
            </dt>
            <dd>{formatAttributeValue(value)}</dd>
          </div>
        ))}
      </dl>

      <footer className="entity-card-footer">
        <span className={entity.validTo !== null ? "entity-validity entity-validity--closed" : "entity-validity"}>
          {formatValidity(entity)}
        </span>
        {entity.contested ? <span className="badge badge--contested">disputed</span> : null}
      </footer>

      {revision !== undefined ? (
        <div className="entity-revision">
          <span className="badge badge--revised">revised since</span>
          <ul className="entity-revision-fields">
            {revision.changedFields.map((field) => (
              <li key={field}>
                <span className="entity-revision-field">{field}</span>
                <span className="entity-revision-arrow">
                  {formatAttributeValue(revision.asOfSelected.attributes[field])}
                  {" → "}
                  {formatAttributeValue(revision.now.attributes[field])}
                </span>
              </li>
            ))}
            {revision.changedFields.length === 0 ? (
              <li>
                <span className="entity-revision-field">validity</span>
                <span className="entity-revision-arrow">
                  {formatValidity(revision.asOfSelected)}
                  {" → "}
                  {formatValidity(revision.now)}
                </span>
              </li>
            ) : null}
          </ul>
        </div>
      ) : null}
    </article>
  );
}

export function BeliefPanel({ anchor, isAtLatest, knownAsOfSelected, notYetKnown, revisedSince }: BeliefPanelProps) {
  const revisionByEntity = new Map(revisedSince.map((revision) => [revision.entityId, revision]));
  const sortedEntities = [...knownAsOfSelected].sort((a, b) => a.entityId.localeCompare(b.entityId));

  return (
    <section className="belief-panel">
      <header className="belief-panel-header">
        <h2>{isAtLatest ? "Known now" : "Known as of this point"}</h2>
        {anchor !== undefined ? <p className="belief-panel-summary">{anchor.summary}</p> : null}
      </header>

      {sortedEntities.length === 0 ? (
        <p className="belief-empty">Nothing has been recorded yet at this point in time.</p>
      ) : (
        <div className="entity-grid">
          {sortedEntities.map((entity) => (
            <EntityCard key={entity.entityId} entity={entity} revision={revisionByEntity.get(entity.entityId)} />
          ))}
        </div>
      )}

      {!isAtLatest && notYetKnown.length > 0 ? (
        <div className="not-yet-known">
          <h3>Not yet known at this point</h3>
          <p className="not-yet-known-caption">
            These exist in the graph today, but this moment has no record of them yet.
          </p>
          <ul className="ghost-list">
            {notYetKnown.map((entity) => (
              <li key={entity.entityId} className="ghost-chip">
                <span className="entity-kind">{entity.kind}</span>
                <span className="ghost-label">?</span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </section>
  );
}
