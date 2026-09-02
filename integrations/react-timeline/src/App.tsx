import { useState } from "react";
import { resolveMode, mockCollections, electricCollections } from "./collections";
import { useRecordedTimeline, type RecordedTimelineState } from "./useRecordedTimeline";
import { RecordedScrubber, type ScrubberStep } from "./RecordedScrubber";
import { BeliefPanel } from "./BeliefPanel";
import type { SyncMode } from "./collections";

// Land the scrubber a few beats before "now" rather than on it, so the
// contrast this app exists to show — not-yet-known entities, revised
// beliefs — is visible on first paint, before anyone touches the control.
function defaultAnchorIndex(anchorCount: number): number {
  return Math.max(0, anchorCount - 5);
}

/**
 * Picks a sync mode once, then mounts exactly one concrete subtree.
 *
 * `MockApp` and `ElectricApp` each construct their own collections (a
 * distinct `utils`/key shape per sync mode — see collections.ts) and pass
 * them straight into `useRecordedTimeline` at a single, concretely-typed
 * call site, rather than this component trying to hold "either kind of
 * collection" in one variable.
 */
export function App() {
  const [mode] = useState(resolveMode);
  return mode === "mock" ? <MockApp /> : <ElectricApp />;
}

function MockApp() {
  const [collections] = useState(mockCollections);
  const timeline = useRecordedTimeline(collections.versions, collections.anchors, {
    initialIndex: defaultAnchorIndex,
  });
  return <TimelineShell mode="mock" timeline={timeline} />;
}

function ElectricApp() {
  const [collections] = useState(electricCollections);
  const timeline = useRecordedTimeline(collections.versions, collections.anchors, {
    initialIndex: defaultAnchorIndex,
  });
  return <TimelineShell mode="electric" timeline={timeline} />;
}

function TimelineShell({ mode, timeline }: { readonly mode: SyncMode; readonly timeline: RecordedTimelineState }) {
  const steps: ScrubberStep[] = timeline.anchors.map((anchor) => ({ id: anchor.id, label: anchor.label }));

  return (
    <div className="app">
      <header className="app-header">
        <div>
          <h1>Recorded-time timeline</h1>
          <p className="app-subtitle">
            One belief graph, scrubbed through recorded time. Drag, or use the arrow keys.
          </p>
        </div>
        <span className={`mode-badge mode-badge--${mode}`}>
          {mode === "mock" ? "offline fixtures" : "live via Electric"}
        </span>
      </header>

      {timeline.isLoading ? (
        <p className="loading">Loading…</p>
      ) : timeline.anchors.length === 0 ? (
        <p className="loading">No recorded-time anchors yet.</p>
      ) : (
        <>
          <div className="scrubber-row">
            <button
              type="button"
              className="scrubber-step-button"
              onClick={() => timeline.step(-1)}
              disabled={timeline.selectedIndex <= 0}
              aria-label="Step to the previous recorded instant"
            >
              ◀
            </button>
            <RecordedScrubber
              steps={steps}
              index={timeline.selectedIndex}
              onChange={timeline.select}
              ariaLabel="Recorded-time position"
              caption={timeline.selectedAnchor?.summary}
            />
            <button
              type="button"
              className="scrubber-step-button"
              onClick={() => timeline.step(1)}
              disabled={timeline.isAtLatest}
              aria-label="Step to the next recorded instant"
            >
              ▶
            </button>
          </div>

          <div className="anchor-meta">
            <span>{timeline.selectedAnchor?.agent}</span>
            <span aria-hidden="true">·</span>
            <span>offset {timeline.selectedAnchor?.offset}</span>
            <span aria-hidden="true">·</span>
            <code>{timeline.selectedAnchor?.recorded}</code>
            {timeline.isAtLatest ? <span className="now-badge">now</span> : null}
          </div>

          <BeliefPanel
            anchor={timeline.selectedAnchor}
            isAtLatest={timeline.isAtLatest}
            knownAsOfSelected={timeline.knownAsOfSelected}
            notYetKnown={timeline.notYetKnown}
            revisedSince={timeline.revisedSince}
          />
        </>
      )}

      <footer className="app-footer">
        <p>
          Reads flat, entity-resolved history rows synced with{" "}
          <a href="https://tanstack.com/db" target="_blank" rel="noreferrer">
            TanStack DB
          </a>
          . The belief graph itself lives server-side in{" "}
          <a href="https://github.com/nicia-ai/agent-stream-graph" target="_blank" rel="noreferrer">
            @nicia-ai/agent-stream-graph
          </a>
          . See README.md for what does and doesn't ship in this package.
        </p>
      </footer>
    </div>
  );
}
