/**
 * A reusable, unopinionated scrubber over an ordered list of steps.
 *
 * It knows nothing about belief graphs, entities, or recorded time — it
 * takes a list of `{ id, label }` steps and an index, and reports index
 * changes. `useRecordedTimeline` is where "step" becomes "recorded-time
 * anchor"; this component would work identically driving a video's
 * keyframes or a changelog's releases. Lift it into another app as-is.
 *
 * Built on a native `<input type="range">` rather than custom pointer-event
 * handling: that's what makes dragging feel physically correct for free
 * (including touch and trackpad), and keyboard support (arrow keys, Home,
 * End, Page Up/Down) and slider a11y semantics come with the element
 * rather than needing to be reimplemented.
 */
import { useId } from "react";
import type { CSSProperties } from "react";

export interface ScrubberStep {
  readonly id: string;
  readonly label: string;
}

export interface RecordedScrubberProps {
  readonly steps: readonly ScrubberStep[];
  readonly index: number;
  readonly onChange: (index: number) => void;
  readonly ariaLabel: string;
  /** Rendered above the track — typically the currently-selected step's label. */
  readonly caption?: string | undefined;
}

export function RecordedScrubber({ steps, index, onChange, ariaLabel, caption }: RecordedScrubberProps) {
  const trackId = useId();
  const lastIndex = Math.max(steps.length - 1, 0);
  const clampedIndex = Math.min(Math.max(index, 0), lastIndex);
  const percent = lastIndex === 0 ? 0 : (clampedIndex / lastIndex) * 100;
  const trackStyle = { "--scrubber-fill": `${percent}%` } as CSSProperties;

  return (
    <div className="scrubber" style={trackStyle}>
      {caption !== undefined ? <div className="scrubber-caption">{caption}</div> : null}
      <div className="scrubber-track-wrap">
        <input
          id={trackId}
          className="scrubber-input"
          type="range"
          min={0}
          max={lastIndex}
          step={1}
          value={clampedIndex}
          disabled={steps.length === 0}
          aria-label={ariaLabel}
          onChange={(event) => onChange(Number(event.currentTarget.value))}
        />
        <div className="scrubber-ticks" aria-hidden="true">
          {steps.map((step, stepIndex) => (
            <span
              key={step.id}
              className={`scrubber-tick${stepIndex === clampedIndex ? " scrubber-tick--active" : ""}${
                stepIndex < clampedIndex ? " scrubber-tick--past" : ""
              }`}
              style={{ left: lastIndex === 0 ? "0%" : `${(stepIndex / lastIndex) * 100}%` }}
            />
          ))}
        </div>
      </div>
      <div className="scrubber-labels">
        <span>{steps[0]?.label ?? ""}</span>
        <span>{steps[lastIndex]?.label ?? ""}</span>
      </div>
    </div>
  );
}
