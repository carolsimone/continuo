import { useEffect } from 'react';
import type { NodeRun } from './types';
import { groupRunsBySnapshot } from './node-helpers';

interface Props {
  runs: NodeRun[];
  operation: 'run' | 'test' | 'build';
  onPick: (runId: string) => void;
  onClose: () => void;
}

// Compact absolute date for a snapshot's last run, e.g. "11 Sep 2026".
function formatWhen(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime())
    ? iso
    : d.toLocaleDateString(undefined, { day: '2-digit', month: 'short', year: 'numeric' });
}

export default function RunSourcePickerDialog({ runs, operation, onPick, onClose }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [onClose]);

  const snapshots = groupRunsBySnapshot(runs);
  const opWord = operation === 'test' ? 'test' : operation === 'build' ? 'build' : 'run';
  const totalRuns = snapshots.reduce((n, s) => n + s.runCount, 0);

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog dialog--wide"
        role="dialog"
        aria-modal="true"
        aria-labelledby="run-source-picker-title"
        onClick={e => e.stopPropagation()}
      >
        <h2 id="run-source-picker-title" className="dialog-title">Run with an old snapshot</h2>
        <p className="dialog-subtitle">
          {`Pick a snapshot to ${opWord} this node against — it runs with that snapshot's image and manifest version.`}
        </p>

        {snapshots.length === 0 ? (
          <div className="info-strip info-strip--neutral">
            <span className="info-strip__icon">–</span>
            No past snapshots available for this node yet.
          </div>
        ) : (
          <>
            <div className="section-header">
              <div className="section-header__main">
                <span className="section-header__title">Snapshots</span>
                <span className="section-header__count">{snapshots.length}</span>
              </div>
              <div className="section-header__sub">
                {totalRuns} run{totalRuns === 1 ? '' : 's'} total
              </div>
            </div>
            <ul className="pick-list">
              {snapshots.map(s => {
                // The accessible name carries the full (image_tag, manifest_version)
                // identity: two snapshots sharing an image tag but differing only in
                // manifest must not be announced identically to a screen reader.
                const ariaLabel = [
                  `${s.imageTag || 'unknown image'} snapshot`,
                  s.manifestVersion ? `manifest ${s.manifestVersion}` : null,
                  `${s.runCount} run${s.runCount === 1 ? '' : 's'}`,
                  `last run ${s.status}`,
                ].filter(Boolean).join(', ');
                return (
                <li key={s.representativeRunId}>
                  <button
                    type="button"
                    className="pick-row"
                    onClick={() => onPick(s.representativeRunId)}
                    aria-label={ariaLabel}
                  >
                    <span className="pick-row__main">
                      <span className="pick-row__id">{s.imageTag || '—'}</span>
                      <span className={`pill-sm pill-sm--${s.status}`}>{s.status}</span>
                    </span>
                    <span className="pick-row__meta">
                      {s.manifestVersion && (
                        <>
                          <span>manifest {s.manifestVersion}</span>
                          <span className="pick-row__sep">·</span>
                        </>
                      )}
                      <span>{s.runCount} run{s.runCount === 1 ? '' : 's'}</span>
                      <span className="pick-row__sep">·</span>
                      <span>last {formatWhen(s.lastRunAt)}</span>
                    </span>
                  </button>
                </li>
                );
              })}
            </ul>
          </>
        )}

        <div className="dialog-actions">
          <button type="button" className="btn btn--secondary" onClick={onClose}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}
