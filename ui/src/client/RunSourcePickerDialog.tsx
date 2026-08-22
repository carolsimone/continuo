import { useEffect } from 'react';
import type { NodeRun } from './types';
import { kindLabel } from './node-helpers';

interface Props {
  runs: NodeRun[];
  operation: 'run' | 'test' | 'build';
  onPick: (runId: string) => void;
  onClose: () => void;
}

// Stale-mode (snapshot_of_run) eligibility mirrors state.TriggerSingleNodeRun's
// validation: the source RUN must be terminal (scheduler-level status), not the
// per-task status on this node. A FAILED run where this node stayed PENDING is
// a valid source; an in-flight run where this node already succeeded is NOT.
function isTerminalRun(r: NodeRun): boolean {
  const s = r.terminal_status;
  return s === 'succeeded' || s === 'failed' || s === 'cancelled';
}

function formatTime(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

export default function RunSourcePickerDialog({ runs, operation, onPick, onClose }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [onClose]);

  const eligible = runs.filter(isTerminalRun);
  const opWord = operation === 'test' ? 'test' : operation === 'build' ? 'build' : 'run';

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="run-source-picker-title"
        onClick={e => e.stopPropagation()}
      >
        <h2 id="run-source-picker-title" className="dialog-title">Pick a past run</h2>
        <p className="dialog-subtitle">
          Pick a past run to {opWord} this node against — it will execute with that run's
          (image_tag, manifest_version) pair.
        </p>
        {eligible.length === 0 ? (
          <p className="dialog-empty">No past runs available for this node yet.</p>
        ) : (
          <ul className="run-source-list">
            {eligible.map(r => (
              <li key={r.run_id}>
                <button
                  type="button"
                  className="run-source-row"
                  onClick={() => onPick(r.run_id)}
                  aria-label={`${r.run_id} ${kindLabel(r.kind)} ${r.task_status} ${formatTime(r.created_at)}`}
                >
                  <span className="run-source-row-time">{formatTime(r.created_at)}</span>
                  <span className="run-source-row-kind">{kindLabel(r.kind)}</span>
                  <span className={`pill-sm pill-sm--${r.task_status}`}>{r.task_status}</span>
                  <span className="run-source-row-image">{r.image_tag || '—'}</span>
                </button>
              </li>
            ))}
          </ul>
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
