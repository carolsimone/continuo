import { useEffect } from 'react';
import type { NodeRun } from './types';
import { kindLabel } from './node-helpers';

interface Props {
  runs: NodeRun[];
  onPick: (runId: string) => void;
  onClose: () => void;
}

function isTerminal(r: NodeRun): boolean {
  const s = r.task_status;
  return s === 'succeeded' || s === 'failed' || s === 'cancelled';
}

function formatTime(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

export default function RunSourcePickerDialog({ runs, onPick, onClose }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [onClose]);

  const eligible = runs.filter(isTerminal);

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
          The node will execute with the (image_tag, manifest_version) pair from the run you pick.
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
          <button type="button" className="dialog-btn dialog-btn--secondary" onClick={onClose}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}
