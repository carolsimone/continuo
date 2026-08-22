import { useState } from 'react';

export type RerunFailedMode = 'this' | 'latest';

interface Props {
  driftLabel: string | null;
  submitting: boolean;
  onClose: () => void;
  onSubmit: (mode: RerunFailedMode) => void;
}

export default function RerunFailedModal({ driftLabel, submitting, onClose, onSubmit }: Props) {
  const [mode, setMode] = useState<RerunFailedMode>('this');

  return (
    <div className="dialog-overlay" role="dialog" aria-modal="true">
      <div className="dialog rerun-confirm-dialog">
        <h2 className="dialog-title">Rerun failed nodes</h2>

        {driftLabel && (
          <div className="info-strip info-strip--warning">
            <span className="info-strip__icon">⚠</span>
            <span>{driftLabel}</span>
          </div>
        )}

        <label className="rerun-mode-option">
          <input
            type="radio"
            name="rerun-mode"
            value="this"
            checked={mode === 'this'}
            onChange={() => setMode('this')}
          />
          <span className="rerun-mode-option__title">This snapshot</span>
          <span className="rerun-mode-option__desc">
            Reuse the image + manifest pinned to this run. Reproduces the failure exactly.
          </span>
        </label>

        <label className="rerun-mode-option">
          <input
            type="radio"
            name="rerun-mode"
            value="latest"
            checked={mode === 'latest'}
            onChange={() => setMode('latest')}
          />
          <span className="rerun-mode-option__title">Latest snapshot</span>
          <span className="rerun-mode-option__desc">
            Use current image + manifest. Includes any new nodes added since.
          </span>
        </label>

        <div className="dialog-actions">
          <button
            type="button"
            className="btn btn--secondary"
            onClick={onClose}
            disabled={submitting}
          >
            Cancel
          </button>
          <button
            type="button"
            className={`btn btn--primary${submitting ? ' is-loading' : ''}`}
            onClick={() => onSubmit(mode)}
            disabled={submitting}
          >
            Rerun
          </button>
        </div>
      </div>
    </div>
  );
}
