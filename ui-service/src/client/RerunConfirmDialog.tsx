import { useEffect, useMemo } from 'react';
import { getDriftMessage } from './drift-helpers';

interface Props {
  state: 'stale' | 'unknown';
  runGen: number;
  latestGen: number;
  onConfirm: () => void;
  onCancel: () => void;
}

/**
 * Portal-rendered confirmation dialog for rerunning a node when the run's
 * pinned topology drifts from the orchestrator's latest. Mirrors the structure
 * of CancelDialog: backdrop + centered dialog body, ESC and outside-click both
 * dismiss.
 *
 * Modal-freeze: the runGen / latestGen displayed are frozen at mount time via
 * useMemo. If the parent re-fetches and the underlying numbers change while
 * the dialog is open, the dialog continues to show the values from when the
 * user opened it. Avoids the rare confusion where the warning text mutates
 * mid-decision.
 */
export default function RerunConfirmDialog({
  state,
  runGen,
  latestGen,
  onConfirm,
  onCancel,
}: Props) {
  // Freeze gen values at first render. The parent unmounts/remounts the
  // dialog on each open, so each open captures fresh values.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const frozen = useMemo(() => ({ runGen, latestGen }), []);
  const msg = getDriftMessage(state, frozen.runGen, frozen.latestGen);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onCancel();
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onCancel]);

  return (
    <div
      className="dialog-overlay"
      data-testid="rerun-confirm-overlay"
      onClick={onCancel}
    >
      <div
        className="dialog rerun-confirm-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="rerun-confirm-title"
        onClick={e => e.stopPropagation()}
      >
        <h2 className="dialog-title rerun-confirm-title" id="rerun-confirm-title">
          <span aria-hidden="true" className="rerun-confirm-title-icon">
            {state === 'unknown' ? '?' : '⚠'}
          </span>
          {msg.modalTitle}
        </h2>
        <span className={`rerun-confirm-chip rerun-confirm-chip--${state}`}>
          {msg.modalChip}
        </span>
        <div className="rerun-confirm-body">
          {msg.modalBody.split('\n\n').map((para, idx) => (
            <p key={idx} className="rerun-confirm-paragraph">{para}</p>
          ))}
        </div>
        <div className="dialog-actions">
          <button
            type="button"
            className="dialog-btn dialog-btn--secondary"
            onClick={onCancel}
          >
            Cancel
          </button>
          <button
            type="button"
            className="dialog-btn dialog-btn--primary"
            onClick={onConfirm}
          >
            {msg.modalCta}
          </button>
        </div>
      </div>
    </div>
  );
}
