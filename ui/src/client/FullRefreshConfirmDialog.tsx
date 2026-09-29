import { useEffect } from 'react';

interface Props {
  fqn: string;
  onConfirm: () => void;
  onClose: () => void;
}

// FullRefreshConfirmDialog guards a full refresh: the node's production table
// is dropped and rebuilt, which also drops dependent views on Postgres.
export default function FullRefreshConfirmDialog({ fqn, onConfirm, onClose }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [onClose]);

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="full-refresh-dialog-title"
        onClick={e => e.stopPropagation()}
      >
        <h2 className="dialog-title" id="full-refresh-dialog-title">Full refresh {fqn}?</h2>
        <p className="dialog-subtitle">
          The table is dropped and rebuilt from scratch with <code>--full-refresh</code>. Only this
          node runs. On Postgres, views that select from it are dropped too, until their own models
          run again.
        </p>
        <div className="dialog-actions">
          <button type="button" className="btn btn--secondary" onClick={onClose}>Cancel</button>
          <button type="button" className="btn btn--danger" onClick={onConfirm} autoFocus>Full refresh</button>
        </div>
      </div>
    </div>
  );
}
