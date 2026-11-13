import { useEffect, useState } from 'react';

interface Props {
  fqn: string;
  // Describes the past snapshot being rebuilt; absent when the latest version runs.
  snapshot?: string;
  // True for a seed: its full refresh also rebuilds the views that read it.
  seed?: boolean;
  // The objects the seed's last load stopped on (lowercased schema.name); each
  // is offered as one the load may drop. Absent or empty offers none.
  objects?: string[];
  // Called with the objects the operator ticked, in the order of `objects`.
  onConfirm: (acknowledged: string[]) => void;
  onClose: () => void;
}

// FullRefreshConfirmDialog guards a full refresh: the node's production table
// is dropped and rebuilt, which also drops dependent views on Postgres. For a
// seed whose last load stopped on objects it would have dropped, it lists them
// so the operator can tick the ones this refresh may drop.
export default function FullRefreshConfirmDialog({ fqn, snapshot, seed, objects, onConfirm, onClose }: Props) {
  const listed = Array.from(new Set(objects ?? []));
  const [ticked, setTicked] = useState<ReadonlySet<string>>(new Set());
  const toggle = (name: string) => setTicked(prev => {
    const next = new Set(prev);
    if (!next.delete(name)) next.add(name);
    return next;
  });
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [onClose]);

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className={listed.length > 0 ? 'dialog dialog--wide' : 'dialog'}
        role="dialog"
        aria-modal="true"
        aria-labelledby="full-refresh-dialog-title"
        onClick={e => e.stopPropagation()}
      >
        <h2 className="dialog-title" id="full-refresh-dialog-title">Full refresh {fqn}?</h2>
        {seed ? (
          <p className="dialog-subtitle">
            The table is dropped and rebuilt from scratch with <code>--full-refresh</code>, and the
            continuo views that read it are rebuilt in the same run.
          </p>
        ) : (
          <p className="dialog-subtitle">
            The table is dropped and rebuilt from scratch with <code>--full-refresh</code>. Only this
            node runs. On Postgres, views that select from it are dropped too, until their own models
            run again.
          </p>
        )}
        {snapshot && <p className="dialog-subtitle">{snapshot}</p>}
        {listed.length > 0 && (
          <>
            <div className="info-strip info-strip--warning">
              <span className="info-strip__icon">⚠</span>
              The last load of this seed stopped because these objects read it and no run rebuilds
              them. Tick the ones this refresh may drop.
            </div>
            {listed.map((name, i) => (
              <label className="rerun-mode-option" key={name}>
                <input
                  type="checkbox"
                  checked={ticked.has(name)}
                  aria-labelledby={`full-refresh-object-${i}`}
                  onChange={() => toggle(name)}
                />
                <span className="rerun-mode-option__title" id={`full-refresh-object-${i}`}>{name}</span>
                <span className="rerun-mode-option__desc">Reads this seed. Tick it to let the load drop it.</span>
              </label>
            ))}
          </>
        )}
        <div className="dialog-actions">
          <button type="button" className="btn btn--secondary" onClick={onClose} autoFocus>Cancel</button>
          <button type="button" className="btn btn--danger" onClick={() => onConfirm(listed.filter(name => ticked.has(name)))}>Full refresh</button>
        </div>
      </div>
    </div>
  );
}
