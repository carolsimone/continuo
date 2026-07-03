import { useEffect, useState } from 'react';
import { ProposalDTO } from './types';
import { fetchProposals } from './remediation-api';
import { useCurrentUser } from './auth/AuthContext';
import CreatePrModal from './CreatePrModal';

// DiffView mirrors the LogView component in ReleaseDetailPage: toggle view/hide,
// fetch content from /api/releases/log?key=<uri>, and link out to the full source.
function DiffView({ uri }: { uri: string }) {
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const logUrl = `/api/releases/log?key=${encodeURIComponent(uri)}`;

  const toggle = () => {
    if (open) { setOpen(false); return; }
    setOpen(true);
    if (content === null) {
      setErr(null);
      fetch(logUrl)
        .then(r => (r.ok ? r.text() : Promise.reject(new Error(`HTTP ${r.status}`))))
        .then(setContent)
        .catch(e => setErr(e.message));
    }
  };

  return (
    <>
      <button type="button" className="btn btn--secondary" onClick={toggle}>
        {open ? 'hide' : 'view'}
      </button>{' '}
      <a className="btn btn--secondary" href={logUrl} target="_blank" rel="noreferrer">
        open full ↗
      </a>
      {open && (err
        ? <div className="info-strip info-strip--error"><span className="info-strip__icon">⚠</span>{err}</div>
        : <pre className="log-block">{content ?? 'loading…'}</pre>)}
    </>
  );
}

function sourceLabel(resolved: boolean): string {
  return resolved ? 'yes' : 'no';
}

// prStateBadge renders terminal PR outcomes as colored chips; non-terminal
// pr_state values stay plain text.
function prStateBadge(prState: string) {
  if (prState === 'merged' || prState === 'rejected') {
    return <span className={`pr-chip pr-chip--${prState}`}>{prState}</span>;
  }
  return <>{prState}</>;
}

export default function RemediationPanel() {
  const currentUser = useCurrentUser();
  const [proposals, setProposals] = useState<ProposalDTO[]>([]);
  const [selected, setSelected] = useState<ProposalDTO | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showCreatePrModal, setShowCreatePrModal] = useState(false);

  useEffect(() => {
    fetchProposals()
      .then(data => {
        setProposals(data);
        setError(null);
      })
      .catch(e => setError(e.message));
  }, []);

  return (
    <>
      {error && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>{error}
        </div>
      )}

      {proposals.length === 0 && !error && (
        <div className="info-strip info-strip--neutral">
          <span className="info-strip__icon">○</span>No proposals yet.
        </div>
      )}

      {proposals.length > 0 && (
        <>
          <div className="section-header">
            <div className="section-header__main">
              <span className="section-header__title">Proposals</span>
              <span className="section-header__count">{proposals.length}</span>
            </div>
          </div>
          <table className="nodes-table">
            <thead>
              <tr>
                <th>Node</th>
                <th>Release</th>
                <th>Confidence</th>
                <th>Source</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {proposals.map(p => (
                <tr
                  key={p.id}
                  onClick={() => setSelected(prev => (prev?.id === p.id ? null : p))}
                >
                  <td>{p.node_id}</td>
                  <td>{p.release_id}</td>
                  <td>{p.confidence}</td>
                  <td>{sourceLabel(p.source_resolved)}</td>
                  <td>{p.status}{p.pr_state ? <> · {prStateBadge(p.pr_state)}</> : null}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {selected && (
        <div className="detail-card remediation-detail">
          <div className="section-header">
            <div className="section-header__main">
              <span className="section-header__title">{selected.node_id}</span>
            </div>
            <div className="section-header__sub">
              release {selected.release_id} · confidence {selected.confidence}
            </div>
          </div>

          <div className="detail-card__body">
            <p className="detail-card__rationale">
              {selected.rationale}
            </p>

            {selected.diff_uri && (
              <div className="detail-card__row">
                <DiffView uri={selected.diff_uri} />
              </div>
            )}

            {!selected.source_resolved && (
              <div className="info-strip info-strip--warning detail-card__row">
                <span className="info-strip__icon">⚠</span>
                No real-source fix — a PR cannot be opened for this proposal.
              </div>
            )}

            {selected.pr_url && (
              <a
                className="btn btn--secondary"
                href={selected.pr_url}
                target="_blank"
                rel="noreferrer"
              >
                open PR ↗
              </a>
            )}

            {currentUser?.role === 'operator' && selected.source_resolved && !selected.pr_url && (
              <button
                type="button"
                className="btn btn--secondary"
                onClick={() => setShowCreatePrModal(true)}
              >
                Create PR
              </button>
            )}

            {showCreatePrModal && selected && (
              <CreatePrModal
                proposal={selected}
                onClose={() => setShowCreatePrModal(false)}
                onCreated={(prUrl) => {
                  setSelected(prev => prev ? { ...prev, pr_url: prUrl } : prev);
                  setProposals(prev => prev.map(p => p.id === selected.id ? { ...p, pr_url: prUrl } : p));
                  setShowCreatePrModal(false);
                }}
              />
            )}
          </div>
        </div>
      )}
    </>
  );
}
