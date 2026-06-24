import { useEffect, useState } from 'react';
import { ProposalDTO } from './types';
import { fetchProposals } from './remediation-api';

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

function statusLabel(proposal: ProposalDTO): string {
  if (proposal.pr_state) return `${proposal.status} · ${proposal.pr_state}`;
  return proposal.status;
}

export default function RemediationPanel() {
  const [proposals, setProposals] = useState<ProposalDTO[]>([]);
  const [selected, setSelected] = useState<ProposalDTO | null>(null);
  const [error, setError] = useState<string | null>(null);

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
                  style={{ cursor: 'pointer' }}
                  onClick={() => setSelected(prev => (prev?.id === p.id ? null : p))}
                >
                  <td>{p.node_id}</td>
                  <td>{p.release_id}</td>
                  <td>{p.confidence}</td>
                  <td>{sourceLabel(p.source_resolved)}</td>
                  <td>{statusLabel(p)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {selected && (
        <div className="detail-card" style={{ marginTop: 16 }}>
          <div className="section-header">
            <div className="section-header__main">
              <span className="section-header__title">{selected.node_id}</span>
            </div>
            <div className="section-header__sub">
              release {selected.release_id} · confidence {selected.confidence}
            </div>
          </div>

          <div style={{ padding: '12px 16px' }}>
            <p style={{ margin: '0 0 12px', fontSize: 13, color: '#374151' }}>
              {selected.rationale}
            </p>

            {selected.diff_uri && (
              <div style={{ marginBottom: 12 }}>
                <DiffView uri={selected.diff_uri} />
              </div>
            )}

            {!selected.source_resolved && (
              <div className="info-strip info-strip--warning" style={{ marginBottom: 12 }}>
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

            {/* Task 16 adds the Create-PR operator action here */}
          </div>
        </div>
      )}
    </>
  );
}
