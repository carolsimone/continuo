import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ReleaseDetail, NodeValidationResult } from './types';
import { releasePillClass } from './release-helpers';

function LogView({ uri }: { uri: string }) {
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
      <button type="button" className="btn btn--secondary" onClick={toggle}>{open ? 'hide' : 'view'}</button>{' '}
      <a className="btn btn--secondary" href={logUrl} target="_blank" rel="noreferrer">open full log ↗</a>
      {open && (err
        ? <div className="info-strip info-strip--error"><span className="info-strip__icon">⚠</span>{err}</div>
        : <pre className="log-block">{content ?? 'loading…'}</pre>)}
    </>
  );
}

export default function ReleaseDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [rel, setRel] = useState<ReleaseDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(`/api/releases/${id}`)
      .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then(setRel)
      .catch(e => setError(e.message));
  }, [id]);

  if (error) {
    return (
      <div className="page">
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>{error}
        </div>
      </div>
    );
  }
  if (!rel) return <div className="page"><p className="empty">Loading…</p></div>;

  const perNode: NodeValidationResult[] = rel.per_node_results ?? [];

  return (
    <div className="page">
      <header className="page-header">
        <button type="button" className="detail-back-link" onClick={() => navigate('/?tab=releases')}>← Back</button>
        <div className="detail-page-title">{rel.release_id}</div>
        <span className={`pill ${releasePillClass(rel.status)}`}>{rel.status}</span>
      </header>

      {rel.reject_reason && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>{rel.reject_reason}
        </div>
      )}

      <main className="page-content page-content--readable">
        <p>Image tags: {Object.entries(rel.image_tags || {}).map(([s, t]) => `${s}=${t}`).join(', ') || '—'}</p>

        <div className="section-header">
          <div className="section-header__main">
            <span className="section-header__title">Timeline</span>
          </div>
        </div>
        <ul>
          {rel.transitions.map((t, i) => (
            <li key={i}>{t.to} · {t.at.slice(0, 19).replace('T', ' ')}</li>
          ))}
        </ul>

        <div className="section-header">
          <div className="section-header__main">
            <span className="section-header__title">Per-node validation</span>
            <span className="section-header__count">{perNode.length}</span>
          </div>
        </div>
        {perNode.length === 0 ? <p className="empty">No per-node results.</p> : (
          <table className="nodes-table">
            <thead>
              <tr><th>Node</th><th>Status</th><th>Duration</th><th>Log</th></tr>
            </thead>
            <tbody>
              {perNode.map(n => (
                <tr key={n.node_id}>
                  <td><div className="nodes-node-name">{n.node_id}</div></td>
                  <td>
                    <span className={`pill-sm ${releasePillClass(n.status).replace('pill--', 'pill-sm--')}`}>
                      {n.status}
                    </span>
                  </td>
                  <td>{n.duration_ms ? `${n.duration_ms} ms` : '—'}</td>
                  <td>{n.dbt_log_uri ? <LogView uri={n.dbt_log_uri} /> : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </main>
    </div>
  );
}
