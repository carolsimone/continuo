import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { ReleaseDetail, NodeValidationResult } from './types';

function LogView({ uri }: { uri: string }) {
  const [open, setOpen] = useState(false);
  const [content, setContent] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const toggle = () => {
    if (open) { setOpen(false); return; }
    setOpen(true);
    if (content === null) {
      fetch(`/api/releases/log?key=${encodeURIComponent(uri)}`)
        .then(r => (r.ok ? r.text() : Promise.reject(new Error(`HTTP ${r.status}`))))
        .then(setContent)
        .catch(e => setErr(e.message));
    }
  };

  return (
    <div>
      <button className="btn btn--small" onClick={toggle}>{open ? 'hide' : 'view'}</button>
      {open && (err ? <pre className="log-view log-view--error">{err}</pre>
                    : <pre className="log-view">{content ?? 'loading…'}</pre>)}
    </div>
  );
}

export default function ReleaseDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [rel, setRel] = useState<ReleaseDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(`/api/releases/${id}`)
      .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then(setRel)
      .catch(e => setError(e.message));
  }, [id]);

  if (error) return <div className="page"><div className="info-strip info-strip--error">{error}</div></div>;
  if (!rel) return <div className="page"><p className="empty">Loading…</p></div>;

  const perNode: NodeValidationResult[] = rel.per_node_results ?? [];

  return (
    <div className="page">
      <header className="page-header">
        <h1>{rel.release_id}</h1>
        <Link to="/?tab=releases" className="btn btn--small">← back</Link>
      </header>
      <main className="page-content page-content--readable">
        <p>Status: <strong>{rel.status}</strong>{rel.reject_reason ? ` · ${rel.reject_reason}` : ''}</p>
        <p>Image tags: {Object.entries(rel.image_tags || {}).map(([s, t]) => `${s}=${t}`).join(', ') || '—'}</p>

        <h3>Timeline</h3>
        <ul>{rel.transitions.map((t, i) => <li key={i}>{t.to} · {t.at.slice(0, 19).replace('T', ' ')}</li>)}</ul>

        <h3>Per-node validation</h3>
        {perNode.length === 0 ? <p className="empty">No per-node results.</p> : (
          <table className="release-table">
            <thead><tr><th>Node</th><th>Status</th><th>Duration</th><th>Log</th></tr></thead>
            <tbody>
              {perNode.map(n => (
                <tr key={n.node_id}>
                  <td>{n.node_id}</td>
                  <td>{n.status}</td>
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
