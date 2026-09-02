import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router';
import { ReleaseListItem, ReleasesListResponse, CurrentProd, PipelineResponse } from './types';
import { releasePillClass, reasonLabel } from './release-helpers';

const STATUS_FILTERS = ['', 'promoted', 'rejected', 'superseded', 'validating', 'seed_building', 'parsing', 'compiling', 'received'];

export default function ReleasesPanel() {
  const navigate = useNavigate();
  const [items, setItems] = useState<ReleaseListItem[]>([]);
  const [nextCursor, setNextCursor] = useState('');
  const [status, setStatus] = useState('');
  const [currentProd, setCurrentProd] = useState<CurrentProd | null>(null);
  const [pipeline, setPipeline] = useState<PipelineResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = (cursor: string, replace: boolean) => {
    const qs = new URLSearchParams();
    if (status) qs.set('status', status);
    if (cursor) qs.set('cursor', cursor);
    fetch(`/api/releases?${qs.toString()}`)
      .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((data: ReleasesListResponse) => {
        setItems(prev => (replace ? data.releases : [...prev, ...data.releases]));
        setNextCursor(data.next_cursor || '');
        setError(null);
      })
      .catch(e => setError(e.message));
  };

  useEffect(() => { load('', true); /* reload on filter change */ }, [status]);

  useEffect(() => {
    const f = () => {
      fetch('/api/releases/current-prod').then(r => r.json()).then(setCurrentProd).catch(() => {});
      fetch('/api/pipeline').then(r => r.json()).then(setPipeline).catch(() => {});
    };
    f();
    const id = setInterval(f, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <>
      {error && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>{error}
        </div>
      )}

      {currentProd?.current_prod_release_id ? (
        <div className="info-strip info-strip--info">
          <span className="info-strip__icon">ⓘ</span>
          Live prod · <strong>{currentProd.current_prod_release_id}</strong> · {currentProd.node_count} nodes
        </div>
      ) : (
        <div className="info-strip info-strip--neutral">
          <span className="info-strip__icon">○</span>No release promoted yet.
        </div>
      )}

      {pipeline?.active ? (
        <div className="info-strip info-strip--warning">
          <span className="info-strip__icon">⚠</span>
          {pipeline.active.run_kind === 'verification' ? (
            <>
              In flight · verification run{' '}
              <Link to={`/verifications/${pipeline.active.run_id}`}><strong>{pipeline.active.run_id}</strong></Link>
              {' '}for <Link to={`/releases/${pipeline.active.verifies_release_id}`}>{pipeline.active.verifies_release_id}</Link>
              {' '}(attempt {pipeline.active.attempt}, service {pipeline.active.service}) · {pipeline.active.status}
            </>
          ) : (
            <>
              In flight · <Link to={`/releases/${pipeline.active.run_id}`}><strong>{pipeline.active.run_id}</strong></Link> · {pipeline.active.status}
            </>
          )}
        </div>
      ) : (
        <div className="info-strip info-strip--neutral">
          <span className="info-strip__icon">○</span>Nothing in flight.
        </div>
      )}

      <div className="form-field">
        <label htmlFor="release-status-filter">Status</label>
        <select
          id="release-status-filter"
          value={status}
          onChange={e => setStatus(e.target.value)}
        >
          {STATUS_FILTERS.map(s => (
            <option key={s} value={s}>{s === '' ? 'all' : s}</option>
          ))}
        </select>
      </div>

      {items.length === 0 && !error && <p className="empty">No releases found.</p>}
      {items.length > 0 && (
        <>
          <div className="section-header">
            <div className="section-header__main">
              <span className="section-header__title">Releases</span>
              <span className="section-header__count">{items.length}</span>
            </div>
          </div>
          <table className="nodes-table">
            <thead>
              <tr><th>Release</th><th>Author</th><th>Status</th><th>Reason</th><th>When</th><th>Nodes</th></tr>
            </thead>
            <tbody>
              {items.map(r => (
                <tr
                  key={r.release_id}
                  onClick={e => {
                    // Whole-row click is a mouse convenience; the release-id
                    // cell holds the real link. Defer to it for clicks that
                    // land on the anchor, and leave modifier-clicks to the
                    // browser so Cmd/Ctrl/middle-click can open a new tab.
                    if (e.metaKey || e.ctrlKey || e.shiftKey) return;
                    if ((e.target as HTMLElement).closest('a')) return;
                    navigate(`/releases/${r.release_id}`);
                  }}
                >
                  <td>
                    <Link
                      className="nodes-node-name"
                      to={`/releases/${r.release_id}`}
                      onClick={e => e.stopPropagation()}
                    >
                      {r.release_id}
                    </Link>
                  </td>
                  <td>
                    {r.author?.login ? (
                      <a
                        className="release-author"
                        href={r.author.html_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        onClick={e => e.stopPropagation()}
                      >
                        {r.author.avatar_url && (
                          <img className="release-author__avatar" src={r.author.avatar_url} alt="" width={16} height={16} />
                        )}
                        @{r.author.login}
                      </a>
                    ) : r.author?.name ? (
                      <span className="release-author release-author--name">{r.author.name}</span>
                    ) : (
                      <span className="nodes-dash">—</span>
                    )}
                  </td>
                  <td>
                    <span className={`pill-sm ${releasePillClass(r.status).replace('pill--', 'pill-sm--')}`}>
                      {r.status}
                    </span>
                  </td>
                  <td>
                    {r.status === 'rejected' && r.reject_reason ? (
                      <span className="nodes-reason">{reasonLabel(r.reject_reason)}</span>
                    ) : (
                      <span className="nodes-dash">—</span>
                    )}
                  </td>
                  <td className="nodes-ts">{(r.resolved_at || r.created_at || '').slice(0, 19).replace('T', ' ')}</td>
                  <td>{r.node_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
      {nextCursor && (
        <button className="btn btn--secondary" onClick={() => load(nextCursor, false)}>
          Load more
        </button>
      )}
    </>
  );
}
