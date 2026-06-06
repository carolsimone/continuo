import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ReleaseListItem, ReleasesListResponse, CurrentProd } from './types';
import { firstInFlight, releasePillClass } from './release-helpers';

const STATUS_FILTERS = ['', 'promoted', 'rejected', 'validating', 'received'];

export default function ReleasesPanel() {
  const navigate = useNavigate();
  const [items, setItems] = useState<ReleaseListItem[]>([]);
  const [nextCursor, setNextCursor] = useState('');
  const [status, setStatus] = useState('');
  const [currentProd, setCurrentProd] = useState<CurrentProd | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = (cursor: string, replace: boolean) => {
    const qs = new URLSearchParams();
    if (status) qs.set('status', status);
    if (cursor) qs.set('cursor', cursor);
    fetch(`/api/releases?${qs.toString()}`)
      .then(r => r.json())
      .then((data: ReleasesListResponse) => {
        setItems(prev => (replace ? data.releases : [...prev, ...data.releases]));
        setNextCursor(data.next_cursor || '');
        setError(null);
      })
      .catch(e => setError(e.message));
  };

  useEffect(() => { load('', true); /* reload on filter change */ }, [status]);

  useEffect(() => {
    const f = () => fetch('/api/releases/current-prod').then(r => r.json()).then(setCurrentProd).catch(() => {});
    f();
    const id = setInterval(f, 5000);
    return () => clearInterval(id);
  }, []);

  const inFlight = firstInFlight(items);

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

      {inFlight ? (
        <div className="info-strip info-strip--warning">
          <span className="info-strip__icon">⚠</span>
          In flight · <strong>{inFlight.release_id}</strong> · {inFlight.status}
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
              <tr><th>Release</th><th>Status</th><th>When</th><th>Nodes</th></tr>
            </thead>
            <tbody>
              {items.map(r => (
                <tr
                  key={r.release_id}
                  tabIndex={0}
                  role="button"
                  onClick={() => navigate(`/releases/${r.release_id}`)}
                  onKeyDown={e => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      e.preventDefault();
                      navigate(`/releases/${r.release_id}`);
                    }
                  }}
                >
                  <td><div className="nodes-node-name">{r.release_id}</div></td>
                  <td>
                    <span className={`pill-sm ${releasePillClass(r.status).replace('pill--', 'pill-sm--')}`}>
                      {r.status}
                    </span>
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
