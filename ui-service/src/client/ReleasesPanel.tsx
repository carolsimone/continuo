import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { ReleaseListItem, ReleasesListResponse, CurrentProd } from './types';
import { firstInFlight } from './release-helpers';

const STATUS_FILTERS = ['', 'promoted', 'rejected', 'validating', 'received'];

export default function ReleasesPanel() {
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
      {error && <div className="info-strip info-strip--error">{error}</div>}

      <div className="release-banner">
        <div className="release-banner__card">
          <h3>Live prod</h3>
          {currentProd?.current_prod_release_id
            ? <p><strong>{currentProd.current_prod_release_id}</strong> · {currentProd.node_count} nodes</p>
            : <p className="empty">No release promoted yet.</p>}
        </div>
        <div className="release-banner__card">
          <h3>In flight</h3>
          {inFlight
            ? <p><strong>{inFlight.release_id}</strong> · {inFlight.status}</p>
            : <p className="empty">Nothing in flight.</p>}
        </div>
      </div>

      <div className="release-filter">
        <label>Status:&nbsp;
          <select value={status} onChange={e => setStatus(e.target.value)}>
            {STATUS_FILTERS.map(s => <option key={s} value={s}>{s === '' ? 'all' : s}</option>)}
          </select>
        </label>
      </div>

      {items.length === 0 && !error && <p className="empty">No releases found.</p>}
      {items.length > 0 && (
        <table className="release-table">
          <thead><tr><th>Release</th><th>Status</th><th>When</th><th>Nodes</th></tr></thead>
          <tbody>
            {items.map(r => (
              <tr key={r.release_id}>
                <td><Link to={`/releases/${r.release_id}`}>{r.release_id}</Link></td>
                <td>{r.status}</td>
                <td>{(r.resolved_at || r.created_at || '').slice(0, 19).replace('T', ' ')}</td>
                <td>{r.node_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {nextCursor && <button className="btn" onClick={() => load(nextCursor, false)}>Load more</button>}
    </>
  );
}
