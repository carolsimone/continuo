import { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import type { NodeSummary, NodesResponse } from './types';
import { formatDuration, formatRelative } from './node-helpers';

const PAGE = 50;

function fqnOf(n: NodeSummary): string {
  return `${n.service_name}.${n.schema_name}.${n.table_name}`;
}
function pct(v: number | null): string { return v === null ? '—' : `${v}%`; }
function durPair(avg: number | null, p95: number | null): string {
  if (avg === null && p95 === null) return '—';
  return `${formatDuration(avg)} / ${formatDuration(p95)}`;
}

export default function NodesCatalogPanel() {
  const navigate = useNavigate();
  const [operation, setOperation] = useState('run');
  const [search, setSearch] = useState('');
  const [service, setService] = useState('');
  const [nodes, setNodes] = useState<NodeSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [nameOptions, setNameOptions] = useState<string[]>([]);
  const offsetRef = useRef(0);
  const genRef = useRef(0);

  const fetchPage = useCallback(async (offset: number, append: boolean) => {
    const myGen = ++genRef.current;
    const params = new URLSearchParams();
    params.set('operation', operation);
    if (search) params.set('search', search);
    if (service) params.set('service', service);
    params.set('limit', String(PAGE));
    params.set('offset', String(offset));
    try {
      const res = await fetch(`/api/nodes?${params.toString()}`);
      if (myGen !== genRef.current) return;            // superseded while awaiting fetch
      if (!res.ok) { setError('Failed to load nodes'); return; }
      const data: NodesResponse = await res.json();
      if (myGen !== genRef.current) return;            // superseded while awaiting json
      setError(null);
      setTotal(data.total_count || 0);
      offsetRef.current = offset;
      setNodes(prev => (append ? [...prev, ...(data.nodes || [])] : (data.nodes || [])));
    } catch {
      if (myGen !== genRef.current) return;
      setError('Failed to load nodes');
    }
  }, [operation, search, service]);

  // Refetch the first page on filter change (debounced for search typing).
  useEffect(() => {
    const id = setTimeout(() => { fetchPage(0, false); }, 250);
    return () => clearTimeout(id);
  }, [fetchPage]);

  // Live refresh of the FIRST page only; suppressed once extra pages are loaded.
  useEffect(() => {
    const id = setInterval(() => {
      if (offsetRef.current === 0) fetchPage(0, false);
    }, 10000);
    return () => clearInterval(id);
  }, [fetchPage]);

  // Autocomplete: the COMPLETE distinct-name list, independent of the paginated
  // catalog. Refetched on mount and on service change (not on search/paging).
  useEffect(() => {
    const params = new URLSearchParams();
    if (service) params.set('service', service);
    fetch(`/api/nodes/names?${params.toString()}`)
      .then(r => (r.ok ? r.json() : { names: [] }))
      .then((d: { names?: string[] }) => setNameOptions(d.names || []))
      .catch(() => { /* autocomplete is best-effort */ });
  }, [service]);

  const services = Array.from(new Set(nodes.map(n => n.service_name))).sort();
  const canLoadMore = nodes.length < total;

  return (
    <>
      {error && <div className="info-strip info-strip--error">{error}</div>}

      <div className="form-field">
        <label htmlFor="node-operation">Operation</label>
        <select id="node-operation" value={operation} onChange={e => setOperation(e.target.value)}>
          <option value="run">Model</option>
          <option value="test">Test</option>
          <option value="build">Build</option>
        </select>
      </div>
      {operation === 'build' && (
        <div className="info-strip info-strip--info">
          <span className="info-strip__icon">ℹ</span>
          Build runs <code>dbt build</code> — it materializes each model and runs its
          attached tests together in a single execution (one pod), in dependency
          order. These stats combine model + tests, not either on its own.
        </div>
      )}

      <div className="form-field">
        <label htmlFor="node-search">Filter</label>
        <input id="node-search" className="node-search-input" list="node-name-options"
               placeholder="exact table name… e.g. customers"
               value={search} onChange={e => setSearch(e.target.value)} />
        <datalist id="node-name-options">
          {nameOptions.map(name => <option key={name} value={name} />)}
        </datalist>
        <label htmlFor="node-service">Service</label>
        <select id="node-service" value={service} onChange={e => setService(e.target.value)}>
          <option value="">all</option>
          {services.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>

      <section className="detail-card">
        <div className="section-header">
          <div className="section-header__main">
            <span className="section-header__title">Nodes</span>
            <span className="section-header__count">{total}</span>
          </div>
          <div className="section-header__sub">over last 50 runs</div>
        </div>

        {nodes.length === 0 && !error ? (
          <div className="info-strip info-strip--neutral">
            <span className="info-strip__icon">ⓘ</span>
            No node runs yet — trigger a schedule to populate.
          </div>
        ) : (
          <>
            <table className="nodes-table">
              <thead>
                <tr>
                  <th>Node</th><th>Last run</th><th>Success</th>
                  <th>Runs</th><th>Avg / p95</th><th>Flaky</th>
                </tr>
              </thead>
              <tbody>
                {nodes.map(n => {
                  const go = () => navigate(`/node/${encodeURIComponent(fqnOf(n))}`,
                                            { state: { from: { type: 'nodes' } } });
                  return (
                    <tr key={fqnOf(n)} onClick={go} tabIndex={0} role="button"
                        onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); go(); } }}>
                      <td>
                        <div className="nodes-node-name">{n.table_name}</div>
                        <div className="nodes-node-schema">{n.service_name} · {n.schema_name}</div>
                      </td>
                      <td>
                        <span className={`pill-sm pill-sm--${n.last_status ?? 'pending'}`}>{n.last_status ?? '—'}</span>
                        {' '}<span className="nodes-ts nodes-ts--dim">{formatRelative(n.last_run_at)}</span>
                      </td>
                      <td>{pct(n.success_rate_pct)}</td>
                      <td>{n.run_count}</td>
                      <td><code>{durPair(n.avg_duration_sec, n.p95_duration_sec)}</code></td>
                      <td>{n.flaky_rate_pct}%</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {canLoadMore && (
              <div className="nodes-loadmore">
                <button type="button" className="btn btn--secondary"
                        onClick={() => fetchPage(offsetRef.current + PAGE, true)}>
                  Load more
                </button>
              </div>
            )}
          </>
        )}
      </section>
    </>
  );
}
