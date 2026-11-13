import { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import type { NodeSummary, NodesResponse } from './types';
import { dormantTitle, formatDuration, formatRelative } from './node-helpers';
import { taskPillClass } from './detail-page-helpers';
import { fetchNodeServices } from './node-api';

const PAGE = 50;

function fqnOf(n: NodeSummary): string {
  return `${n.service_name}.${n.schema_name}.${n.table_name}`;
}
function pct(v: number | null): string { return v === null ? '—' : `${v}%`; }
function durPair(avg: number | null, p95: number | null): string {
  if (avg === null && p95 === null) return '—';
  return `${formatDuration(avg)} / ${formatDuration(p95)}`;
}

// The URL parameter of the "Dormant only" filter: dormant=1 lists only the
// dormant nodes, the ones that belong to no schedule and run by hand.
const DORMANT_PARAM = 'dormant';

const dash = <span className="nodes-dash">—</span>;

// Every node of the live release, newest run first; the nodes that never ran
// come last.
export default function NodesCatalogPanel() {
  const navigate = useNavigate();
  // The Dormant only filter lives in the URL, so a link can open the list
  // filtered and the box keeps its state across the dashboard's tabs.
  const [searchParams, setSearchParams] = useSearchParams();
  const dormantOnly = searchParams.get(DORMANT_PARAM) === '1';
  const setDormantOnly = (on: boolean) => {
    const next = new URLSearchParams(searchParams);
    if (on) next.set(DORMANT_PARAM, '1');
    else next.delete(DORMANT_PARAM);
    setSearchParams(next, { replace: true });
  };
  const [operation, setOperation] = useState('run');
  const [search, setSearch] = useState('');
  const [service, setService] = useState('');
  const [nodes, setNodes] = useState<NodeSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState<string | null>(null);
  // True while the first page for the current filters is in flight, so an
  // empty table reads as loading rather than as "no nodes".
  const [loading, setLoading] = useState(true);
  const [nameOptions, setNameOptions] = useState<string[]>([]);
  const [services, setServices] = useState<string[]>([]);
  const offsetRef = useRef(0);
  const genRef = useRef(0);

  const fetchPage = useCallback(async (offset: number, append: boolean) => {
    const myGen = ++genRef.current;
    const params = new URLSearchParams();
    params.set('operation', operation);
    if (search) params.set('search', search);
    if (service) params.set('service', service);
    if (dormantOnly) params.set(DORMANT_PARAM, '1');
    params.set('limit', String(PAGE));
    params.set('offset', String(offset));
    if (!append) setLoading(true);
    try {
      const res = await fetch(`/api/nodes?${params.toString()}`);
      if (myGen !== genRef.current) return;            // superseded while awaiting fetch
      if (!res.ok) { setError('Failed to load nodes'); setLoading(false); return; }
      const data: NodesResponse = await res.json();
      if (myGen !== genRef.current) return;            // superseded while awaiting json
      setError(null);
      setTotal(data.total_count || 0);
      offsetRef.current = offset;
      setNodes(prev => (append ? [...prev, ...(data.nodes || [])] : (data.nodes || [])));
      setLoading(false);
    } catch {
      if (myGen !== genRef.current) return;
      setError('Failed to load nodes');
      setLoading(false);
    }
  }, [operation, search, service, dormantOnly]);

  // Fetch the first page on mount and on filter change (debounced for search
  // typing). The list is not polled: each call aggregates every node's recent
  // runs on the server, so its cost grows with the install's history.
  useEffect(() => {
    const id = setTimeout(() => { fetchPage(0, false); }, 250);
    return () => clearTimeout(id);
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

  // The Service filter offers every service with a node in the list, a
  // service whose nodes never ran included, independent of the loaded page
  // and of the chosen service. Fetched once on mount; a failure leaves only
  // "all".
  useEffect(() => {
    fetchNodeServices().then(setServices).catch(() => setServices([]));
  }, []);

  const canLoadMore = nodes.length < total;
  // The test operation lists only the nodes with tests, so it narrows the list
  // as the other filters do.
  const filtered = search !== '' || service !== '' || dormantOnly || operation === 'test';

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
        <label htmlFor="node-dormant" title="Nodes with no schedule tag, which run by hand">Dormant only</label>
        <input id="node-dormant" type="checkbox" checked={dormantOnly}
               onChange={e => setDormantOnly(e.target.checked)} />
      </div>

      <section className="detail-card">
        <div className="section-header">
          <div className="section-header__main">
            <span className="section-header__title">Nodes</span>
            {!(loading && nodes.length === 0) && <span className="section-header__count">{total}</span>}
          </div>
          <div className="section-header__sub">over last 50 runs</div>
        </div>

        {nodes.length === 0 && !error ? (
          loading ? (
            <div className="info-strip info-strip--neutral">
              <span>Loading nodes…</span>
            </div>
          ) : filtered ? (
            <div className="info-strip info-strip--neutral">
              <span className="info-strip__icon">ⓘ</span>
              No node in the live release matches this filter.
            </div>
          ) : (
            <div className="info-strip info-strip--neutral">
              <span className="info-strip__icon">ⓘ</span>
              No release has been promoted yet.
            </div>
          )
        ) : (
          <>
            <table className="nodes-table">
              <thead>
                <tr>
                  <th>Node</th><th>Last run</th><th>Success</th>
                  <th>Runs</th><th>Avg / p95</th>
                </tr>
              </thead>
              <tbody>
                {nodes.map(n => {
                  const go = () => navigate(`/node/${encodeURIComponent(fqnOf(n))}`,
                                            { state: { from: { type: 'nodes' }, operation } });
                  // A node that never ran has no last run and no stats.
                  const neverRan = !n.last_run_at;
                  return (
                    <tr key={fqnOf(n)} onClick={go} tabIndex={0} role="button"
                        onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); go(); } }}>
                      <td>
                        <div className="nodes-node-name">
                          {n.table_name}
                          {n.dormant && (
                            <span className="pill-sm pill-sm--muted" title={dormantTitle(n.last_run_at)}>dormant</span>
                          )}
                        </div>
                        <div className="nodes-node-schema">{n.service_name} · {n.schema_name}</div>
                      </td>
                      {neverRan ? (
                        <>
                          <td><span className="nodes-ts">never</span></td>
                          <td>{dash}</td>
                          <td>{dash}</td>
                          <td>{dash}</td>
                        </>
                      ) : (
                        <>
                          <td>
                            <span className={`pill-sm ${taskPillClass(n.last_status ?? 'pending')}`}>{n.last_status ?? '—'}</span>
                            {' '}<span className="nodes-ts nodes-ts--dim">{formatRelative(n.last_run_at)}</span>
                          </td>
                          <td>{pct(n.success_rate_pct)}</td>
                          <td>{n.task_count}</td>
                          <td><code>{durPair(n.avg_duration_sec, n.p95_duration_sec)}</code></td>
                        </>
                      )}
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
