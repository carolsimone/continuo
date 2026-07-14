import { useCallback, useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import type { NodeRun, NodeRunsResponse, NodeDetailFrom } from './types';
import { kindLabel, computeNodeStats, formatDuration, formatRelative } from './node-helpers';
import RunSourcePickerDialog from './RunSourcePickerDialog';

interface NodeMetaResponse {
  node_type?: string;
  test_count?: number;
  test_count_known?: boolean;
}

function formatTime(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

function durationSec(r: NodeRun): number | null {
  if (!r.started_at || !r.completed_at) return null;
  const ms = new Date(r.completed_at).getTime() - new Date(r.started_at).getTime();
  if (Number.isNaN(ms) || ms < 0) return null;
  return Math.round(ms / 1000);
}

function NodeRunRow({ run: r }: { run: NodeRun }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <tr key={r.task_id}>
      <td>{formatTime(r.created_at)}</td>
      <td>{kindLabel(r.kind)}</td>
      <td>
        <span className={`pill-sm pill-sm--${r.task_status}`}>{r.task_status || '—'}</span>
        {r.error_message && (
          <div className="nodes-error-text">
            <span
              className={['nodes-error-short', expanded ? 'nodes-error-short--hidden' : ''].filter(Boolean).join(' ')}
              title={r.error_message}
            >
              {r.error_message}
            </span>
            <span className={['nodes-error-full', expanded ? 'nodes-error-full--visible' : ''].filter(Boolean).join(' ')}>
              {r.error_message}
            </span>
            <button
              type="button"
              className="nodes-error-toggle"
              onClick={() => setExpanded(e => !e)}
            >
              {expanded ? 'less' : 'more'}
            </button>
          </div>
        )}
      </td>
      <td>{r.retry_count + 1}</td>
      <td>{formatDuration(durationSec(r))}</td>
      <td><code>{r.image_tag || '—'}</code></td>
      <td><code>{r.manifest_version || '—'}</code></td>
      <td>
        {r.log_s3_key
          ? <a
               href={`/api/task-execution/${r.task_id}/logs?key=${encodeURIComponent(r.log_s3_key)}`}
               target="_blank" rel="noopener noreferrer">logs</a>
          : <span className="nodes-dash">—</span>}
      </td>
    </tr>
  );
}

export default function NodeDetailPage() {
  const { fqn } = useParams<{ fqn: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const from = (location.state as { from?: NodeDetailFrom } | null)?.from;

  let backLabel = '← Back to Nodes';
  let backPath = '/?tab=nodes';
  if (from?.type === 'schedule') {
    backLabel = `← Back to ${from.name}`;
    backPath = from.mode === 'latest' ? `/schedule/${from.name}/latest` : `/schedule/${from.name}`;
  }
  const [runs, setRuns] = useState<NodeRun[]>([]);
  const [runState, setRunState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [runError, setRunError] = useState<string | null>(null);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [operation, setOperation] = useState<'run' | 'test' | 'build'>('run');
  const [testCount, setTestCount] = useState<{ count: number; known: boolean } | null>(null);

  const parts = (fqn ?? '').split('.');
  const service = parts[0] ?? '';
  const schema  = parts[1] ?? '';
  const table   = parts.slice(2).join('.');

  const fetchRuns = useCallback(() => {
    if (!service || !schema || !table) return;
    fetch(`/api/nodes/${encodeURIComponent(service)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/runs`)
      .then(r => r.json())
      .then((data: NodeRunsResponse) => setRuns(data.runs || []))
      .catch(() => setRuns([]));
  }, [service, schema, table]);

  useEffect(() => { fetchRuns(); }, [fetchRuns]);

  useEffect(() => {
    if (!service || !schema || !table) return;
    fetch(`/api/nodes/${encodeURIComponent(service)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/meta`)
      .then(r => (r.ok ? r.json() : null))
      .then((m: NodeMetaResponse | null) => setTestCount(m && typeof m.test_count === 'number'
        ? { count: m.test_count, known: Boolean(m.test_count_known) }
        : null))
      .catch(() => setTestCount(null));
  }, [service, schema, table]);

  const testDisabled = testCount !== null && testCount.known && testCount.count === 0;
  useEffect(() => {
    if (testDisabled && operation === 'test') setOperation('run');
  }, [testDisabled, operation]);

  const postRun = useCallback(async (body: object) => {
    setRunState('loading');
    setRunError(null);
    try {
      const withOp = { ...body, operation: operation === 'run' ? '' : operation };
      const res = await fetch(
        `/api/nodes/${encodeURIComponent(service)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/run`,
        { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(withOp) },
      );
      if (res.ok) {
        setRunState('success');
        fetchRuns();
        setTimeout(() => setRunState('idle'), 3000);
      } else {
        const errBody = await res.json().catch(() => ({ error: 'Request failed — please try again' }));
        setRunError(errBody.error ?? 'Request failed — please try again');
        setRunState('error');
      }
    } catch {
      setRunError('Request failed — please try again');
      setRunState('error');
    }
  }, [service, schema, table, fetchRuns, operation]);

  const handleRunLatest  = useCallback(() => postRun({}), [postRun]);
  const handlePickSource = useCallback((runId: string) => {
    setPickerOpen(false);
    postRun({ source_run_id: runId });
  }, [postRun]);

  const stats = computeNodeStats(runs);

  const runLatestClass = ['btn', 'btn--secondary', runState === 'loading' ? 'is-loading' : '', runState === 'success' ? 'is-success' : ''].filter(Boolean).join(' ');

  const runVerb =
    operation === 'test' ? '🧪 Test this node'
    : operation === 'build' ? '🔨 Build this node'
    : '▶ Run this node';

  return (
    <div className="page">
      {pickerOpen && createPortal(
        <RunSourcePickerDialog
          runs={runs}
          operation={operation}
          onPick={handlePickSource}
          onClose={() => setPickerOpen(false)}
        />,
        document.body,
      )}

      <header className="page-header">
        <button className="detail-back-link" onClick={() => navigate(backPath)}>
          {backLabel}
        </button>
        <div className="detail-scheduler-name">{fqn}</div>
      </header>

      <div className="page-action-row">
        <div className="form-field">
          <label htmlFor="node-operation">Operation</label>
          <select
            id="node-operation"
            value={operation}
            disabled={runState === 'loading'}
            onChange={e => setOperation(e.target.value as 'run' | 'test' | 'build')}
          >
            <option value="run">Run</option>
            <option value="test" disabled={testDisabled}>Test</option>
            <option value="build">Build</option>
          </select>
        </div>
        <button
          type="button"
          className={runLatestClass}
          disabled={runState === 'loading' || runState === 'success'}
          onClick={handleRunLatest}
          title="Run only this node against the latest topology"
        >
          {runState === 'loading' ? 'Triggering…' : runState === 'success' ? 'Triggered' : runVerb}
        </button>
        <button
          type="button"
          className="btn btn--secondary"
          disabled={runState === 'loading'}
          onClick={() => setPickerOpen(true)}
          title="Run this node with the (image_tag, manifest_version) pair from a past run"
        >
          ⏱ Run with old snapshot…
        </button>
      </div>

      {testDisabled && (
        <div className="info-strip info-strip--info">This node has no tests, so the test operation is unavailable.</div>
      )}

      {runState === 'error' && runError && (
        <div className="info-strip info-strip--error">{runError}</div>
      )}

      <main className="page-content">
        <section className="detail-card">
          <div className="section-header">
            <div className="section-header__main">
              <span className="section-header__title">Node history</span>
              <span className="section-header__count">{stats.total}</span>
            </div>
            <div className="section-header__sub">
              {stats.successRatePct !== null ? `${stats.successRatePct}% succeeded` : 'no terminal runs'}
              {' · '}avg {formatDuration(stats.avgDurationSec)}
              {' · '}p95 {formatDuration(stats.p95DurationSec)}
              {' · '}{stats.flakyRatePct}% flaky
              {stats.lastRunAt && <> {' · '}last {formatRelative(stats.lastRunAt)}</>}
            </div>
          </div>
          {runs.length === 0 ? (
            <p className="empty">No runs yet on this node.</p>
          ) : (
            <table className="nodes-table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Kind</th>
                  <th>Status</th>
                  <th>Attempt</th>
                  <th>Duration</th>
                  <th>Image tag</th>
                  <th>Manifest</th>
                  <th>Logs</th>
                </tr>
              </thead>
              <tbody>
                {runs.map(r => (
                  <NodeRunRow key={r.task_id} run={r} />
                ))}
              </tbody>
            </table>
          )}
        </section>
      </main>
    </div>
  );
}
