import { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useLocation, useNavigate, useParams } from 'react-router';
import type { NodeRun, NodeRunsResponse, NodeDetailFrom } from './types';
import { kindLabel, computeNodeStats, formatDuration, formatRelative } from './node-helpers';
import NodeTypeIcon from './NodeTypeIcon';
import RunSourcePickerDialog from './RunSourcePickerDialog';

interface NodeMetaResponse {
  node_type?: string;
  test_count?: number;
  test_count_known?: boolean;
  // The CSV file a python-csv node loads; absent on every other node type.
  source_uri?: string;
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
  const navState = location.state as { from?: NodeDetailFrom; operation?: string } | null;
  const from = navState?.from;
  const navOperation = navState?.operation;
  const initialOperation = navOperation === 'test' || navOperation === 'build' || navOperation === 'run'
    ? navOperation
    : 'run';

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
  const [operation, setOperation] = useState<'run' | 'test' | 'build'>(initialOperation);
  const [testCount, setTestCount] = useState<{ count: number; known: boolean } | null>(null);
  const [nodeType, setNodeType] = useState('');
  const [sourceUri, setSourceUri] = useState('');
  const genRef = useRef(0);

  const parts = (fqn ?? '').split('.');
  const service = parts[0] ?? '';
  const schema  = parts[1] ?? '';
  const table   = parts.slice(2).join('.');

  const fetchRuns = useCallback(() => {
    if (!service || !schema || !table) return;
    const myGen = ++genRef.current;
    fetch(`/api/nodes/${encodeURIComponent(service)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/runs?operation=${encodeURIComponent(operation)}`)
      .then(r => r.json())
      .then((data: NodeRunsResponse) => {
        if (myGen !== genRef.current) return;
        setRuns(data.runs || []);
      })
      .catch(() => {
        if (myGen !== genRef.current) return;
        setRuns([]);
      });
  }, [service, schema, table, operation]);

  useEffect(() => { fetchRuns(); }, [fetchRuns]);

  useEffect(() => {
    if (!service || !schema || !table) return;
    fetch(`/api/nodes/${encodeURIComponent(service)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/meta`)
      .then(r => (r.ok ? r.json() : null))
      .then((m: NodeMetaResponse | null) => {
        setTestCount(m && typeof m.test_count === 'number'
          ? { count: m.test_count, known: Boolean(m.test_count_known) }
          : null);
        setNodeType(m?.node_type ?? '');
        setSourceUri(m?.source_uri ?? '');
      })
      .catch(() => {
        setTestCount(null);
        setNodeType('');
        setSourceUri('');
      });
  }, [service, schema, table]);

  // latestHasNoTests reflects the LATEST topology's test_count only. It gates the
  // latest-mode trigger (which the backend evaluates against latest metadata) but
  // NOT the old-snapshot path: a snapshot_of_run test is validated against the
  // source run's own pinned test_count, which this page does not know, so testing
  // an older version stays available and the backend's no_tests check is the
  // backstop there. Only a KNOWN, positive test_count is runnable: a known zero
  // and an unset count (a node predating test_count capture, so test_count_known
  // is false) are both treated as "no tests" and gate the latest-mode trigger,
  // matching the orchestrator's single-node gate.
  const latestHasNoTests = testCount !== null && !(testCount.known && testCount.count > 0);

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

  // The latest-mode test trigger is the only one gated by latest metadata; the
  // old-snapshot trigger is never blocked here (see latestHasNoTests).
  const latestTestBlocked = operation === 'test' && latestHasNoTests;

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
        <div className="detail-title-block">
          <div className="detail-scheduler-name detail-node-title">
            <NodeTypeIcon nodeType={nodeType} size={15} />
            {fqn}
            {nodeType && (
              <span className="info-strip info-strip--neutral info-strip--inline">{nodeType}</span>
            )}
          </div>
          {nodeType === 'python-csv' && sourceUri && (
            <div className="detail-node-source">source: {sourceUri}</div>
          )}
        </div>
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
            <option value="test">Test</option>
            <option value="build">Build</option>
          </select>
        </div>
        <button
          type="button"
          className={runLatestClass}
          disabled={runState === 'loading' || runState === 'success' || latestTestBlocked}
          onClick={handleRunLatest}
          title={latestTestBlocked
            ? 'The latest version of this node has no tests'
            : 'Run only this node against the latest topology'}
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

      {latestTestBlocked && (
        <div className="info-strip info-strip--info">
          The latest version of this node has no tests. Run with an old snapshot to test a version that did, or choose another operation.
        </div>
      )}

      {operation === 'build' && (
        <div className="info-strip info-strip--info">
          <span className="info-strip__icon">ℹ</span>
          Build runs <code>dbt build</code> — it materializes each model and runs its
          attached tests together in a single execution (one pod), in dependency
          order. These stats combine model + tests, not either on its own.
        </div>
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
