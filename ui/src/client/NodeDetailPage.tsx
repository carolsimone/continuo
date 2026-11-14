import { useCallback, useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useLocation, useNavigate, useParams } from 'react-router';
import PageHeader from './PageHeader';
import { useSingleNodeRun } from './useSingleNodeRun';
import type { NodeOutage, NodeRun, NodeRunsResponse, NodeDetailFrom, NodesResponse } from './types';
import { kindLabel, computeNodeStats, dormantTitle, formatDuration, formatRelative, formatTimeout } from './node-helpers';
import { isTerminalStatus, taskPillClass, taskStatusTitle } from './detail-page-helpers';
import NodeTypeIcon from './NodeTypeIcon';
import RunSourcePickerDialog from './RunSourcePickerDialog';
import FullRefreshConfirmDialog from './FullRefreshConfirmDialog';
import { type NodeType, type RunStatus, NODE_TYPES_SUPPORTING_FULL_REFRESH } from '../server/generated/vocabulary.gen';
import { isNodeOperation, type NodeOperation } from '../server/shared/operation';

const PYTHON_CSV: NodeType = 'python-csv';
const DBT_SEED: NodeType = 'dbt-seed';

// Node types a full refresh can rebuild, as the contract declares them.
const FULL_REFRESH_NODE_TYPES: ReadonlySet<string> = new Set<string>(NODE_TYPES_SUPPORTING_FULL_REFRESH);

interface NodeMetaResponse {
  node_type?: string;
  test_count?: number;
  test_count_known?: boolean;
  // The CSV file a python-csv node loads; absent on every other node type.
  source_uri?: string;
  // True when the node is no longer active in the latest topology.
  inactive?: boolean;
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
        <span className={`pill-sm ${taskPillClass(r.task_status)}`} title={taskStatusTitle(r.task_status)}>{r.task_status || '—'}</span>
        {(r.failure_class || r.failure_reason) && (
          <div className="nodes-reason">{[r.failure_class, r.failure_reason].filter(Boolean).join(' · ')}</div>
        )}
        {r.operation === 'full_refresh' && <span className="pill-sm pill-sm--muted">Full refresh</span>}
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
      <td>{r.attempt + 1}</td>
      <td>{formatDuration(durationSec(r))}</td>
      <td><code>{r.image_tag || '—'}</code></td>
      <td>
        {r.log_s3_key
          ? <a
               href={`/api/attempts/${r.task_id}/logs?key=${encodeURIComponent(r.log_s3_key)}`}
               target="_blank" rel="noopener noreferrer">logs</a>
          : <span className="nodes-dash">—</span>}
      </td>
    </tr>
  );
}

const HISTORY_POLL_INTERVAL_MS = 5000;

export default function NodeDetailPage() {
  const { fqn } = useParams<{ fqn: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const navState = location.state as { from?: NodeDetailFrom; operation?: string } | null;
  const from = navState?.from;
  const navOperation = navState?.operation;
  const initialOperation: NodeOperation = isNodeOperation(navOperation) ? navOperation : 'run';

  let backLabel = '← Back to Nodes';
  let backPath = '/?tab=nodes';
  if (from?.type === 'schedule') {
    backLabel = `← Back to ${from.name}`;
    backPath = from.mode === 'latest' ? `/schedule/${from.name}/latest` : `/schedule/${from.name}`;
  }
  const [runs, setRuns] = useState<NodeRun[]>([]);
  // Whether runs holds the answer of the latest history read: false while a
  // read is in flight and after one failed.
  const [runsKnown, setRunsKnown] = useState(false);
  // The outage of the node's production object the latest history read
  // reported, null when it has none.
  const [outage, setOutage] = useState<NodeOutage | null>(null);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [operation, setOperation] = useState<NodeOperation>(initialOperation);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [pendingSource, setPendingSource] = useState<string | null>(null);
  const [testCount, setTestCount] = useState<{ count: number; known: boolean } | null>(null);
  const [nodeType, setNodeType] = useState('');
  const [sourceUri, setSourceUri] = useState('');
  const [inactive, setInactive] = useState(false);
  // The task timeout the node's tasks get and whether the live release
  // declares it (else it is the install default); null while unknown or when
  // the node has no node-list row.
  const [taskTimeout, setTaskTimeout] = useState<{ seconds: number; declared: boolean } | null>(null);
  // Whether the node is dormant in the live release (it belongs to no
  // schedule), and when it last ran; null while unknown or when the node has no
  // node-list row.
  const [dormant, setDormant] = useState<{ lastRunAt: string | null } | null>(null);
  const genRef = useRef(0);

  const parts = (fqn ?? '').split('.');
  const service = parts[0] ?? '';
  const schema  = parts[1] ?? '';
  const table   = parts.slice(2).join('.');

  // The history route filters on run | test | build; a full refresh is a run
  // of the node, so its history is queried on the run dimension.
  const historyOperation = operation === 'full_refresh' ? 'run' : operation;

  // A poll leaves what the page holds in place until its answer lands, and when
  // the read fails, so the next poll can try again.
  const fetchRuns = useCallback((poll = false) => {
    if (!service || !schema || !table) return;
    const myGen = ++genRef.current;
    if (!poll) setRunsKnown(false);
    fetch(`/api/nodes/${encodeURIComponent(service)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/runs?operation=${encodeURIComponent(historyOperation)}`)
      .then(r => r.json())
      .then((data: NodeRunsResponse) => {
        if (myGen !== genRef.current) return;
        setRuns(data.tasks || []);
        setRunsKnown(Array.isArray(data.tasks));
        setOutage(data.outage ?? null);
      })
      .catch(() => {
        if (poll || myGen !== genRef.current) return;
        setRuns([]);
        setRunsKnown(false);
        setOutage(null);
      });
  }, [service, schema, table, historyOperation]);

  useEffect(() => { fetchRuns(); }, [fetchRuns]);

  // The history is read again every 5 s while a row has not ended.
  const historyInFlight = runs.some(r => !isTerminalStatus(r.task_status as RunStatus));
  useEffect(() => {
    if (!historyInFlight) return;
    const timer = window.setInterval(() => fetchRuns(true), HISTORY_POLL_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [historyInFlight, fetchRuns]);

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
        setInactive(Boolean(m?.inactive));
      })
      .catch(() => {
        setTestCount(null);
        setNodeType('');
        setSourceUri('');
        setInactive(false);
      });
  }, [service, schema, table]);

  // The node's row of the node list: its task timeout and whether it is
  // dormant, with its last run. The search filters on the table name only (an
  // exact match within the service), so rows of other schemas sharing the table
  // name can come back: the schema is matched client-side.
  //
  // Each read takes the next number, and an answer replaces what the page holds
  // only when its number is above that of every answer already applied, so an
  // older read never lands over a newer one. A read that fails applies nothing
  // and does not hold back an older read still in flight. A change of node
  // counts every read started so far as applied: the answers still in flight
  // for the node left land nowhere.
  const summaryStartedRef = useRef(0);
  const summaryAppliedRef = useRef(0);
  const fetchNodeSummary = useCallback(() => {
    if (!service || !schema || !table) return;
    const myRead = ++summaryStartedRef.current;
    fetch(`/api/nodes?service=${encodeURIComponent(service)}&search=${encodeURIComponent(table)}&operation=run`)
      .then(r => {
        if (!r.ok) throw new Error(`node list answered ${r.status}`);
        return r.json();
      })
      .then((data: NodesResponse) => {
        const row = data.nodes.find(n => n.schema_name === schema);
        if (myRead <= summaryAppliedRef.current) return;
        summaryAppliedRef.current = myRead;
        setTaskTimeout(row && typeof row.timeout_seconds === 'number' && row.timeout_seconds > 0
          ? { seconds: row.timeout_seconds, declared: !!row.timeout_declared }
          : null);
        setDormant(row?.dormant ? { lastRunAt: row.last_run_at ?? null } : null);
      })
      .catch(() => { /* the page keeps what it holds */ });
  }, [service, schema, table]);

  // A new node starts with nothing known about it.
  useEffect(() => {
    setTaskTimeout(null);
    setDormant(null);
    fetchNodeSummary();
    return () => { summaryAppliedRef.current = summaryStartedRef.current; };
  }, [fetchNodeSummary]);

  const canFullRefresh = FULL_REFRESH_NODE_TYPES.has(nodeType);

  // A full refresh selected via navigation state falls back to Run once the
  // node's type is known and does not support it.
  useEffect(() => {
    if (operation === 'full_refresh' && nodeType !== '' && !canFullRefresh) setOperation('run');
  }, [operation, nodeType, canFullRefresh]);

  // latestHasNoTests reflects the LATEST topology's test_count only. It gates the
  // latest-mode trigger (which the backend evaluates against latest metadata) but
  // NOT the old-snapshot path: a snapshot_of_run test is validated against the
  // source run's own pinned test_count, which this page does not know, so testing
  // an older version stays available and the backend's no_tests check is the
  // backstop there. Only a KNOWN, positive test_count is runnable: a known zero
  // and an unset count (a node predating test_count capture, so test_count_known
  // is false) are both treated as "no tests" and gate the latest-mode trigger,
  // matching run-controller's single-node gate.
  const latestHasNoTests = testCount !== null && !(testCount.known && testCount.count > 0);

  // The newest run in the history that a press of this page could have made:
  // a single-node run. A scheduled run of the node is not the outcome of a
  // press, so it does not end the wait for one. While the history is not
  // known (a read in flight or failed) the page observes nothing; a history
  // with no single-node run in its window observes none.
  const newestNodeRunId = runsKnown ? (runs.find(r => r.kind === 'single_node_run')?.run_id ?? null) : undefined;

  // A started run reads the history again, and the node's row of the node list,
  // whose last run a run or a full refresh moves.
  const handleStarted = useCallback(() => {
    fetchRuns();
    fetchNodeSummary();
  }, [fetchRuns, fetchNodeSummary]);

  // A press is one action while the history shows no node run newer than the
  // one the press saw.
  const { runState, runError, postRun } = useSingleNodeRun(
    { service, schema, table },
    { operation, observed: newestNodeRunId, onStarted: handleStarted },
  );

  // A full refresh drops the node's table, so it is only posted once confirmed.
  const handleRunLatest = useCallback(() => {
    if (operation === 'full_refresh') { setPendingSource(null); setConfirmOpen(true); return; }
    postRun({});
  }, [postRun, operation]);
  const handlePickSource = useCallback((runId: string) => {
    setPickerOpen(false);
    if (operation === 'full_refresh') { setPendingSource(runId); setConfirmOpen(true); return; }
    postRun({ source_run_id: runId });
  }, [postRun, operation]);
  const handleConfirmFullRefresh = useCallback((acknowledged: string[]) => {
    setConfirmOpen(false);
    postRun({
      ...(pendingSource ? { source_run_id: pendingSource } : {}),
      ...(acknowledged.length > 0 ? { acknowledged_dependents: acknowledged } : {}),
    }, 'full_refresh');
  }, [postRun, pendingSource]);

  const pendingRun = pendingSource ? runs.find(r => r.run_id === pendingSource) : undefined;
  const snapshotLine = pendingRun
    ? `Rebuilds from the snapshot of the run on ${formatTime(pendingRun.created_at)} (image ${pendingRun.image_tag}).`
    : undefined;

  const stats = computeNodeStats(runs);

  const runLatestClass = ['btn', 'btn--secondary', runState === 'loading' ? 'is-loading' : '', runState === 'success' ? 'is-success' : ''].filter(Boolean).join(' ');

  const runVerb =
    operation === 'test' ? 'Test this node'
    : operation === 'build' ? 'Build this node'
    : operation === 'full_refresh' ? 'Full refresh this node'
    : 'Run this node';

  // A node no longer active in the latest topology has no latest version to
  // run, so every latest-mode operation is unavailable for it; only the
  // old-snapshot trigger remains.
  const latestUnavailable = inactive;

  // The latest-mode test trigger is also gated by latest test metadata; the
  // old-snapshot trigger is never blocked here (see latestHasNoTests). For an
  // inactive node the inactive strip already explains why latest is unavailable.
  const latestTestBlocked = operation === 'test' && latestHasNoTests && !inactive;

  // Until the node's type is known a full refresh cannot be validated, so both
  // triggers stay disabled while it is selected.
  const fullRefreshUnavailable = operation === 'full_refresh' && !canFullRefresh;

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

      {confirmOpen && createPortal(
        <FullRefreshConfirmDialog
          fqn={fqn ?? ''}
          snapshot={snapshotLine}
          seed={nodeType === DBT_SEED}
          objects={runs[0]?.blocking_objects}
          onConfirm={handleConfirmFullRefresh}
          onClose={() => setConfirmOpen(false)}
        />,
        document.body,
      )}

      <PageHeader>
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
            {dormant !== null && (
              <span className="info-strip info-strip--neutral info-strip--inline" title={dormantTitle(dormant.lastRunAt)}>
                dormant
              </span>
            )}
            {taskTimeout !== null && (
              <span className="info-strip info-strip--neutral info-strip--inline">
                {`task timeout ${formatTimeout(taskTimeout.seconds)} · ${taskTimeout.declared ? 'declared' : 'install default'}`}
              </span>
            )}
          </div>
          {nodeType === PYTHON_CSV && sourceUri && (
            <div className="detail-node-source">source: {sourceUri}</div>
          )}
        </div>
      </PageHeader>

      <div className="page-action-row">
        <div className="form-field">
          <label htmlFor="node-operation">Operation</label>
          <select
            id="node-operation"
            value={operation}
            disabled={runState === 'loading'}
            onChange={e => setOperation(e.target.value as NodeOperation)}
          >
            <option value="run">Run</option>
            <option value="test">Test</option>
            <option value="build">Build</option>
            {canFullRefresh && <option value="full_refresh">Full refresh</option>}
          </select>
        </div>
        <button
          type="button"
          className={runLatestClass}
          disabled={runState === 'loading' || runState === 'success' || latestTestBlocked || fullRefreshUnavailable || latestUnavailable}
          onClick={handleRunLatest}
          title={latestTestBlocked
            ? 'The latest version of this node has no tests'
            : latestUnavailable
              ? 'This node is no longer active in the topology'
              : 'Run only this node against the latest topology'}
        >
          {runState === 'loading' ? 'Triggering…' : runState === 'success' ? 'Triggered' : runVerb}
        </button>
        <button
          type="button"
          className="btn btn--secondary"
          disabled={runState === 'loading' || fullRefreshUnavailable}
          onClick={() => setPickerOpen(true)}
          title="Run this node with the image_tag from a past run"
        >
          Run with old snapshot…
        </button>
      </div>

      {latestTestBlocked && (
        <div className="info-strip info-strip--info">
          The latest version of this node has no tests. Run with an old snapshot to test a version that did, or choose another operation.
        </div>
      )}

      {latestUnavailable && (
        <div className="info-strip info-strip--info">
          <span className="info-strip__icon">ℹ</span>
          This node is no longer active in the topology, so it cannot run from its latest version.
          Use “Run with old snapshot…” to run a version it had.
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

      {operation === 'full_refresh' && (
        <div className="info-strip info-strip--info">
          <span className="info-strip__icon">ℹ</span>
          Full refresh runs this node alone with <code>--full-refresh</code>: its table is dropped
          and rebuilt from scratch. History below shows this node's runs, full refreshes included.
        </div>
      )}

      {runState === 'error' && runError && (
        <div className="info-strip info-strip--error">{runError}</div>
      )}

      {outage && (
        <div className={`info-strip ${outage.state === 'missing' ? 'info-strip--error' : 'info-strip--warning'}`}>
          <span className="info-strip__icon">⚠</span>
          {outage.state === 'missing' ? 'Missing in production' : 'Stale'}
          {outage.since ? ` since ${formatTime(outage.since)}` : ''}, after the load of <code>{outage.seed_node_id}</code>
        </div>
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
