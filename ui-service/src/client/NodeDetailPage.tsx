import { useCallback, useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate, useParams } from 'react-router-dom';
import type { NodeRun, NodeRunsResponse } from './types';
import { kindLabel, computeNodeStats, formatDuration } from './node-helpers';
import RunSourcePickerDialog from './RunSourcePickerDialog';

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
          ? <a className="nodes-log-link"
               href={`/api/task-execution/${r.task_id}/logs?key=${encodeURIComponent(r.log_s3_key)}`}
               target="_blank" rel="noopener noreferrer">logs</a>
          : <span className="nodes-dash">—</span>}
      </td>
    </tr>
  );
}

export default function NodeDetailPage() {
  const { name, fqn } = useParams<{ name: string; fqn: string }>();
  const navigate = useNavigate();
  const [runs, setRuns] = useState<NodeRun[]>([]);
  const [runState, setRunState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [runError, setRunError] = useState<string | null>(null);
  const [pickerOpen, setPickerOpen] = useState(false);

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

  const postRun = useCallback(async (body: object) => {
    setRunState('loading');
    setRunError(null);
    try {
      const res = await fetch(
        `/api/nodes/${encodeURIComponent(service)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/run`,
        { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) },
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
  }, [service, schema, table, fetchRuns]);

  const handleRunLatest  = useCallback(() => postRun({}), [postRun]);
  const handlePickSource = useCallback((runId: string) => {
    setPickerOpen(false);
    postRun({ source_run_id: runId });
  }, [postRun]);

  const stats = computeNodeStats(runs);

  const runLatestClass = ['btn', 'btn--secondary', runState === 'loading' ? 'is-loading' : '', runState === 'success' ? 'is-success' : ''].filter(Boolean).join(' ');

  return (
    <div className="page">
      {pickerOpen && createPortal(
        <RunSourcePickerDialog
          runs={runs}
          onPick={handlePickSource}
          onClose={() => setPickerOpen(false)}
        />,
        document.body,
      )}

      <header className="page-header">
        <button className="detail-back-link" onClick={() => navigate(`/schedule/${name}`)}>
          ← Back to {name}
        </button>
        <div className="detail-scheduler-name">{fqn}</div>
      </header>

      <div className="page-action-row">
        <button
          type="button"
          className={runLatestClass}
          disabled={runState === 'loading' || runState === 'success'}
          onClick={handleRunLatest}
          title="Run only this node against the latest topology"
        >
          {runState === 'loading' ? 'Triggering…' : runState === 'success' ? 'Triggered' : '▶ Run this node'}
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

      {runState === 'error' && runError && (
        <div className="info-strip info-strip--error">{runError}</div>
      )}

      <main className="page-content">
        <section className="detail-card">
          <div className="detail-card-header">
            Node history
            <span className="node-stats-summary">
              {stats.successRatePct !== null
                ? `${stats.successRatePct}% succeeded`
                : 'no terminal runs'}
              {' · '}
              avg {formatDuration(stats.avgDurationSec)}
              {' · '}
              last {stats.total} runs
            </span>
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
