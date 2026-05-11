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

  return (
    <div className="node-detail-page">
      {pickerOpen && createPortal(
        <RunSourcePickerDialog
          runs={runs}
          onPick={handlePickSource}
          onClose={() => setPickerOpen(false)}
        />,
        document.body,
      )}

      <div className="detail-topbar">
        <button className="detail-back-link" onClick={() => navigate(`/schedule/${name}`)}>
          ← Back to {name}
        </button>
        <div className="detail-scheduler-name">{fqn}</div>
        <div className="node-detail-actions">
          <button
            type="button"
            className="rerun-btn rerun-btn--latest"
            disabled={runState === 'loading'}
            onClick={handleRunLatest}
            title="Run only this node against the latest topology"
          >
            {runState === 'loading' ? 'Triggering…' : '▶ Run this node (latest)'}
          </button>
          <button
            type="button"
            className="rerun-btn rerun-btn--stale"
            disabled={runState === 'loading'}
            onClick={() => setPickerOpen(true)}
            title="Run this node with the (image_tag, manifest_version) pair from a past run"
          >
            ⏱ Run with old snapshot…
          </button>
        </div>
        {runState === 'success' && (
          <span className="rerun-feedback rerun-feedback--success">✓ Run triggered</span>
        )}
        {runState === 'error' && runError && (
          <span className="rerun-feedback rerun-feedback--error">{runError}</span>
        )}
      </div>

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
                <tr key={r.task_id}>
                  <td>{formatTime(r.created_at)}</td>
                  <td>{kindLabel(r.kind)}</td>
                  <td>
                    <span className={`pill-sm pill-sm--${r.task_status}`}>{r.task_status || '—'}</span>
                    {r.error_message && <div className="nodes-error-text">{r.error_message}</div>}
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
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}
