import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate } from 'react-router';
import {
  getScheduleProgressLabel,
  getScheduleProgressPercent,
} from './scheduler-card-helpers';
import { getDriftState, getDriftBadge } from './drift-helpers';
import { ScheduleSummary, Task } from './types';
import CancelDialog from './CancelDialog';
import { fetchAllPages } from './fetch-all-pages';

function formatTime(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleTimeString();
}

function cardBorderClass(status: string): string {
  if (status === 'running' || status === 'succeeded') return 'card-border-green';
  if (status === 'failed') return 'card-border-red';
  return 'card-border-grey';
}

interface Props {
  schedule: ScheduleSummary;
}

export default function SchedulerCard({ schedule }: Props) {
  const navigate = useNavigate();
  const neverRun = !schedule.last_run_id;
  const [tasks, setTasks] = useState<Task[]>([]);
  const [drift, setDrift] = useState<{ run: number; latest: number } | null>(null);
  const [triggerLoading, setTriggerLoading] = useState(false);
  const [triggerStatus, setTriggerStatus] = useState<'idle' | 'success'>('idle');
  const [triggerError, setTriggerError] = useState<string | null>(null);
  const [operation, setOperation] = useState<'run' | 'test' | 'build'>('run');
  const [cancelDialogOpen, setCancelDialogOpen] = useState(false);

  useEffect(() => {
    if (neverRun) return;
    let cancelled = false;
    const controller = new AbortController();
    const fetch_ = () =>
      fetchAllPages<Task>(`/api/schedulers/${schedule.last_run_id}/tasks`, 'tasks', {
        signal: controller.signal,
      })
        .then(all => { if (!cancelled) setTasks(all); })
        .catch(() => {});
    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => { cancelled = true; controller.abort(); clearInterval(id); };
  }, [schedule.last_run_id]);

  useEffect(() => {
    if (!schedule.last_run_id) {
      setDrift(null);
      return;
    }
    let cancelled = false;
    const fetch_ = () =>
      fetch(`/api/runs/${schedule.last_run_id}/graph`)
        .then(r => r.json())
        .then(data => {
          if (cancelled) return;
          setDrift({
            run: Number(data.run_topology_generation ?? 0),
            latest: Number(data.latest_topology_generation ?? 0),
          });
        })
        .catch(() => {});
    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => { cancelled = true; clearInterval(id); };
  }, [schedule.last_run_id]);

  useEffect(() => {
    if (!schedule.is_running) setCancelDialogOpen(false);
  }, [schedule.is_running]);

  const displayStatus = neverRun
    ? 'never run'
    : schedule.is_running
    ? 'running'
    : schedule.last_run_status;

  // A schedule is "active" when its name matches an entry in schedules.yaml:
  // state fills cron_expression only for those, so a non-empty cron is the
  // exact signal. Inactive schedules exist purely as node tags and never fire
  // on a cron — they run only when triggered manually.
  const isActive = schedule.cron_expression.trim() !== '';

  const total = tasks.length;
  const succeeded = tasks.filter(t => t.status === 'succeeded').length;
  const failed = tasks.filter(t => t.status === 'failed').length;
  const pending = tasks.filter(t => t.status === 'pending').length;
  const running = tasks.filter(t => t.status === 'running').length;
  const pct = getScheduleProgressPercent(tasks);

  const driftState = drift ? getDriftState(drift.run, drift.latest) : 'fresh';
  const showDriftStrip = driftState !== 'fresh';
  const driftBadge = showDriftStrip && drift
    ? getDriftBadge(driftState as 'stale' | 'unknown', drift.run, drift.latest)
    : null;

  const handleClick = () =>
    navigate(`/schedule/${schedule.schedule_name}`, {
      state: { last_run_id: schedule.last_run_id },
    });

  const handleTrigger = (e: React.MouseEvent) => {
    e.stopPropagation();
    setTriggerLoading(true);
    setTriggerError(null);
    fetch(`/api/schedules/${encodeURIComponent(schedule.schedule_name)}/trigger`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ operation: operation === 'run' ? '' : operation }),
    })
      .then(async r => {
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          throw new Error(body.error || `HTTP ${r.status}`);
        }
        setTriggerStatus('success');
        setTimeout(() => setTriggerStatus('idle'), 3000);
      })
      .catch(err => setTriggerError(err.message))
      .finally(() => setTriggerLoading(false));
  };

  const handleCancelClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    setCancelDialogOpen(true);
  };

  const triggerIdleLabel =
    operation === 'test' ? 'Trigger test' : operation === 'build' ? 'Trigger build' : 'Trigger run';

  return (
    <>
      {cancelDialogOpen && createPortal(
        <CancelDialog
          scheduleName={schedule.schedule_name}
          onClose={() => setCancelDialogOpen(false)}
        />,
        document.body,
      )}
      <div
        className={`scheduler-card ${cardBorderClass(displayStatus)}`}
        onClick={handleClick}
        role="button"
        tabIndex={0}
        // Navigate only when the card itself is focused. Restricting to
        // currentTarget keeps Enter on a nested control (the operation select,
        // the Trigger/Cancel buttons) from bubbling up and navigating away.
        onKeyDown={e => { if (e.key === 'Enter' && e.target === e.currentTarget) handleClick(); }}
      >
        <div className="scheduler-card-header">
          <span className="schedule-name">{schedule.schedule_name}</span>
          <span className={`status-badge status-${displayStatus.replace(' ', '-')}`}>
            {displayStatus}
          </span>
          <span
            className={`activity-badge activity-badge--${isActive ? 'active' : 'inactive'}`}
            title={isActive
              ? 'Scheduled in schedules.yaml — fires automatically on its cron'
              : 'Not in schedules.yaml — runs only when triggered manually'}
          >
            {isActive ? 'Active' : 'Inactive'}
          </span>
          {isActive && (
            <span className="schedule-cron" title="Cron schedule (from schedules.yaml)">
              {schedule.cron_expression}
              {schedule.timezone ? ` · ${schedule.timezone}` : ''}
            </span>
          )}
          {!neverRun && (
            <span className="timestamps">
              {schedule.last_run_at && <>last run {formatTime(schedule.last_run_at)}</>}
            </span>
          )}
          <div className="form-field" onClick={e => e.stopPropagation()}>
            <label htmlFor={`op-${schedule.schedule_name}`}>Operation</label>
            <select
              id={`op-${schedule.schedule_name}`}
              value={operation}
              disabled={schedule.is_running || triggerLoading}
              onChange={e => setOperation(e.target.value as 'run' | 'test' | 'build')}
            >
              <option value="run">Run</option>
              <option value="test">Test</option>
              <option value="build">Build</option>
            </select>
          </div>
          <button
            type="button"
            className={[
              'btn',
              'btn--secondary',
              triggerLoading ? 'is-loading' : '',
              triggerStatus === 'success' ? 'is-success' : '',
            ].filter(Boolean).join(' ')}
            disabled={schedule.is_running || triggerLoading || triggerStatus === 'success'}
            onClick={handleTrigger}
            title={schedule.is_running ? 'A run is already active' : 'Trigger a full DAG run'}
          >
            {triggerLoading ? 'Triggering…' : triggerStatus === 'success' ? 'Triggered' : triggerIdleLabel}
          </button>
          {schedule.is_running && (
            <button
              type="button"
              className="btn btn--danger"
              onClick={handleCancelClick}
              title="Cancel the active run"
            >
              Cancel
            </button>
          )}
        </div>
        {showDriftStrip && driftBadge && (
          <div
            className={`info-strip ${driftState === 'unknown' ? 'info-strip--neutral' : 'info-strip--warning'}`}
            onClick={e => e.stopPropagation()}
          >
            <span aria-hidden="true">{driftState === 'unknown' ? '?' : '⚠'}</span>
            <span>{driftBadge}</span>
          </div>
        )}
        {triggerError && <div className="info-strip info-strip--error">{triggerError}</div>}
        {!neverRun && total > 0 && (
          <div className="scheduler-card-body">
            <div className="progress-row">
              <span className="progress-label">{getScheduleProgressLabel(tasks)}</span>
              <div className="progress-bar-track">
                <div className="progress-bar-fill" style={{ width: `${pct}%` }} />
              </div>
              <span className="progress-pct">{pct}%</span>
            </div>
            <div className="summary-row">
              <span><span aria-hidden="true">✅</span> {succeeded} succeeded</span>
              <span><span aria-hidden="true">❌</span> {failed} failed</span>
              <span><span aria-hidden="true">⏳</span> {pending} pending</span>
              <span><span aria-hidden="true">🏃</span> {running} running</span>
            </div>
          </div>
        )}
      </div>
    </>
  );
}
