import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate } from 'react-router';
import {
  getScheduleProgressLabel,
  getScheduleProgressPercent,
} from './scheduler-card-helpers';
import { getDriftState, getDriftBadge } from './drift-helpers';
import { ScheduleSummary, Task } from './types';
import type { RunOperation } from '../server/shared/operation';
import CancelDialog from './CancelDialog';
import { fetchAllPages } from './fetch-all-pages';
import { settlesRequest, useActionRequestId } from './request-id';

function formatTime(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleTimeString();
}

function cardBorderClass(status: string): string {
  if (status === 'running' || status === 'cancelling' || status === 'succeeded') return 'card-border-green';
  if (status === 'failed') return 'card-border-red';
  return 'card-border-grey';
}

interface Props {
  schedule: ScheduleSummary;
  // Reads the schedule list again at once. The card calls it after a trigger
  // or a cancel succeeds, so the card shows the new run or the cancelling run
  // without waiting for the next poll.
  onRunChanged: () => void;
}

export default function SchedulerCard({ schedule, onRunChanged }: Props) {
  const navigate = useNavigate();
  const neverRun = !schedule.last_run_id;
  const [tasks, setTasks] = useState<Task[]>([]);
  const [tasksReload, setTasksReload] = useState(0);
  const [drift, setDrift] = useState<{ run: number; latest: number } | null>(null);
  const [triggerLoading, setTriggerLoading] = useState(false);
  const [triggerStatus, setTriggerStatus] = useState<'idle' | 'success'>('idle');
  const [triggerError, setTriggerError] = useState<string | null>(null);
  const [operation, setOperation] = useState<RunOperation>('run');
  const [cancelDialogOpen, setCancelDialogOpen] = useState(false);
  // The trigger button's request id: a press that gets no answer is repeated
  // with the same id, so the run the first press made is returned rather than
  // made twice. The id is renewed once the server has given its final answer
  // and when the operation changes, since another operation is another request.
  const { id: triggerId, renew: renewTriggerId } = useActionRequestId();

  // Reads the run's tasks now and every 5 s after. A change of run or a bump
  // of tasksReload starts the reads over; the reads already in flight are
  // aborted and their answers dropped, so an answer from before a cancel never
  // replaces one from after it.
  useEffect(() => {
    if (neverRun) return;
    let cancelled = false;
    const controller = new AbortController();
    const fetch_ = () =>
      fetchAllPages<Task>(`/api/runs/${schedule.last_run_id}/tasks`, 'tasks', {
        signal: controller.signal,
      })
        .then(all => { if (!cancelled) setTasks(all); })
        .catch(() => {});
    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => { cancelled = true; controller.abort(); clearInterval(id); };
  }, [schedule.last_run_id, tasksReload]);

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
            run: Number(data.run_promotion_seq ?? 0),
            latest: Number(data.latest_promotion_seq ?? 0),
          });
        })
        .catch(() => {});
    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => { cancelled = true; clearInterval(id); };
  }, [schedule.last_run_id]);

  useEffect(() => {
    if (!schedule.has_active_run) setCancelDialogOpen(false);
  }, [schedule.has_active_run]);

  // A cancelling run is active (has_active_run) but not running: it reads
  // cancelling, and its Cancel button stays available.
  const displayStatus = neverRun
    ? 'never run'
    : schedule.has_active_run
    ? (schedule.last_run_status === 'cancelling' ? 'cancelling' : 'running')
    : schedule.last_run_status;

  // A schedule is "active" when its name matches an entry in schedules.yaml:
  // state fills cron_expression only for those, so a non-empty cron is the
  // exact signal. Inactive schedules exist purely as node tags and never fire
  // on a cron — they run only when triggered manually.
  const isActive = schedule.cron_expression.trim() !== '';

  const total = tasks.length;
  const succeeded = tasks.filter(t => t.status === 'succeeded').length;
  const failed = tasks.filter(t => t.status === 'failed').length;
  const skipped = tasks.filter(t => t.status === 'skipped').length;
  const cancelledTasks = tasks.filter(t => t.status === 'cancelled').length;
  const dispatched = tasks.filter(t => t.status === 'dispatched').length;
  const queued = tasks.filter(t => t.status === 'queued').length;
  const stopping = tasks.filter(t => t.status === 'stopping').length;
  const running = tasks.filter(t => t.status === 'running').length;
  const pending = tasks.filter(t => t.status === 'pending').length;
  const pct = getScheduleProgressPercent(tasks);

  const driftState = drift ? getDriftState(drift.run, drift.latest) : 'fresh';
  // Only a stale run (pinned to an older topology than the latest) warrants a
  // strip. The 'unknown' state (a run predating topology tracking) is silent.
  const showDriftStrip = driftState === 'stale';
  const driftBadge = showDriftStrip && drift
    ? getDriftBadge('stale', drift.run, drift.latest)
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
      body: JSON.stringify({ request_id: triggerId, operation: operation === 'run' ? '' : operation }),
    })
      .then(async r => {
        if (settlesRequest(r)) renewTriggerId();
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          throw new Error(body.error || `HTTP ${r.status}`);
        }
        setTriggerStatus('success');
        setTimeout(() => setTriggerStatus('idle'), 3000);
        // The new run reaches the card through the schedule list, whose new
        // last_run_id starts the task reads of that run.
        onRunChanged();
      })
      .catch(err => setTriggerError(err.message))
      .finally(() => setTriggerLoading(false));
  };

  const handleCancelClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    setCancelDialogOpen(true);
  };

  // A cancel keeps the same run, so the card reads both the schedule list (the
  // run's status) and the run's tasks (now stopping or cancelled) again.
  const handleCancelled = () => {
    setTasksReload(n => n + 1);
    onRunChanged();
  };

  const triggerIdleLabel =
    operation === 'test' ? 'Trigger test' : operation === 'build' ? 'Trigger build' : 'Trigger run';

  return (
    <>
      {cancelDialogOpen && schedule.last_run_id && createPortal(
        <CancelDialog
          runId={schedule.last_run_id}
          onCancelled={handleCancelled}
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
              disabled={schedule.has_active_run || triggerLoading}
              onChange={e => { setOperation(e.target.value as RunOperation); renewTriggerId(); }}
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
            disabled={schedule.has_active_run || triggerLoading || triggerStatus === 'success'}
            onClick={handleTrigger}
            title={schedule.has_active_run ? 'A run is already active' : 'Trigger a full DAG run'}
          >
            {triggerLoading ? 'Triggering…' : triggerStatus === 'success' ? 'Triggered' : triggerIdleLabel}
          </button>
          {schedule.has_active_run && (
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
            className="info-strip info-strip--warning"
            onClick={e => e.stopPropagation()}
          >
            <span aria-hidden="true">⚠</span>
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
              <span><span aria-hidden="true">⏭️</span> {skipped} skipped</span>
              <span><span aria-hidden="true">🚫</span> {cancelledTasks} cancelled</span>
              <span><span aria-hidden="true">📤</span> {dispatched} dispatched</span>
              <span><span aria-hidden="true">🕒</span> {queued} queued</span>
              <span><span aria-hidden="true">✋</span> {stopping} stopping</span>
              <span><span aria-hidden="true">🏃</span> {running} running</span>
              <span><span aria-hidden="true">⏳</span> {pending} pending</span>
            </div>
          </div>
        )}
      </div>
    </>
  );
}
