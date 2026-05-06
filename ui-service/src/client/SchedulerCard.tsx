import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate } from 'react-router-dom';
import {
  getScheduleProgressLabel,
  getScheduleProgressPercent,
} from './scheduler-card-helpers';
import { getDriftState, getDriftMessage } from './drift-helpers';
import { ScheduleSummary, Task } from './types';
import CancelDialog from './CancelDialog';

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
  latestTopologyGeneration: number;
}

export default function SchedulerCard({ schedule, latestTopologyGeneration }: Props) {
  const navigate = useNavigate();
  const neverRun = !schedule.last_run_id;
  const [tasks, setTasks] = useState<Task[]>([]);
  const [triggerLoading, setTriggerLoading] = useState(false);
  const [triggerError, setTriggerError] = useState<string | null>(null);
  const [cancelDialogOpen, setCancelDialogOpen] = useState(false);

  useEffect(() => {
    if (neverRun) return;
    const fetch_ = () =>
      fetch(`/api/schedulers/${schedule.last_run_id}/tasks`)
        .then(r => r.json())
        .then(data => setTasks(data.tasks || []))
        .catch(() => {});
    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => clearInterval(id);
  }, [schedule.last_run_id]);

  useEffect(() => {
    if (!schedule.is_running) setCancelDialogOpen(false);
  }, [schedule.is_running]);

  const displayStatus = neverRun
    ? 'never run'
    : schedule.is_running
    ? 'running'
    : schedule.last_run_status;

  const total = tasks.length;
  const succeeded = tasks.filter(t => t.status === 'succeeded').length;
  const failed = tasks.filter(t => t.status === 'failed').length;
  const pending = tasks.filter(t => t.status === 'pending').length;
  const running = tasks.filter(t => t.status === 'running').length;
  const pct = getScheduleProgressPercent(tasks);

  // Drift only matters while a run is in flight. Once finalised
  // (active_run_id === null) the strip disappears even if there's drift.
  const driftState =
    schedule.active_run_id !== null
      ? getDriftState(schedule.active_run_topology_generation, latestTopologyGeneration)
      : 'fresh';
  const showDriftStrip = driftState !== 'fresh';
  const driftMsg = showDriftStrip
    ? getDriftMessage(
        driftState,
        Number(schedule.active_run_topology_generation ?? 0),
        latestTopologyGeneration,
      )
    : null;

  const handleClick = () =>
    navigate(`/schedule/${schedule.schedule_name}`, {
      state: { last_run_id: schedule.last_run_id },
    });

  const handleTrigger = (e: React.MouseEvent) => {
    e.stopPropagation();
    setTriggerLoading(true);
    setTriggerError(null);
    fetch(`/api/schedules/${encodeURIComponent(schedule.schedule_name)}/trigger`, { method: 'POST' })
      .then(async r => {
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          throw new Error(body.error || `HTTP ${r.status}`);
        }
      })
      .catch(err => setTriggerError(err.message))
      .finally(() => setTriggerLoading(false));
  };

  const handleCancelClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    setCancelDialogOpen(true);
  };

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
        onKeyDown={e => e.key === 'Enter' && handleClick()}
      >
        <div className="scheduler-card-header">
          <span className="schedule-name">{schedule.schedule_name}</span>
          <span className={`status-badge status-${displayStatus.replace(' ', '-')}`}>
            {displayStatus}
          </span>
          {!neverRun && (
            <span className="timestamps">
              {schedule.last_run_at && <>last run {formatTime(schedule.last_run_at)}</>}
            </span>
          )}
          <button
            type="button"
            className={`trigger-run-btn${triggerLoading ? ' loading' : ''}`}
            disabled={schedule.is_running || triggerLoading}
            onClick={handleTrigger}
            title={schedule.is_running ? 'A run is already active' : 'Trigger a full DAG run'}
          >
            {triggerLoading ? 'Starting...' : 'Run'}
          </button>
          {schedule.is_running && (
            <button
              type="button"
              className="cancel-run-btn"
              onClick={handleCancelClick}
              title="Cancel the active run"
            >
              Cancel
            </button>
          )}
        </div>
        {showDriftStrip && driftMsg && (
          <div
            className={`scheduler-card-stale-strip${driftState === 'unknown' ? ' scheduler-card-stale-strip--unknown' : ''}`}
            onClick={e => e.stopPropagation()}
          >
            <span aria-hidden="true">{driftState === 'unknown' ? '?' : '⚠'}</span>
            <span>{driftMsg.stripLine}</span>
          </div>
        )}
        {triggerError && <div className="trigger-error">{triggerError}</div>}
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
