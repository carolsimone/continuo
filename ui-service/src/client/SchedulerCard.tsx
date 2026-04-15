import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  getScheduleProgressLabel,
  getScheduleProgressPercent,
} from './scheduler-card-helpers';
import { ScheduleSummary, Task } from './types';

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
  const [triggerLoading, setTriggerLoading] = useState(false);
  const [triggerError, setTriggerError] = useState<string | null>(null);

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

  const displayStatus = neverRun
    ? 'never run'
    : schedule.is_running
    ? 'running'
    : schedule.last_run_status;

  const total = tasks.length;
  const succeeded = tasks.filter(t => t.status === 'succeeded').length;
  const failed = tasks.filter(t => t.status === 'failed').length;
  const pending = tasks.filter(t => t.status === 'pending').length;
  const pct = getScheduleProgressPercent(tasks);

  const handleClick = () =>
    navigate(`/schedule/${schedule.schedule_name}`, {
      state: { last_run_id: schedule.last_run_id },
    });

  const handleTrigger = (e: React.MouseEvent) => {
    e.stopPropagation();
    setTriggerLoading(true);
    setTriggerError(null);
    fetch(`/api/schedules/${schedule.schedule_name}/trigger`, { method: 'POST' })
      .then(async r => {
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          throw new Error(body.error || `HTTP ${r.status}`);
        }
      })
      .catch(err => setTriggerError(err.message))
      .finally(() => setTriggerLoading(false));
  };

  return (
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
          className={`trigger-run-btn${triggerLoading ? ' loading' : ''}`}
          disabled={schedule.is_running || triggerLoading}
          onClick={handleTrigger}
          title={schedule.is_running ? 'A run is already active' : 'Trigger a full DAG run'}
        >
          {triggerLoading ? 'Starting...' : 'Run'}
        </button>
      </div>
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
            <span>✅ {succeeded} succeeded</span>
            <span>❌ {failed} failed</span>
            <span>⏳ {pending} pending</span>
          </div>
        </div>
      )}
    </div>
  );
}
