import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { Link, useNavigate } from 'react-router';
import { formatDayTime, scheduleState } from './scheduler-card-helpers';
import { taskPillClass, taskStatusTitle } from './detail-page-helpers';
import { getDriftState, getDriftBadge } from './drift-helpers';
import { formatRelative } from './node-helpers';
import { joinFacts } from './PreviewList';
import { nodesComplete, nodeTotal, summarizeTasks, TaskStatusBar, TaskStatusCounts } from './TaskStatusProgress';
import { RunGraph, ScheduleSummary, Task } from './types';
import type { RunOperation } from '../server/shared/operation';
import CancelDialog from './CancelDialog';
import CronDialog from './CronDialog';
import { useCurrentUser } from './auth/AuthContext';
import { fetchAllPages } from './fetch-all-pages';
import { readJson } from './read-json';
import { settlesRequest, useActionRequestId } from './request-id';

interface Props {
  schedule: ScheduleSummary;
  // Reads the schedule list again at once. The card calls it after a trigger
  // or a cancel succeeds, so the card shows the new run or the cancelling run
  // without waiting for the next poll, and when its cron dialog closes, so it
  // shows the cron as saved.
  onRunChanged: () => void;
}

// whenLine is the muted fact after a schedule's cron: when it fires next, or
// who paused it and when. A schedule whose cron fires no more has none.
function whenLine(schedule: ScheduleSummary): string | null {
  if (schedule.paused) {
    const who = schedule.updated_by ? `paused by ${schedule.updated_by}` : 'paused';
    return schedule.updated_at ? `${who} · ${formatDayTime(schedule.updated_at)}` : who;
  }
  return schedule.next_fire_at ? `next ${formatDayTime(schedule.next_fire_at, schedule.timezone)}` : null;
}

// How long ago the last run was created. It is not when the run ended: a run
// that took three hours and has just ended started 3h ago.
function startedLine(iso: string): string {
  const ago = formatRelative(iso);
  return ago === 'now' ? 'started just now' : `started ${ago}`;
}

// True once a drag has selected text: the click that ends the drag is not a
// click on what lies under it.
const hasSelectedText = () => (window.getSelection()?.toString() ?? '') !== '';

export default function SchedulerCard({ schedule, onRunChanged }: Props) {
  const navigate = useNavigate();
  const neverRun = !schedule.last_run_id;
  // The latest answer for a run's tasks, with the run it is for, so a new run
  // never shows the tasks of the run before it.
  const [taskRead, setTaskRead] = useState<{ runId: string; tasks: Task[] } | null>(null);
  const [tasksReload, setTasksReload] = useState(0);
  const [drift, setDrift] = useState<{ run: number; latest: number } | null>(null);
  const [triggerLoading, setTriggerLoading] = useState(false);
  const [triggerStatus, setTriggerStatus] = useState<'idle' | 'success'>('idle');
  const [triggerError, setTriggerError] = useState<string | null>(null);
  const [operation, setOperation] = useState<RunOperation>('run');
  const [cancelDialogOpen, setCancelDialogOpen] = useState(false);
  // The mode of the open cron dialog, fixed when it opens: a poll that adds
  // or removes the cron meanwhile does not turn an Add into an Edit.
  const [cronDialogMode, setCronDialogMode] = useState<'edit' | 'add' | null>(null);
  const isOperator = useCurrentUser()?.role === 'operator';
  // The trigger button's request id: a press that gets no answer and is
  // repeated for the same operation, while the card still shows the last run
  // the first press saw, sends the same id, so the run the first press made is
  // returned rather than made twice. A newer last run on the card (the
  // schedule list's poll) makes the next press a new action, and the id is
  // renewed once the server has given its final answer.
  const { take: takeRequestId, renew: renewTriggerId } = useActionRequestId();
  const actionsRef = useRef<HTMLDivElement>(null);
  const triggerGroupRef = useRef<HTMLDivElement>(null);
  const cancelRef = useRef<HTMLButtonElement>(null);

  // Reads the run's tasks now and every 5 s after. A change of run or a bump
  // of tasksReload starts the reads over; the reads already in flight are
  // aborted and their answers dropped, so an answer from before a cancel never
  // replaces one from after it. A read that fails keeps the tasks the run
  // already has, and settles a run that has none yet as having none.
  useEffect(() => {
    const runId = schedule.last_run_id;
    if (!runId) return;
    let cancelled = false;
    const controller = new AbortController();
    const fetch_ = () =>
      fetchAllPages<Task>(`/api/runs/${runId}/tasks`, 'tasks', {
        signal: controller.signal,
      })
        .then(tasks => { if (!cancelled) setTaskRead({ runId, tasks }); })
        .catch(() => {
          if (!cancelled) setTaskRead(read => (read?.runId === runId ? read : { runId, tasks: [] }));
        });
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
      readJson<RunGraph>(`/api/runs/${schedule.last_run_id}/graph`)
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

  // An error from an earlier press says nothing about a newer run, and while
  // that run is active there is no Trigger to press and clear it.
  useEffect(() => {
    setTriggerError(null);
  }, [schedule.last_run_id]);

  // Trigger is on offer while no run is active. A press stays on screen until
  // its request has answered and its success has shown, even once the run it
  // started reads active; Cancel appears as soon as a run is active, so it is
  // never hidden while there is a run to cancel.
  const triggerBusy = triggerLoading || triggerStatus === 'success';
  const showTrigger = !schedule.has_active_run || triggerBusy;

  // A browser drops the focus to the page when the focused control leaves the
  // document. This render, the last before that control goes, reads whether the
  // focus is on it; only here can that be told from focus the operator moved
  // away, since a removal fires blur too. The focus then goes to the group of
  // controls, which holds the keyboard's place and does nothing on a key press: a
  // key meant for Cancel must not land on Trigger, which has no confirmation.
  const holdsFocus = (el: HTMLElement | null) => Boolean(el?.contains(document.activeElement));
  const focusIsLeaving =
    (!showTrigger && holdsFocus(triggerGroupRef.current)) ||
    (!schedule.has_active_run && holdsFocus(cancelRef.current));
  useLayoutEffect(() => {
    if (focusIsLeaving) actionsRef.current?.focus({ preventScroll: true });
  }, [focusIsLeaving]);

  // A cancelling run is active (has_active_run) but not running: it reads
  // cancelling, and its Cancel button stays available.
  const displayStatus = neverRun
    ? 'never run'
    : schedule.has_active_run
    ? (schedule.last_run_status === 'cancelling' ? 'cancelling' : 'running')
    : schedule.last_run_status;

  const cronState = scheduleState(schedule);
  const hasCron = cronState !== 'manual';
  const when = hasCron ? whenLine(schedule) : null;
  // A cron that fires while no model of the live release has the tag records
  // a failed run at each fire.
  const showNoModelsStrip = cronState === 'scheduled' && schedule.model_count === 0;

  const read = taskRead?.runId === schedule.last_run_id ? taskRead : null;
  const summary = summarizeTasks(read?.tasks ?? []);
  // Until the first read of a run settles, its body holds its place as an empty
  // track, so the card does not grow when the tasks arrive. A run read as having
  // no tasks has no body.
  const showBody = !neverRun && (read === null || summary.total > 0);

  const driftState = drift ? getDriftState(drift.run, drift.latest) : 'fresh';
  // Only a stale run (pinned to an older topology than the latest) warrants a
  // chip. The 'unknown' state (a run predating topology tracking) is silent.
  const driftBadge = driftState === 'stale' && drift
    ? getDriftBadge('stale', drift.run, drift.latest)
    : null;

  const schedulePath = `/schedule/${encodeURIComponent(schedule.schedule_name)}`;
  const navigationState = { last_run_id: schedule.last_run_id };

  // A click on the card is a mouse shortcut for the name, which holds the real
  // link: it leaves a modified click to the browser and ignores the click that
  // ends a text selection.
  const handleCardClick = (e: React.MouseEvent) => {
    if (e.metaKey || e.ctrlKey || e.shiftKey || hasSelectedText()) return;
    navigate(schedulePath, { state: navigationState });
  };

  const stopCardClick = (e: React.MouseEvent) => e.stopPropagation();

  const handleTrigger = () => {
    setTriggerLoading(true);
    setTriggerError(null);
    fetch(`/api/schedules/${encodeURIComponent(schedule.schedule_name)}/trigger`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ request_id: takeRequestId(operation, schedule.last_run_id), operation: operation === 'run' ? '' : operation }),
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

  // A cancel keeps the same run, so the card reads both the schedule list (the
  // run's status) and the run's tasks (now stopping or cancelled) again.
  const handleCancelled = () => {
    setTasksReload(n => n + 1);
    onRunChanged();
  };

  const handleCronDialogClose = () => {
    setCronDialogMode(null);
    onRunChanged();
  };

  const triggerIdleLabel =
    operation === 'test' ? 'Trigger test' : operation === 'build' ? 'Trigger build' : 'Trigger run';
  const triggerStateClass = triggerLoading ? 'is-loading' : triggerStatus === 'success' ? 'is-success' : '';

  const scheduleFacts: ReactNode[] = [];
  if (hasCron) {
    const cronClass = [
      'schedule-cron',
      cronState === 'paused' ? 'schedule-cron--paused' : '',
      schedule.cron_words ? '' : 'schedule-cron--raw',
    ].filter(Boolean).join(' ');
    scheduleFacts.push(
      <span className={cronClass} title={schedule.cron_words ? schedule.cron : undefined}>
        {schedule.cron_words || schedule.cron}
      </span>,
    );
    if (schedule.timezone) scheduleFacts.push(<span>{schedule.timezone}</span>);
  } else {
    scheduleFacts.push(<span>No cron: runs only when triggered</span>);
  }
  if (when) scheduleFacts.push(<span>{when}</span>);
  if (!neverRun && schedule.last_run_at) {
    scheduleFacts.push(
      <span title={new Date(schedule.last_run_at).toLocaleString()}>{startedLine(schedule.last_run_at)}</span>,
    );
  }

  const progressFacts: ReactNode[] = schedule.has_active_run
    ? [<span>{nodesComplete(summary)}</span>, <span>{summary.percent}%</span>]
    : [<span>{nodeTotal(summary)}</span>];

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
      {cronDialogMode && createPortal(
        <CronDialog schedule={schedule} mode={cronDialogMode} onClose={handleCronDialogClose} />,
        document.body,
      )}
      <div
        className={`detail-card scheduler-card${displayStatus === 'failed' ? ' scheduler-card--failed' : ''}`}
        onClick={handleCardClick}
      >
        <div className="scheduler-card-header">
          <Link
            className="schedule-name"
            to={schedulePath}
            state={navigationState}
            onClick={stopCardClick}
          >
            {schedule.schedule_name}
          </Link>
          <span
            className={`pill-sm ${taskPillClass(displayStatus)}`}
            title={taskStatusTitle(displayStatus)}
            data-testid="run-status"
          >
            {displayStatus}
          </span>
          {driftBadge && (
            <span className="info-strip info-strip--warning info-strip--inline" onClick={stopCardClick}>
              <span className="info-strip__icon" aria-hidden="true">⚠</span>
              {driftBadge}
            </span>
          )}
          <div
            ref={actionsRef}
            className="scheduler-card-actions"
            role="group"
            aria-label={`${schedule.schedule_name} controls`}
            tabIndex={-1}
            onClick={stopCardClick}
          >
            {showTrigger && (
              <div ref={triggerGroupRef} className="btn-group">
                <button
                  type="button"
                  className={['btn', 'btn--secondary', triggerStateClass].filter(Boolean).join(' ')}
                  disabled={triggerBusy}
                  onClick={handleTrigger}
                  title="Trigger a full DAG run"
                >
                  {triggerLoading ? 'Triggering…' : triggerStatus === 'success' ? 'Triggered' : triggerIdleLabel}
                </button>
                <label className={['btn', 'btn--secondary', 'btn--caret', triggerStateClass].filter(Boolean).join(' ')} title="Operation">
                  <span className="sr-only">Operation</span>
                  <select
                    value={operation}
                    disabled={triggerBusy}
                    onChange={e => setOperation(e.target.value as RunOperation)}
                  >
                    <option value="run">Run</option>
                    <option value="test">Test</option>
                    <option value="build">Build</option>
                  </select>
                  <svg className="btn__ico" width="8" height="5" viewBox="0 0 8 5" aria-hidden="true">
                    <path d="M1 1l3 3 3-3" fill="none" stroke="currentColor" strokeWidth="1.25" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </label>
              </div>
            )}
            {schedule.has_active_run && (
              <button
                ref={cancelRef}
                type="button"
                className="btn btn--danger"
                onClick={() => setCancelDialogOpen(true)}
                title="Cancel the active run"
              >
                Cancel
              </button>
            )}
            {isOperator && (
              <button
                type="button"
                className="btn btn--secondary"
                onClick={() => setCronDialogMode(hasCron ? 'edit' : 'add')}
              >
                {hasCron ? 'Edit cron' : 'Add cron'}
              </button>
            )}
          </div>
        </div>
        <div className="preview-facts schedule-facts">
          {cronState === 'paused' && <span className="pill-sm pill-sm--paused">paused</span>}
          {joinFacts(scheduleFacts)}
        </div>
        {showNoModelsStrip && (
          <div className="info-strip info-strip--warning" onClick={stopCardClick}>
            <span className="info-strip__icon" aria-hidden="true">⚠</span>
            <span>
              No model in the live release has the tag <b>{schedule.schedule_name}</b>. Each fire records a failed
              run until a release brings them back, or you remove its cron.
            </span>
          </div>
        )}
        {triggerError && <div className="info-strip info-strip--error" onClick={stopCardClick}>{triggerError}</div>}
        {showBody && (
          <div className="scheduler-card-body">
            <TaskStatusBar summary={summary} />
            {summary.total > 0 && (
              <div className="scheduler-card-summary">
                <TaskStatusCounts summary={summary} hideZero />
                <div className="preview-facts schedule-facts scheduler-card-total">{joinFacts(progressFacts)}</div>
              </div>
            )}
          </div>
        )}
      </div>
    </>
  );
}
