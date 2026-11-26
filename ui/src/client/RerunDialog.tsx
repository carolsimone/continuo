import { useEffect, useState } from 'react';
import PreviewList, { PreviewItem } from './PreviewList';
import { fetchAllPages } from './fetch-all-pages';
import { formatCount } from './node-helpers';
import { useActionRequestId } from './request-id';
import type { Task } from './types';

const REQUEST_FAILED = 'Request failed — please try again';
const READ_FAILED = 'Can\'t read the nodes of this run. Close and try again.';
const TITLE_ID = 'rerun-dialog-title';

interface Props {
  // runId is the failed or cancelled run to rerun.
  runId: string;
  // releaseId is the release the run ran on, which the rerun runs on too;
  // null or absent when run-controller recorded none.
  releaseId: string | null | undefined;
  driftLabel: string | null;
  // projectColors is the page's accent palette, so a project is painted here
  // as everywhere else on the page.
  projectColors: Map<string, string>;
  onClose: () => void;
  // onSubmitted runs once the rerun has been accepted.
  onSubmitted: () => void;
}

type ButtonState = 'idle' | 'loading' | 'success';

// The run's tasks as the dialog read them: the ones that did not succeed run
// again.
type TasksRead = { state: 'loading' } | { state: 'failed' } | { state: 'read'; tasks: Task[] };

// taskItem lists a task under its schema.table id with its status.
function taskItem(t: Task): PreviewItem {
  const id = `${t.schema_name}.${t.table_name}`;
  return { key: t.task_id, project: t.project_name, label: id, title: `${t.project_name}.${id}`, facts: [t.status] };
}

// RerunDialog confirms a rerun: it names the release the rerun runs on (the
// run's own), reads the run's tasks as it opens and lists the ones that did
// not succeed, which run again with the nodes below them, then posts the
// rerun. Rerun stays disabled until the tasks are read. A refusal stays in
// the dialog.
export default function RerunDialog({
  runId, releaseId, driftLabel, projectColors, onClose, onSubmitted,
}: Props) {
  // One request id for the dialog: a press repeated after a timeout returns
  // the run the first one made.
  const { id: requestId } = useActionRequestId();
  const [read, setRead] = useState<TasksRead>({ state: 'loading' });
  const [buttonState, setButtonState] = useState<ButtonState>('idle');
  const [submitError, setSubmitError] = useState<string | null>(null);
  const submitting = buttonState === 'loading';

  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    fetchAllPages<Task>(`/api/runs/${encodeURIComponent(runId)}/tasks`, 'tasks', { signal })
      .then(tasks => { if (!signal.aborted) setRead({ state: 'read', tasks }); })
      .catch(() => { if (!signal.aborted) setRead({ state: 'failed' }); });
    return () => controller.abort();
  }, [runId]);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !submitting) onClose();
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [onClose, submitting]);

  const tasks = read.state === 'read' ? read.tasks : [];
  const again = tasks.filter(t => t.status !== 'succeeded');
  const inherited = tasks.length - again.length;

  const submit = async () => {
    setButtonState('loading');
    setSubmitError(null);
    try {
      const res = await fetch(`/api/runs/${encodeURIComponent(runId)}/rerun`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: requestId }),
      });
      if (res.ok) {
        setButtonState('success');
        onSubmitted();
        return;
      }
      const body = await res.json().catch(() => ({}));
      setButtonState('idle');
      setSubmitError(body?.error ?? REQUEST_FAILED);
    } catch {
      setButtonState('idle');
      setSubmitError(REQUEST_FAILED);
    }
  };

  const rerunClass = [
    'btn', 'btn--primary',
    buttonState === 'loading' ? 'is-loading' : '',
    buttonState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');
  const rerunLabel = buttonState === 'loading' ? 'Rerunning…' : buttonState === 'success' ? 'Reran' : 'Rerun';

  return (
    <div className="dialog-overlay" onClick={() => { if (!submitting) onClose(); }}>
      <div
        className="dialog dialog--wide"
        role="dialog"
        aria-modal="true"
        aria-labelledby={TITLE_ID}
        onClick={e => e.stopPropagation()}
      >
        <h2 className="dialog-title" id={TITLE_ID}>Rerun this run</h2>
        <p className="dialog-subtitle">
          {releaseId ? `On release ${releaseId}, the release this run ran on.` : 'On the release this run ran on.'}
        </p>

        {driftLabel && (
          <div className="info-strip info-strip--warning">
            <span className="info-strip__icon">⚠</span>
            <span>{driftLabel}</span>
          </div>
        )}

        {read.state === 'read' && (
          <section className="preview-section">
            <div className="section-header">
              <div className="section-header__main">
                <span className="section-header__title">Will run</span>
                <span className="section-header__count">{formatCount(again.length)}</span>
              </div>
              <div className="section-header__sub">
                {`of ${formatCount(tasks.length)} nodes · ${formatCount(inherited)} inherited`}
              </div>
            </div>
            <PreviewList items={again.map(taskItem)} colors={projectColors} />
          </section>
        )}

        {read.state === 'loading' && (
          <div className="info-strip info-strip--neutral">
            <span>Loading the nodes this run did not finish…</span>
          </div>
        )}
        {read.state === 'failed' && (
          <div className="info-strip info-strip--error">
            <span className="info-strip__icon">⚠</span>
            <span>{READ_FAILED}</span>
          </div>
        )}
        {submitError && (
          <div className="info-strip info-strip--error">
            <span className="info-strip__icon">⚠</span>
            <span>{submitError}</span>
          </div>
        )}

        <div className="dialog-actions">
          <button type="button" className="btn btn--secondary" onClick={onClose} disabled={submitting}>
            Cancel
          </button>
          <button
            type="button"
            className={rerunClass}
            onClick={submit}
            disabled={buttonState !== 'idle' || read.state !== 'read'}
          >
            {rerunLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
