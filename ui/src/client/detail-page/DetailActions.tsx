import type { useTriggerRun } from './useTriggerRun';

// The action row under the header — Trigger run, and Rerun failed when the
// last run can be rerun — and the trigger's error.
export default function DetailActions({
  name, liveRunExists, showRerunFailed, trigger, rerunState, onOpenRerun,
}: {
  name: string | undefined;
  liveRunExists: boolean;
  showRerunFailed: boolean;
  trigger: ReturnType<typeof useTriggerRun>;
  rerunState: 'idle' | 'success';
  onOpenRerun: () => void;
}) {
  const { triggerState, triggerError, trigger: onTrigger } = trigger;

  const triggerBtnClass = [
    'btn', 'btn--secondary',
    triggerState === 'loading' ? 'is-loading' : '',
    triggerState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');

  const rerunFailedBtnClass = [
    'btn', 'btn--secondary',
    rerunState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');

  const triggerLabel =
    triggerState === 'loading' ? 'Triggering…' :
    triggerState === 'success' ? 'Triggered' :
    '▶ Trigger run';

  const rerunFailedLabel = rerunState === 'success' ? 'Reran' : '↺ Rerun failed';

  return (
    <>
      {(name || showRerunFailed) && (
        <div className="page-action-row">
          {name && (
            <button
              type="button"
              className={triggerBtnClass}
              disabled={liveRunExists || triggerState === 'loading' || triggerState === 'success'}
              onClick={onTrigger}
              title={liveRunExists ? 'A run is already active' : 'Trigger a full DAG run'}
            >
              {triggerLabel}
            </button>
          )}
          {showRerunFailed && (
            <button
              type="button"
              className={rerunFailedBtnClass}
              disabled={rerunState === 'success'}
              onClick={onOpenRerun}
              title="Re-execute non-succeeded tasks"
            >
              {rerunFailedLabel}
            </button>
          )}
        </div>
      )}

      {triggerState === 'error' && triggerError && (
        <div className="info-strip info-strip--error">{triggerError}</div>
      )}
    </>
  );
}
