import type { RunAction } from '../detail-page-helpers';
import type { useTriggerRun } from './useTriggerRun';

type PressState = 'idle' | 'success';

// The hover title of an enabled Rerun or Rebase: what the press does.
const RERUN_TITLE = 'Run again the nodes this run did not finish, on the release it ran on';
const REBASE_TITLE = 'Restart this run from its failure on the live release';

// actionClass is a run action's button: secondary, green while its success
// shows.
function actionClass(state: PressState): string {
  return ['btn', 'btn--secondary', state === 'success' ? 'is-success' : ''].filter(Boolean).join(' ');
}

// The action row under the header — Trigger run, Rerun and Rebase onto live
// release — and the trigger's error. Rerun and Rebase are always shown; one
// that cannot act on the run on screen is greyed out and its hover title says
// why.
export default function DetailActions({
  name, liveRunExists, trigger, rerun, rebase, rerunState, rebaseState, onOpenRerun, onOpenRebase,
}: {
  name: string | undefined;
  liveRunExists: boolean;
  trigger: ReturnType<typeof useTriggerRun>;
  rerun: RunAction;
  rebase: RunAction;
  rerunState: PressState;
  rebaseState: PressState;
  onOpenRerun: () => void;
  onOpenRebase: () => void;
}) {
  const { triggerState, triggerError, trigger: onTrigger } = trigger;

  const triggerBtnClass = [
    'btn', 'btn--secondary',
    triggerState === 'loading' ? 'is-loading' : '',
    triggerState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');

  const triggerLabel =
    triggerState === 'loading' ? 'Triggering…' :
    triggerState === 'success' ? 'Triggered' :
    '▶ Trigger run';

  return (
    <>
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
        <button
          type="button"
          className={actionClass(rerunState)}
          disabled={!rerun.enabled || rerunState === 'success'}
          onClick={onOpenRerun}
          title={rerun.reason ?? RERUN_TITLE}
        >
          {rerunState === 'success' ? 'Reran' : '↺ Rerun'}
        </button>
        <button
          type="button"
          className={actionClass(rebaseState)}
          disabled={!rebase.enabled || rebaseState === 'success'}
          onClick={onOpenRebase}
          title={rebase.reason ?? REBASE_TITLE}
        >
          {rebaseState === 'success' ? 'Rebased' : '↪ Rebase onto live release'}
        </button>
      </div>

      {triggerState === 'error' && triggerError && (
        <div className="info-strip info-strip--error">{triggerError}</div>
      )}
    </>
  );
}
