import type { RunAction } from '../detail-page-helpers';
import type { useManualRun } from './useManualRun';

type PressState = 'idle' | 'success';

// The hover title of an enabled Rerun or Rebase: what the press does.
const RERUN_TITLE = 'Run again the nodes this run did not finish, on the release it ran on';
const REBASE_TITLE = 'Restart this run from its failure on the live release';

// actionClass is a run action's button: secondary, green while its success
// shows.
function actionClass(state: PressState): string {
  return ['btn', 'btn--secondary', state === 'success' ? 'is-success' : ''].filter(Boolean).join(' ');
}

// The action row under the header — Start run (a manual run), Rerun and Rebase onto live
// release — and the manual run's error. Rerun and Rebase are always shown; one
// that cannot act on the run on screen is greyed out and its hover title says
// why.
export default function DetailActions({
  name, liveRunExists, manualRun, rerun, rebase, rerunState, rebaseState, onOpenRerun, onOpenRebase,
}: {
  name: string | undefined;
  liveRunExists: boolean;
  manualRun: ReturnType<typeof useManualRun>;
  rerun: RunAction;
  rebase: RunAction;
  rerunState: PressState;
  rebaseState: PressState;
  onOpenRerun: () => void;
  onOpenRebase: () => void;
}) {
  const { startState, startError, start: onStart } = manualRun;

  const startBtnClass = [
    'btn', 'btn--secondary',
    startState === 'loading' ? 'is-loading' : '',
    startState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');

  const startLabel =
    startState === 'loading' ? 'Starting…' :
    startState === 'success' ? 'Started' :
    '▶ Start run';

  return (
    <>
      <div className="page-action-row">
        {name && (
          <button
            type="button"
            className={startBtnClass}
            disabled={liveRunExists || startState === 'loading' || startState === 'success'}
            onClick={onStart}
            title={liveRunExists ? 'A run is already active' : 'Start a manual run of the whole schedule'}
          >
            {startLabel}
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

      {startState === 'error' && startError && (
        <div className="info-strip info-strip--error">{startError}</div>
      )}
    </>
  );
}
