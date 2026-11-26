import { useCallback, useMemo, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router';
import { RunSummary } from './types';
import {
  DetailMode,
  isTerminalStatus,
  resolveActiveGraph,
  groupAttemptsByTask,
  runFailureLabel,
  formatStatusLabel,
  graphBadgeClass,
  graphBadgeLabel as graphBadgeLabelOf,
  graphCardState as graphCardStateOf,
  runTabState,
  driftLabelOf,
  runActions,
} from './detail-page-helpers';
import { getDriftState } from './drift-helpers';
import { useActionRequestId, type ActionRequestId } from './request-id';
import DetailActions from './detail-page/DetailActions';
import DetailHeader from './detail-page/DetailHeader';
import LatestView from './detail-page/LatestView';
import RunView from './detail-page/RunView';
import SnapshotStrip from './detail-page/SnapshotStrip';
import { useLastRun } from './detail-page/useLastRun';
import { useRunRows } from './detail-page/useRunRows';
import { useScheduleGraph } from './detail-page/useScheduleGraph';
import { RUNS_PAGE_SIZE, useScheduleRuns } from './detail-page/useScheduleRuns';
import { useSelectedRunGraph } from './detail-page/useSelectedRunGraph';
import { useProjectTree } from './detail-page/useProjectTree';
import { useTriggerRun } from './detail-page/useTriggerRun';
import RebaseDialog from './RebaseDialog';
import RerunDialog from './RerunDialog';

interface DetailPageProps {
  mode?: DetailMode;
}

// A schedule's page. Each schedule gets its own ScheduleDetail, so arriving
// at another schedule starts from empty page state, while a navigation that
// changes only the query string — a panel tab — keeps it. The trigger
// button's request id is held here, above that boundary: a press that got no
// answer and is repeated after the page left the schedule and came back
// still sends the same id.
export default function DetailPage({ mode = 'run' }: DetailPageProps) {
  const { name } = useParams<{ name: string }>();
  const requestId = useActionRequestId();
  return <ScheduleDetail key={name} name={name} mode={mode} requestId={requestId} />;
}

function ScheduleDetail({ name, mode, requestId }: {
  name: string | undefined;
  mode: DetailMode;
  requestId: ActionRequestId;
}) {
  const navigate = useNavigate();
  const location = useLocation();

  const {
    lastRunId, scheduler, setScheduler, liveRunGraph, readFailed: lastRunReadFailed, refreshLastRun, pastRunsVersion,
  } = useLastRun(name, location.state);
  const { graph, graphState, promotionSeq } = useScheduleGraph(name, mode);
  const { runs, runsPage, setRunsPage, runsCount, readFailed: runsReadFailed } = useScheduleRuns(name, pastRunsVersion);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  // The selected run's summary, kept so the header still describes it after
  // paging away from the page it was chosen on.
  const [pinnedRun, setPinnedRun] = useState<RunSummary | null>(null);
  const {
    tasks, attempts, tasksRead, readFailed: rowsReadFailed, clearRows,
  } = useRunRows({
    lastRunId, selectedRunId, mode, onScheduler: setScheduler,
  });
  const { runGraph, readFailed: runGraphReadFailed } = useSelectedRunGraph(selectedRunId);
  const trigger = useTriggerRun(name, lastRunId, requestId, refreshLastRun);
  // Any read of this page is failing and being retried.
  const readFailed = lastRunReadFailed || graphState === 'error' || runsReadFailed || rowsReadFailed || runGraphReadFailed;
  const [rerunState, setRerunState] = useState<'idle' | 'success'>('idle');
  const [rebaseState, setRebaseState] = useState<'idle' | 'success'>('idle');
  const [openDialog, setOpenDialog] = useState<'rerun' | 'rebase' | null>(null);

  const selectedRun = runs.find((run) => run.run_id === selectedRunId)
    ?? (pinnedRun && pinnedRun.run_id === selectedRunId ? pinnedRun : null);
  const activeGraph = resolveActiveGraph({
    mode, scheduleGraph: graph, liveRunGraph, selectedRunGraph: runGraph, selectedRunId,
  });
  const tree = useProjectTree(activeGraph, tasks);
  const attemptsByTask = useMemo(() => groupAttemptsByTask(attempts), [attempts]);

  // The Rerun and Rebase dialogs post their run themselves; once one is
  // accepted the dialog closes and the page's button shows the success.
  const handleRerunSubmitted = useCallback(() => {
    setOpenDialog(null);
    setRerunState('success');
    setTimeout(() => setRerunState('idle'), 3000);
    void refreshLastRun();
  }, [refreshLastRun]);
  const handleRebaseSubmitted = useCallback(() => {
    setOpenDialog(null);
    setRebaseState('success');
    setTimeout(() => setRebaseState('idle'), 3000);
    void refreshLastRun();
  }, [refreshLastRun]);
  const closeDialog = useCallback(() => setOpenDialog(null), []);

  // The shown run's rows are cleared at once, so the newly chosen run never
  // renders over the previous run's tasks while its own are read.
  const handleSelectRun = (runId: string | null) => {
    if (runId !== selectedRunId) clearRows();
    setSelectedRunId(runId);
    setPinnedRun(runs.find((run) => run.run_id === runId) ?? null);
    tree.resetTree();
  };

  const liveSchedulerStatus =
    scheduler?.status ||
    (lastRunId === null ? 'never run' : readFailed ? 'unavailable' : 'loading');
  const schedulerStatus = selectedRun
    ? selectedRun.status || 'pending'
    : liveSchedulerStatus;
  const liveRunExists = !!lastRunId && !!scheduler && !isTerminalStatus(scheduler.status);
  const snapshotRun = selectedRunId
    ? selectedRun ?? { completed_at: null, created_at: null, status: null }
    : null;
  // The release of the run the page shows: a selected past run's own, which
  // is unknown (no link) rather than the latest run's when its summary names
  // none, else the latest run's.
  const shownReleaseId = selectedRunId ? selectedRun?.release_id : scheduler?.release_id;
  const shouldRenderGraph = !!activeGraph && activeGraph.nodes.length > 0;
  // The run Rerun and Rebase act on: the chosen past run, else the latest run.
  // A status still being read is null.
  const runOnScreen = selectedRunId
    ? { run_id: selectedRunId, status: selectedRun?.status ?? null }
    : lastRunId ? { run_id: lastRunId, status: scheduler?.status ?? null } : null;
  const runOnScreenGraph = selectedRunId ? runGraph : liveRunGraph;
  const actions = runActions({
    run: runOnScreen,
    lastRunId,
    liveRunExists,
    driftState: getDriftState(runOnScreenGraph?.run_promotion_seq, runOnScreenGraph?.latest_promotion_seq),
  });
  const graphBadge = graphBadgeLabelOf({ mode, selectedRun, liveRunExists, activeGraph });
  const driftLabel = driftLabelOf(liveRunGraph);
  const pastRuns = {
    runs,
    liveRunId: liveRunExists ? lastRunId : null,
    liveStatus: liveRunExists ? formatStatusLabel(liveSchedulerStatus) : null,
    selectedRunId,
    onSelectRun: handleSelectRun,
    page: runsPage,
    pageSize: RUNS_PAGE_SIZE,
    total: runsCount,
    onPageChange: setRunsPage,
  };

  return (
    <div className="page">
      <DetailHeader
        name={name}
        status={schedulerStatus}
        failureLabel={runFailureLabel(selectedRun ? selectedRun.failure_reason : scheduler?.failure_reason)}
        driftLabel={driftLabel}
        mode={mode}
        promotionSeq={promotionSeq}
        onBack={() => navigate('/')}
      />

      <main className="page-content">
        <DetailActions
          name={name}
          liveRunExists={liveRunExists}
          trigger={trigger}
          rerun={actions.rerun}
          rebase={actions.rebase}
          rerunState={rerunState}
          rebaseState={rebaseState}
          onOpenRerun={() => setOpenDialog('rerun')}
          onOpenRebase={() => setOpenDialog('rebase')}
        />

        {readFailed && (
          <div className="info-strip info-strip--warning" role="status">
            <span className="info-strip__icon">⚠</span>
            Can&apos;t read the latest data from the server. Retrying every 5 seconds.
          </div>
        )}

        {openDialog === 'rerun' && runOnScreen && (
          <RerunDialog
            runId={runOnScreen.run_id}
            releaseId={shownReleaseId}
            driftLabel={driftLabel}
            projectColors={tree.projectColors}
            onClose={closeDialog}
            onSubmitted={handleRerunSubmitted}
          />
        )}
        {openDialog === 'rebase' && runOnScreen && (
          <RebaseDialog
            runId={runOnScreen.run_id}
            driftLabel={driftLabel}
            projectColors={tree.projectColors}
            onClose={closeDialog}
            onSubmitted={handleRebaseSubmitted}
          />
        )}

        {snapshotRun && (
          <SnapshotStrip run={snapshotRun} liveRunExists={liveRunExists} onReturn={() => handleSelectRun(null)} />
        )}

        {mode === 'latest' ? (
          <LatestView
            name={name}
            activeGraph={activeGraph}
            graphCardState={graphCardStateOf({
              selectedRunId, runGraph, shouldRenderGraph, graphState, selectedRunGraphFailed: runGraphReadFailed,
            })}
            graphBadge={graphBadge}
            graphBadgeClass={graphBadgeClass(graphBadge, snapshotRun?.status)}
            selectedNodeId={tree.selectedNodeId}
            selectedRunId={selectedRunId}
            lastRunId={lastRunId}
            expandedProjects={tree.effectiveExpandedProjects}
            projectColors={tree.projectColors}
            onNodeSelect={tree.handleNodeSelect}
            onProjectToggle={tree.handleProjectToggle}
            pastRuns={pastRuns}
          />
        ) : (
          <RunView
            // The Run tab summarises a run's task set. With nothing yet to
            // summarise it must not render RunProgressHeader over an empty
            // list, which would read as a fake "0 of 0 nodes complete / 0%".
            state={runTabState({
              lastRunId, runsCount, taskCount: tasks.length, tasksRead, selectedRunId, runGraph, readFailed,
            })}
            selectedRunId={selectedRunId}
            releaseId={shownReleaseId}
            tasks={tasks}
            attemptsByTask={attemptsByTask}
            activeGraph={activeGraph}
            projects={tree.projects}
            expandedProjects={tree.effectiveExpandedProjects}
            onProjectToggle={tree.handleProjectToggle}
            pastRuns={pastRuns}
          />
        )}
      </main>
    </div>
  );
}
