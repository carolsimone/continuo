import { useCallback, useMemo, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router';
import { RunSummary } from './types';
import {
  DetailMode,
  isRerunnableStatus,
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
} from './detail-page-helpers';
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
import { useServiceTree } from './detail-page/useServiceTree';
import { useTriggerRun } from './detail-page/useTriggerRun';
import RerunFailedModal from './RerunFailedModal';

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
    lastRunId, lastRunObservation, scheduler, setScheduler, liveRunGraph,
  } = useLastRun(name, location.state);
  const { graph, graphState, promotionSeq } = useScheduleGraph(name, mode);
  const { runs, runsPage, setRunsPage, runsCount } = useScheduleRuns(name);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  // The selected run's summary, kept so the header still describes it after
  // paging away from the page it was chosen on.
  const [pinnedRun, setPinnedRun] = useState<RunSummary | null>(null);
  const { tasks, attempts, clearRows } = useRunRows({
    lastRunId, selectedRunId, mode, onScheduler: setScheduler,
  });
  const runGraph = useSelectedRunGraph(selectedRunId);
  const trigger = useTriggerRun(name, lastRunObservation, requestId);
  const [rerunFailedState, setRerunFailedState] = useState<'idle' | 'success'>('idle');
  const [rerunModalOpen, setRerunModalOpen] = useState(false);

  const selectedRun = runs.find((run) => run.run_id === selectedRunId)
    ?? (pinnedRun && pinnedRun.run_id === selectedRunId ? pinnedRun : null);
  const activeGraph = resolveActiveGraph({
    mode, scheduleGraph: graph, liveRunGraph, selectedRunGraph: runGraph, selectedRunId,
  });
  const tree = useServiceTree(activeGraph, tasks);
  const attemptsByTask = useMemo(() => groupAttemptsByTask(attempts), [attempts]);

  // The rerun-failed dialog posts the rerun or the rebase itself; once one is
  // accepted the dialog closes and the page's button shows the success.
  const handleRerunFailedSubmitted = useCallback(() => {
    setRerunModalOpen(false);
    setRerunFailedState('success');
    setTimeout(() => setRerunFailedState('idle'), 3000);
  }, []);

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
    (lastRunId === undefined ? 'loading' : lastRunId === null ? 'never run' : 'loading');
  const schedulerStatus = selectedRun
    ? selectedRun.status || 'pending'
    : liveSchedulerStatus;
  const liveRunExists = !!lastRunId && !!scheduler && !isTerminalStatus(scheduler.status);
  const snapshotRun = selectedRunId
    ? selectedRun ?? { completed_at: null, created_at: null, status: null }
    : null;
  const shouldRenderGraph = !!activeGraph && activeGraph.nodes.length > 0;
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
          showRerunFailed={isRerunnableStatus(scheduler?.status) && Boolean(lastRunId)}
          trigger={trigger}
          rerunState={rerunFailedState}
          onOpenRerun={() => setRerunModalOpen(true)}
        />

        {rerunModalOpen && lastRunId && (
          <RerunFailedModal
            runId={lastRunId}
            driftLabel={driftLabel}
            serviceColors={tree.serviceColors}
            onClose={() => setRerunModalOpen(false)}
            onSubmitted={handleRerunFailedSubmitted}
          />
        )}

        {snapshotRun && (
          <SnapshotStrip run={snapshotRun} liveRunExists={liveRunExists} onReturn={() => handleSelectRun(null)} />
        )}

        {mode === 'latest' ? (
          <LatestView
            name={name}
            activeGraph={activeGraph}
            graphCardState={graphCardStateOf({ selectedRunId, runGraph, shouldRenderGraph, graphState })}
            graphBadge={graphBadge}
            graphBadgeClass={graphBadgeClass(graphBadge, snapshotRun?.status)}
            selectedNodeId={tree.selectedNodeId}
            selectedRunId={selectedRunId}
            lastRunId={lastRunId}
            expandedServices={tree.effectiveExpandedServices}
            serviceColors={tree.serviceColors}
            onNodeSelect={tree.handleNodeSelect}
            onServiceToggle={tree.handleServiceToggle}
            pastRuns={pastRuns}
          />
        ) : (
          <RunView
            // The Run tab summarises a run's task set. With nothing yet to
            // summarise it must not render RunProgressHeader over an empty
            // list, which would read as a fake "0 of 0 nodes complete / 0%".
            state={runTabState({ lastRunId, runsCount, taskCount: tasks.length, selectedRunId, runGraph })}
            selectedRunId={selectedRunId}
            tasks={tasks}
            attemptsByTask={attemptsByTask}
            activeGraph={activeGraph}
            services={tree.services}
            expandedServices={tree.effectiveExpandedServices}
            onServiceToggle={tree.handleServiceToggle}
            pastRuns={pastRuns}
          />
        )}
      </main>
    </div>
  );
}
