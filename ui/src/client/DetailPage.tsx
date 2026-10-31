import { useCallback, useMemo, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router';
import PageHeader from './PageHeader';
import { ReactFlowProvider } from '@xyflow/react';
import { RunSummary, ScheduleGraph } from './types';
import {
  DetailMode,
  isRerunnableStatus,
  isTerminalStatus,
  pillClass,
  resolveActiveGraph,
  headerDriftLabel,
  groupAttemptsByTask,
  runFailureLabel,
  graphBadgeLabel as graphBadgeLabelOf,
  graphCardState as graphCardStateOf,
  runTabState,
  driftLabelOf,
} from './detail-page-helpers';
import { buildServiceColors, listServices, serviceOfNode } from './service-helpers';
import { useLastRun } from './detail-page/useLastRun';
import { useRunRows } from './detail-page/useRunRows';
import { useScheduleGraph } from './detail-page/useScheduleGraph';
import { useSelectedRunGraph } from './detail-page/useSelectedRunGraph';
import { RUNS_PAGE_SIZE, useScheduleRuns } from './detail-page/useScheduleRuns';
import { settlesRequest, useActionRequestId, type ActionRequestId } from './request-id';
import DAGPanel from './DAGPanel';
import NodeTypeIcon from './NodeTypeIcon';
import PastRunsPanel from './PastRunsPanel';
import RunProgressHeader from './RunProgressHeader';
import RunNodeTable from './RunNodeTable';
import RunSwimlane from './RunSwimlane';
import RerunFailedModal from './RerunFailedModal';
import Tabs, { useActiveTab } from './Tabs';

// Adds or removes a key from a Set without mutating the original — used for the
// swimlane's per-lane collapse state, which is toggled by lane label clicks.
function toggleSet(set: Set<string>, key: string): Set<string> {
  const next = new Set(set);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}

function formatStatusLabel(status: string | null | undefined): string {
  if (!status) return 'pending';
  return status.replace(/_/g, ' ');
}

function formatDate(value: string | null | undefined): string {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

interface DetailPageProps {
  mode?: DetailMode;
}

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
  const { take, renew } = useActionRequestId();
  return <ScheduleDetail key={name} name={name} mode={mode} takeRequestId={take} renewTriggerId={renew} />;
}

function ScheduleDetail({ name, mode, takeRequestId, renewTriggerId }: {
  name: string | undefined;
  mode: DetailMode;
  takeRequestId: ActionRequestId['take'];
  renewTriggerId: ActionRequestId['renew'];
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
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [expandedServices, setExpandedServices] = useState<Set<string>>(new Set());
  const [rerunFailedState, setRerunFailedState] = useState<'idle' | 'success'>('idle');
  // The trigger button's request id: a press that gets no answer and is
  // repeated for the same schedule, while the page still shows the last run
  // the first press saw, sends the same id, so the run the first press made is
  // returned rather than made twice. The page reads the schedule's last run
  // when it arrives at the schedule; a newer one read then makes the next
  // press a new action, and the id is renewed once the server has given its
  // final answer. While that read is loading or after it failed, the page
  // does not know the last run (lastRunObservation is undefined).
  const [triggerState, setTriggerState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [triggerError, setTriggerError] = useState<string | null>(null);
  const [rerunModalOpen, setRerunModalOpen] = useState(false);
  const [runView, setRunView] = useState<'list' | 'graph'>('list');
  const [laneCollapsed, setLaneCollapsed] = useState<Set<string>>(new Set());

  // The rerun-failed dialog posts the rerun or the rebase itself; once one is
  // accepted the dialog closes and the page's button shows the success.
  const handleRerunFailedSubmitted = useCallback(() => {
    setRerunModalOpen(false);
    setRerunFailedState('success');
    setTimeout(() => setRerunFailedState('idle'), 3000);
  }, []);

  const handleTriggerRun = useCallback(async () => {
    if (!name) return;
    setTriggerState('loading');
    setTriggerError(null);
    try {
      const res = await fetch(`/api/schedules/${encodeURIComponent(name)}/trigger`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: takeRequestId(name, lastRunObservation) }),
      });
      if (settlesRequest(res)) renewTriggerId();
      if (res.ok) {
        setTriggerState('success');
        setTimeout(() => setTriggerState('idle'), 3000);
      } else {
        const body = await res.json().catch(() => ({ error: 'Request failed — please try again' }));
        setTriggerError(body.error ?? 'Request failed — please try again');
        setTriggerState('error');
      }
    } catch {
      setTriggerError('Request failed — please try again');
      setTriggerState('error');
    }
  }, [name, lastRunObservation, takeRequestId, renewTriggerId]);

  const attemptsByTask = useMemo(() => groupAttemptsByTask(attempts), [attempts]);

  const selectedRun = runs.find((run) => run.run_id === selectedRunId)
    ?? (pinnedRun && pinnedRun.run_id === selectedRunId ? pinnedRun : null);
  const activeGraph: ScheduleGraph | null = resolveActiveGraph({
    mode, scheduleGraph: graph, liveRunGraph, selectedRunGraph: runGraph, selectedRunId,
  });

  // The Run tab summarises a run's task set; when there is nothing yet to
  // summarise it must not render RunProgressHeader over an empty list, which
  // would read as a fake "0 of 0 nodes complete / 0%". A schedule that has
  // never run shows "No runs yet."; a selected historical run whose snapshot
  // is still loading, or a live run whose tasks are still arriving, shows
  // "Loading run…". Only a non-empty task set renders the header and views.
  const runTab = runTabState({
    lastRunId, runsCount, taskCount: tasks.length, selectedRunId, runGraph,
  });

  // Service-level grouping shared by the graph and the nodes table. A graph
  // with a single service always renders expanded — a lone service vertex
  // would carry no information.
  const services = useMemo(
    () => listServices(activeGraph?.nodes ?? [], tasks),
    [activeGraph, tasks],
  );
  const serviceColors = useMemo(() => buildServiceColors(services), [services]);
  const effectiveExpandedServices = useMemo(
    () => (services.length <= 1 ? new Set(services) : expandedServices),
    [services, expandedServices],
  );

  // Selecting a node (graph click, table click, or search) also expands its
  // service so the selection is visible in both panels.
  const handleNodeSelect = useCallback((nodeId: string | null) => {
    setSelectedNodeId(nodeId);
    if (nodeId) {
      const svc = serviceOfNode(nodeId);
      setExpandedServices((prev) => (prev.has(svc) ? prev : new Set([...prev, svc])));
    }
  }, []);

  const handleServiceToggle = useCallback((service: string) => {
    const isCollapsing = expandedServices.has(service);
    setExpandedServices((prev) => {
      const next = new Set(prev);
      if (next.has(service)) next.delete(service);
      else next.add(service);
      return next;
    });
    if (isCollapsing) {
      // Collapsing hides the service's nodes; a selection inside it would
      // point at a node no longer on screen.
      setSelectedNodeId((sel) => (sel && serviceOfNode(sel) === service ? null : sel));
    }
  }, [expandedServices]);
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
  const graphEmptyMessage = selectedRunId
    ? 'No DAG snapshot is available for this run.'
    : lastRunId
      ? 'The run is active, but its DAG snapshot is not available yet.'
      : 'This schedule does not have a dependency graph yet.';
  const graphErrorMessage = selectedRunId
    ? 'Failed to load the historical DAG snapshot.'
    : 'Failed to load the dependency graph.';
  const shouldRenderGraph = !!activeGraph && activeGraph.nodes.length > 0;
  const graphCardState = graphCardStateOf({ selectedRunId, runGraph, shouldRenderGraph, graphState });
  const graphBadgeLabel = graphBadgeLabelOf({ mode, selectedRun, liveRunExists, activeGraph });
  const graphBadgeClass =
    graphBadgeLabel === 'snapshot'
      ? `pill-sm ${pillClass(snapshotRun?.status).replace('pill', 'pill-sm')}`
      : graphBadgeLabel === 'live'
        ? 'pill-sm pill-sm--current'
        : 'pill-sm pill-sm--pending';

  const legendParentIds = selectedNodeId && activeGraph
    ? new Set(activeGraph.edges.filter((e) => e.to_node_id === selectedNodeId).map((e) => e.from_node_id))
    : new Set<string>();

  const selectedNodeType = selectedNodeId && activeGraph
    ? activeGraph.nodes.find((n) => n.node_id === selectedNodeId)?.node_type ?? ''
    : '';

  const legendChildIds = selectedNodeId && activeGraph
    ? new Set(activeGraph.edges.filter((e) => e.from_node_id === selectedNodeId).map((e) => e.to_node_id))
    : new Set<string>();

  const handleSelectRun = (runId: string | null) => {
    if (runId !== selectedRunId) clearRows();
    setSelectedRunId(runId);
    setPinnedRun(runs.find((run) => run.run_id === runId) ?? null);
    setSelectedNodeId(null);
    setExpandedServices(new Set());
  };

  const driftLabel = driftLabelOf(liveRunGraph);

  const showRerunFailed = isRerunnableStatus(scheduler?.status) && Boolean(lastRunId);

  const triggerBtnClass = [
    'btn', 'btn--secondary',
    triggerState === 'loading' ? 'is-loading' : '',
    triggerState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');

  const rerunFailedBtnClass = [
    'btn', 'btn--secondary',
    rerunFailedState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');

  const triggerLabel =
    triggerState === 'loading' ? 'Triggering…' :
    triggerState === 'success' ? 'Triggered' :
    '▶ Trigger run';

  const rerunFailedLabel = rerunFailedState === 'success' ? 'Reran' : '↺ Rerun failed';

  const isLatest = mode === 'latest';
  // Full-width tabs for a run view: status first (Run), then structure
  // (Topology), then history (Past runs). Latest mode keeps its own layout and
  // does not use these.
  const pageTabSpecs = [
    { slug: 'run', label: 'Run' },
    { slug: 'runs', label: 'Past runs', count: runsCount },
  ];
  const activePage = useActiveTab('panel', 'run', pageTabSpecs.map(t => t.slug));

  return (
    <div className="page">
      <PageHeader>
        <button className="detail-back-link" onClick={() => navigate('/')}>
          ← Back
        </button>
        <div className="detail-scheduler-name">{name ?? 'Loading…'}</div>
        <span className={`pill ${pillClass(selectedRun ? selectedRun.status : schedulerStatus)}`}>
          {selectedRun ? formatStatusLabel(selectedRun.status) : formatStatusLabel(schedulerStatus)}
        </span>
        {runFailureLabel(selectedRun ? selectedRun.failure_reason : scheduler?.failure_reason) && (
          <span className="info-strip info-strip--error info-strip--inline" aria-label="run failure reason">
            {runFailureLabel(selectedRun ? selectedRun.failure_reason : scheduler?.failure_reason)}
          </span>
        )}
        {headerDriftLabel({ mode, driftLabel }) && (
          <span className="info-strip info-strip--warning info-strip--inline">
            <span className="info-strip__icon">⚠</span>
            {driftLabel}
          </span>
        )}
        {mode === 'latest' && promotionSeq > 0 && (
          <span className="info-strip info-strip--info info-strip--inline" aria-label="live promotion seq">
            <span className="info-strip__icon">ⓘ</span>
            promotion {promotionSeq}
          </span>
        )}
      </PageHeader>

      <main className="page-content">
      {(name || showRerunFailed) && (
        <div className="page-action-row">
          {name && (
            <button
              type="button"
              className={triggerBtnClass}
              disabled={liveRunExists || triggerState === 'loading' || triggerState === 'success'}
              onClick={handleTriggerRun}
              title={liveRunExists ? 'A run is already active' : 'Trigger a full DAG run'}
            >
              {triggerLabel}
            </button>
          )}
          {showRerunFailed && (
            <button
              type="button"
              className={rerunFailedBtnClass}
              disabled={rerunFailedState === 'success'}
              onClick={() => setRerunModalOpen(true)}
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

      {rerunModalOpen && lastRunId && (
        <RerunFailedModal
          runId={lastRunId}
          driftLabel={driftLabel}
          serviceColors={serviceColors}
          onClose={() => setRerunModalOpen(false)}
          onSubmitted={handleRerunFailedSubmitted}
        />
      )}

      {snapshotRun && (
        <div className="info-strip info-strip--warning">
          <span className="info-strip__icon">⚠</span>
          <span>
            Viewing snapshot from {formatDate(snapshotRun.completed_at ?? snapshotRun.created_at)}.
          </span>
          <span>Status: {formatStatusLabel(snapshotRun.status)}</span>
          <button
            type="button"
            className="btn btn--secondary info-strip__action"
            onClick={() => handleSelectRun(null)}
          >
            {liveRunExists ? 'Return to live run' : 'Back to latest run'}
          </button>
        </div>
      )}

      {isLatest ? (
        // Latest mode is a topology-catalog view: the schedule's current
        // dependency graph on the left and its run history on the right, with
        // no live run status to summarise.
        <div className="detail-layout">
          <section className="detail-card detail-graph-card">
            <div className="detail-card-header">
              Dependency Graph
              <span className={graphBadgeClass}>{graphBadgeLabel}</span>
            </div>
            <div className="dag-card-body">
              {graphCardState === 'ready' && activeGraph ? (
                <>
                  <ReactFlowProvider>
                    <DAGPanel
                      graphNodes={activeGraph.nodes}
                      graphEdges={activeGraph.edges}
                      tasks={[]}
                      selectedNodeId={selectedNodeId}
                      onNodeClick={handleNodeSelect}
                      colorByStatus={false}
                      serviceView={false}
                      expandedServices={effectiveExpandedServices}
                      onServiceClick={handleServiceToggle}
                      serviceColors={serviceColors}
                    />
                  </ReactFlowProvider>
                  {selectedNodeId && !selectedRunId && lastRunId && (
                    <div className="dag-focus-legend">
                      <div className="dag-focus-legend-title">
                        <NodeTypeIcon nodeType={selectedNodeType} size={12} />
                        {selectedNodeId.split('.').pop()}
                      </div>
                      <div className="dag-focus-legend-row">
                        <div className="dag-focus-dot dag-focus-dot--selected" /> Selected
                      </div>
                      <div className="dag-focus-legend-row">
                        <div className="dag-focus-dot dag-focus-dot--parent" />
                        Depends on ({legendParentIds.size})
                      </div>
                      <div className="dag-focus-legend-row">
                        <div className="dag-focus-dot dag-focus-dot--child" />
                        Required by ({legendChildIds.size})
                      </div>
                      <div className="dag-focus-legend-row">
                        <div className="dag-focus-dot dag-focus-dot--dim" /> Unrelated
                      </div>
                      {selectedNodeId && name && (
                        <a
                          className="dag-focus-open-link"
                          href={`/node/${encodeURIComponent(selectedNodeId)}`}
                          onClick={e => {
                            e.preventDefault();
                            navigate(
                              `/node/${encodeURIComponent(selectedNodeId)}`,
                              { state: { from: { type: 'schedule', name, mode: 'latest' } } },
                            );
                          }}
                        >
                          Open node detail →
                        </a>
                      )}
                    </div>
                  )}
                </>
              ) : graphCardState === 'error' ? (
                <div className="graph-empty-state">
                  <p className="graph-empty-title">Graph unavailable</p>
                  <p className="graph-empty-copy">{graphErrorMessage}</p>
                </div>
              ) : graphCardState === 'empty' ? (
                <div className="graph-empty-state">
                  <p className="graph-empty-title">No DAG to display</p>
                  <p className="graph-empty-copy">{graphEmptyMessage}</p>
                </div>
              ) : (
                <div className="graph-empty-state">
                  <p className="graph-empty-title">Loading graph</p>
                  <p className="graph-empty-copy">
                    {selectedRunId ? 'Fetching the historical run snapshot…' : 'Fetching the dependency graph…'}
                  </p>
                </div>
              )}
            </div>
          </section>

          <div className="detail-right-col">
            <section className="detail-card">
              <div className="section-header">
                <div className="section-header__main">
                  <span className="section-header__title">Past Runs</span>
                  <span className="section-header__count">{runsCount}</span>
                </div>
              </div>
              <PastRunsPanel
                runs={runs}
                liveRunId={liveRunExists ? lastRunId : null}
                liveStatus={liveRunExists ? formatStatusLabel(liveSchedulerStatus) : null}
                selectedRunId={selectedRunId}
                onSelectRun={handleSelectRun}
                page={runsPage}
                pageSize={RUNS_PAGE_SIZE}
                total={runsCount}
                onPageChange={setRunsPage}
              />
            </section>
          </div>
        </div>
      ) : (
        <div className="detail-run">
          <Tabs variant="page" param="panel" defaultSlug="run" tabs={pageTabSpecs} />
          {activePage === 'run' && (
            <div className="run-view">
              {runTab === 'no-runs' ? (
                <div className="graph-empty-state">
                  <p className="graph-empty-title">No runs yet.</p>
                  <p className="graph-empty-copy">This schedule has not run.</p>
                </div>
              ) : runTab === 'loading' ? (
                <div className="graph-empty-state">
                  <p className="graph-empty-title">Loading run…</p>
                  <p className="graph-empty-copy">
                    {selectedRunId ? 'Fetching the historical run snapshot…' : 'Fetching the latest run…'}
                  </p>
                </div>
              ) : (
                <>
                  <div className="seg-toggle">
                    <button className={runView === 'list' ? 'on' : ''} onClick={() => setRunView('list')}>List</button>
                    <button className={runView === 'graph' ? 'on' : ''} onClick={() => setRunView('graph')}>Graph</button>
                  </div>
                  <RunProgressHeader tasks={tasks} />
                  {runView === 'list' ? (
                    <RunNodeTable
                      tasks={tasks}
                      attemptsByTask={attemptsByTask}
                      edges={activeGraph?.edges ?? []}
                      expandedServices={effectiveExpandedServices}
                      onServiceToggle={handleServiceToggle}
                    />
                  ) : (
                    <RunSwimlane
                      graph={activeGraph ?? { nodes: [], edges: [] }}
                      tasks={tasks}
                      serviceOrder={services}
                      collapsed={laneCollapsed}
                      onLaneToggle={(s) => setLaneCollapsed((prev) => toggleSet(prev, s))}
                    />
                  )}
                </>
              )}
            </div>
          )}
          {activePage === 'runs' && (
            <PastRunsPanel
              runs={runs}
              liveRunId={liveRunExists ? lastRunId : null}
              liveStatus={liveRunExists ? formatStatusLabel(liveSchedulerStatus) : null}
              selectedRunId={selectedRunId}
              onSelectRun={handleSelectRun}
              page={runsPage}
              pageSize={RUNS_PAGE_SIZE}
              total={runsCount}
              onPageChange={setRunsPage}
            />
          )}
        </div>
      )}
      </main>
    </div>
  );
}
