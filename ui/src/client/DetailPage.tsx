import { useCallback, useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router';
import PageHeader from './PageHeader';
import { ReactFlowProvider } from '@xyflow/react';
import {
  RunGraph,
  RunSummary,
  Scheduler,
  ScheduleGraph,
  ScheduleSummary,
  Task,
  TaskAttempt,
} from './types';
import { resolveActiveGraph, headerDriftLabel, groupAttemptsByTask, runFailureLabel } from './detail-page-helpers';
import { buildServiceColors, listServices, serviceOfNode } from './service-helpers';
import { fetchAllPages } from './fetch-all-pages';
import { getDriftState, getDriftBadge } from './drift-helpers';
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

function initialLastRunId(locationState: unknown): string | null | undefined {
  if (locationState == null) return undefined;
  return ((locationState as { last_run_id?: string | null } | null)?.last_run_id ?? null);
}

function normalizeStatus(status: string | null | undefined): string {
  if (!status) return '';
  return status
    .toLowerCase()
    .replace(/^scheduler_status_/, '')
    .replace(/^task_status_/, '');
}

function formatStatusLabel(status: string | null | undefined): string {
  const normalized = normalizeStatus(status);
  if (!normalized) return 'pending';
  return normalized.replace(/_/g, ' ');
}

export function pillClass(status: string | null | undefined): string {
  const normalized = normalizeStatus(status);
  if (normalized.includes('succeed')) return 'pill--succeeded';
  if (normalized.includes('fail')) return 'pill--failed';
  if (normalized.includes('cancel')) return 'pill--cancelled';
  if (normalized.includes('skip')) return 'pill--skipped';
  if (normalized.includes('run') || normalized.includes('current')) return 'pill--running';
  return 'pill--pending';
}

function formatDate(value: string | null | undefined): string {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

export function isTerminalStatus(status: string | null | undefined): boolean {
  const normalized = normalizeStatus(status);
  return normalized.includes('succeed') || normalized.includes('fail')
    || normalized.includes('cancel') || normalized.includes('skip');
}

function extractScheduler(data: { scheduler?: Scheduler } | Scheduler): Scheduler | null {
  return 'status' in data ? data : data.scheduler ?? null;
}

export function isRerunnableStatus(status: string | null | undefined): boolean {
  if (!status) return false;
  const s = status.toLowerCase();
  return s.includes('fail') || s.includes('cancel');
}

interface DetailPageProps {
  mode?: 'run' | 'latest';
}

export default function DetailPage({ mode = 'run' }: DetailPageProps) {
  const { name } = useParams<{ name: string }>();
  const navigate = useNavigate();
  const location = useLocation();

  const [lastRunId, setLastRunId] = useState<string | null | undefined>(() => initialLastRunId(location.state));
  const [scheduler, setScheduler] = useState<Scheduler | null>(null);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [graph, setGraph] = useState<ScheduleGraph | null>(null);
  const [promotionSeq, setPromotionSeq] = useState<number>(0);
  const [attempts, setAttempts] = useState<TaskAttempt[]>([]);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [liveRunGraph, setLiveRunGraph] = useState<RunGraph | null>(null);
  const [runGraph, setRunGraph] = useState<RunGraph | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [expandedServices, setExpandedServices] = useState<Set<string>>(new Set());
  const [graphState, setGraphState] = useState<'loading' | 'ready' | 'empty' | 'error'>('loading');
  const [rerunFailedState, setRerunFailedState] = useState<'idle' | 'success'>('idle');
  const [triggerState, setTriggerState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [triggerError, setTriggerError] = useState<string | null>(null);
  const [rerunModalOpen, setRerunModalOpen] = useState(false);
  const [runView, setRunView] = useState<'list' | 'graph'>('list');
  const [laneCollapsed, setLaneCollapsed] = useState<Set<string>>(new Set());

  // Arriving at a schedule resets every piece of page state and resolves the
  // run the page opens on. The dashboard card passes the schedule's last run
  // id as navigation state so that lookup can be skipped; without it the id
  // is read from the schedule list. Only the schedule name drives this effect:
  // a navigation that changes just the query string — a panel tab — writes a
  // new history entry with null state, and must not wipe the page.
  useEffect(() => {
    const hintedLastRunId = initialLastRunId(location.state);
    setLastRunId(hintedLastRunId);
    setScheduler(null);
    setTasks([]);
    setGraph(null);
    setAttempts([]);
    setRuns([]);
    setSelectedRunId(null);
    setLiveRunGraph(null);
    setRunGraph(null);
    setSelectedNodeId(null);
    setExpandedServices(new Set());
    setGraphState('loading');
    if (!name || hintedLastRunId !== undefined) return;

    let cancelled = false;
    fetch('/api/schedules')
      .then((response) => response.json())
      .then((data: { schedules: ScheduleSummary[] }) => {
        if (cancelled) return;
        const match = (data.schedules || []).find((schedule) => schedule.schedule_name === name);
        setLastRunId(match?.last_run_id ?? null);
      })
      .catch(() => {
        if (!cancelled) setLastRunId(null);
      });
    return () => { cancelled = true; };
  }, [name]);

  useEffect(() => {
    if (!name) return;
    let cancelled = false;

    const fetchGraph = () => {
      fetch(`/api/schedules/${name}/graph`)
        .then((response) => response.json())
        .then((data: ScheduleGraph) => {
          if (cancelled) return;
          setGraph(data);
          setGraphState(data.nodes.length > 0 ? 'ready' : 'empty');
          setPromotionSeq(Number(data.promotion_seq ?? 0));
        })
        .catch(() => {
          if (cancelled) return;
          setGraph(null);
          setGraphState('error');
        });
    };

    fetchGraph();
    if (mode === 'latest') {
      const id = setInterval(fetchGraph, 5000);
      return () => { cancelled = true; clearInterval(id); };
    }
    return () => { cancelled = true; };
  }, [name, mode]);

  useEffect(() => {
    if (!name) return;
    let cancelled = false;

    fetch(`/api/schedules/${name}/runs`)
      .then((response) => response.json())
      .then((data: { runs: RunSummary[] }) => {
        if (!cancelled) setRuns(data.runs || []);
      })
      .catch(() => {
        if (!cancelled) setRuns([]);
      });

    return () => {
      cancelled = true;
    };
  }, [name]);

  useEffect(() => {
    if (!lastRunId) return;
    let cancelled = false;

    fetch(`/api/schedulers/${lastRunId}`)
      .then((response) => response.json())
      .then((data: { scheduler?: Scheduler } | Scheduler) => {
        if (!cancelled) setScheduler(extractScheduler(data));
      })
      .catch(() => {
        if (!cancelled) setScheduler(null);
      });

    return () => {
      cancelled = true;
    };
  }, [lastRunId]);

  useEffect(() => {
    if (!lastRunId) return;
    let cancelled = false;

    fetch(`/api/runs/${lastRunId}/graph`)
      .then((response) => response.json())
      .then((data: RunGraph) => {
        if (!cancelled) setLiveRunGraph(data);
      })
      .catch(() => {
        if (!cancelled) setLiveRunGraph(null);
      });

    return () => {
      cancelled = true;
    };
  }, [lastRunId]);

  // The run the Run tab shows — a selected past run, else the latest — has
  // its tasks and every attempt of every task read from state. A past run is
  // read once; the latest run is re-read every 5 s until it is terminal. A
  // poll reads the rows alongside the run's status, so the poll that first
  // sees the run terminal may hold rows from before the run ended: once that
  // poll's reads have landed, the rows are read one last time.
  useEffect(() => {
    const shownRunId = selectedRunId ?? lastRunId;
    if (!shownRunId) return;
    if (mode === 'latest') return;
    let cancelled = false;
    let timer: number | undefined;
    const controller = new AbortController();

    // Resolves once both reads have landed (or failed) and been applied.
    const fetchRunRows = (): Promise<void> => {
      const taskRead = fetchAllPages<Task>(`/api/schedulers/${shownRunId}/tasks`, 'tasks', {
        signal: controller.signal,
      })
        .then((all) => {
          if (!cancelled) setTasks(all);
        })
        .catch(() => {
          if (!cancelled) setTasks([]);
        });

      const attemptRead = fetchAllPages<TaskAttempt>(`/api/schedulers/${shownRunId}/attempts`, 'attempts', {
        signal: controller.signal,
      })
        .then((all) => {
          if (!cancelled) setAttempts(all);
        })
        .catch(() => {
          if (!cancelled) setAttempts([]);
        });

      return Promise.all([taskRead, attemptRead]).then(() => undefined);
    };

    if (selectedRunId) {
      fetchRunRows();
      return () => {
        cancelled = true;
        controller.abort();
      };
    }

    const fetchDynamic = () => {
      const schedulerRequest = fetch(`/api/schedulers/${shownRunId}`)
        .then((response) => response.json())
        .then((data: { scheduler?: Scheduler } | Scheduler) => {
          const nextScheduler = extractScheduler(data);
          if (!cancelled) setScheduler(nextScheduler);
          return nextScheduler;
        })
        .catch(() => {
          return null;
        });

      // Resolves with the run's status once its row reads have landed too.
      return Promise.all([schedulerRequest, fetchRunRows()]).then(([nextScheduler]) => nextScheduler);
    };

    fetchDynamic().then((freshScheduler) => {
      if (cancelled) return;
      if (freshScheduler && !isTerminalStatus(freshScheduler.status)) {
        timer = window.setInterval(() => {
          fetchDynamic().then((nextScheduler) => {
            if (cancelled) return;
            if (timer && nextScheduler && isTerminalStatus(nextScheduler.status)) {
              window.clearInterval(timer);
              timer = undefined;
              // The run has ended: read the rows it ended with.
              fetchRunRows();
            }
          });
        }, 5000);
      }
    });

    return () => {
      cancelled = true;
      controller.abort();
      if (timer !== undefined) {
        window.clearInterval(timer);
      }
    };
  }, [lastRunId, selectedRunId, mode]);

  useEffect(() => {
    if (!selectedRunId) {
      setRunGraph(null);
      return;
    }

    let cancelled = false;
    setRunGraph(null);

    fetch(`/api/runs/${selectedRunId}/graph`)
      .then((response) => response.json())
      .then((data: RunGraph) => {
        if (!cancelled) setRunGraph(data);
      })
      .catch(() => {
        if (!cancelled) setRunGraph(null);
      });

    return () => {
      cancelled = true;
    };
  }, [selectedRunId]);

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
      const res = await fetch(`/api/schedules/${encodeURIComponent(name)}/trigger`, { method: 'POST' });
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
  }, [name]);

  const attemptsByTask = useMemo(() => groupAttemptsByTask(attempts), [attempts]);

  const selectedRun = runs.find((run) => run.run_id === selectedRunId) ?? null;
  const activeGraph: ScheduleGraph | null = resolveActiveGraph({
    mode, scheduleGraph: graph, liveRunGraph, selectedRunGraph: runGraph, selectedRunId,
  });

  // The Run tab summarises a run's task set; when there is nothing yet to
  // summarise it must not render RunProgressHeader over an empty list, which
  // would read as a fake "0 of 0 nodes complete / 0%". A schedule that has
  // never run shows "No runs yet."; a selected historical run whose snapshot
  // is still loading, or a live run whose tasks are still arriving, shows
  // "Loading run…". Only a non-empty task set renders the header and views.
  const runTabNoRuns = lastRunId === null && runs.length === 0 && tasks.length === 0;
  const runTabLoading = !runTabNoRuns
    && ((selectedRunId ? runGraph === null : lastRunId === undefined) || tasks.length === 0);

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
    normalizeStatus(scheduler?.status) ||
    (lastRunId === undefined ? 'loading' : lastRunId === null ? 'never run' : 'loading');
  const schedulerStatus = selectedRun
    ? normalizeStatus(selectedRun.terminal_status) || 'pending'
    : liveSchedulerStatus;
  const liveRunExists = !!lastRunId && !!scheduler && !isTerminalStatus(scheduler.status);
  const snapshotRun = selectedRunId
    ? selectedRun ?? { completed_at: null, created_at: null, terminal_status: null }
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
  const graphCardState = selectedRunId
    ? runGraph === null
      ? 'loading'
      : shouldRenderGraph
        ? 'ready'
        : 'empty'
    : shouldRenderGraph
      ? 'ready'
      : graphState;
  const graphBadgeLabel = mode === 'latest'
    ? 'catalog'
    : selectedRun
      ? 'snapshot'
      : liveRunExists
        ? 'live'
        : activeGraph && activeGraph.nodes.length > 0
          ? 'catalog'
          : 'idle';
  const graphBadgeClass =
    graphBadgeLabel === 'snapshot'
      ? `pill-sm ${pillClass(snapshotRun?.terminal_status).replace('pill', 'pill-sm')}`
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

  // The shown run's rows are cleared at once, so the newly chosen run never
  // renders over the previous run's tasks while its own are read.
  const handleSelectRun = (runId: string | null) => {
    if (runId !== selectedRunId) {
      setTasks([]);
      setAttempts([]);
    }
    setSelectedRunId(runId);
    setSelectedNodeId(null);
    setExpandedServices(new Set());
  };

  const driftLabel: string | null = (() => {
    if (!liveRunGraph) return null;
    const state = getDriftState(liveRunGraph.run_promotion_seq, liveRunGraph.latest_promotion_seq);
    // Only a stale run gets a label; 'unknown' (a run without a promotion
    // seq) and 'fresh' are silent.
    if (state !== 'stale') return null;
    return getDriftBadge(
      state,
      Number(liveRunGraph.run_promotion_seq ?? 0),
      Number(liveRunGraph.latest_promotion_seq ?? 0),
    );
  })();

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
    { slug: 'runs', label: 'Past runs', count: runs.length },
  ];
  const activePage = useActiveTab('panel', 'run', pageTabSpecs.map(t => t.slug));

  return (
    <div className="page">
      <PageHeader>
        <button className="detail-back-link" onClick={() => navigate('/')}>
          ← Back
        </button>
        <div className="detail-scheduler-name">{name ?? 'Loading…'}</div>
        <span className={`pill ${pillClass(selectedRun ? selectedRun.terminal_status : schedulerStatus)}`}>
          {selectedRun ? formatStatusLabel(selectedRun.terminal_status) : formatStatusLabel(schedulerStatus)}
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
          <span>Status: {formatStatusLabel(snapshotRun.terminal_status)}</span>
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
                  <span className="section-header__count">{runs.length}</span>
                </div>
              </div>
              <PastRunsPanel
                runs={runs}
                liveRunId={liveRunExists ? lastRunId : null}
                liveStatus={liveRunExists ? formatStatusLabel(liveSchedulerStatus) : null}
                selectedRunId={selectedRunId}
                onSelectRun={handleSelectRun}
              />
            </section>
          </div>
        </div>
      ) : (
        <div className="detail-run">
          <Tabs variant="page" param="panel" defaultSlug="run" tabs={pageTabSpecs} />
          {activePage === 'run' && (
            <div className="run-view">
              {runTabNoRuns ? (
                <div className="graph-empty-state">
                  <p className="graph-empty-title">No runs yet.</p>
                  <p className="graph-empty-copy">This schedule has not run.</p>
                </div>
              ) : runTabLoading ? (
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
            />
          )}
        </div>
      )}
      </main>
    </div>
  );
}
