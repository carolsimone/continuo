import { useCallback, useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router';
import { ReactFlowProvider } from '@xyflow/react';
import {
  RunGraph,
  RunSummary,
  Scheduler,
  ScheduleGraph,
  ScheduleSummary,
  Task,
  TaskExecution,
} from './types';
import { resolveActiveGraph, headerDriftLabel } from './detail-page-helpers';
import { buildServiceColors, listServices, serviceOfNode } from './service-helpers';
import { fetchAllPages } from './fetch-all-pages';
import { getDriftState, getDriftBadge } from './drift-helpers';
import DAGPanel from './DAGPanel';
import NodeTypeIcon from './NodeTypeIcon';
import NodesPanel from './NodesPanel';
import PastRunsPanel from './PastRunsPanel';
import RerunFailedModal, { RerunFailedMode } from './RerunFailedModal';
import Tabs, { useActiveTab } from './Tabs';

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

function deriveHistoricalTasks(runGraph: RunGraph | null): Task[] {
  if (!runGraph) return [];
  return runGraph.nodes.map((node) => {
    const [service_name = '', schema_name = '', table_name = ''] = node.node_id.split('.');
    return {
      task_id: node.node_id,
      service_name,
      schema_name,
      table_name,
      job_name: '',
      status: normalizeStatus(node.status) || 'pending',
      retry_count: 0,
      max_retries: 0,
      created_at: null,
    };
  });
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
  const [topologyGeneration, setTopologyGeneration] = useState<number>(0);
  const [executions, setExecutions] = useState<TaskExecution[]>([]);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [liveRunGraph, setLiveRunGraph] = useState<RunGraph | null>(null);
  const [runGraph, setRunGraph] = useState<RunGraph | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [expandedServices, setExpandedServices] = useState<Set<string>>(new Set());
  const [graphState, setGraphState] = useState<'loading' | 'ready' | 'empty' | 'error'>('loading');
  const [rerunState, setRerunState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [rerunError, setRerunError] = useState<string | null>(null);
  const [rebaseState, setRebaseState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [rebaseError, setRebaseError] = useState<string | null>(null);
  const [triggerState, setTriggerState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [triggerError, setTriggerError] = useState<string | null>(null);
  const [rerunModalOpen, setRerunModalOpen] = useState(false);

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
    setExecutions([]);
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
          setTopologyGeneration(Number(data.topology_generation ?? 0));
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

  useEffect(() => {
    if (!lastRunId) return;
    if (selectedRunId) return;
    if (mode === 'latest') return;
    let cancelled = false;
    let timer: number | undefined;
    const controller = new AbortController();

    const fetchDynamic = () => {
      const schedulerRequest = fetch(`/api/schedulers/${lastRunId}`)
        .then((response) => response.json())
        .then((data: { scheduler?: Scheduler } | Scheduler) => {
          const nextScheduler = extractScheduler(data);
          if (!cancelled) setScheduler(nextScheduler);
          return nextScheduler;
        })
        .catch(() => {
          return null;
        });

      fetchAllPages<Task>(`/api/schedulers/${lastRunId}/tasks`, 'tasks', {
        signal: controller.signal,
      })
        .then((all) => {
          if (!cancelled) setTasks(all);
        })
        .catch(() => {
          if (!cancelled) setTasks([]);
        });

      fetchAllPages<TaskExecution>(`/api/schedulers/${lastRunId}/executions`, 'executions', {
        signal: controller.signal,
      })
        .then((all) => {
          if (!cancelled) setExecutions(all);
        })
        .catch(() => {
          if (!cancelled) setExecutions([]);
        });

      return schedulerRequest;
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

  const handleRerunRun = useCallback(async () => {
    if (!lastRunId) return;
    setRerunState('loading');
    setRerunError(null);
    try {
      const res = await fetch(`/api/schedulers/${lastRunId}/rerun`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      });
      if (res.ok) {
        setRerunState('success');
        setTimeout(() => setRerunState('idle'), 3000);
      } else {
        const body = await res.json().catch(() => ({ error: 'Request failed — please try again' }));
        setRerunError(body.error ?? 'Request failed — please try again');
        setRerunState('error');
      }
    } catch {
      setRerunError('Request failed — please try again');
      setRerunState('error');
    }
  }, [lastRunId]);

  const handleRebaseRun = useCallback(async () => {
    if (!lastRunId) return;
    setRebaseState('loading');
    setRebaseError(null);
    try {
      const res = await fetch(`/api/schedulers/${lastRunId}/rebase`, { method: 'POST' });
      if (res.ok) {
        setRebaseState('success');
        setTimeout(() => setRebaseState('idle'), 3000);
      } else {
        const body = await res.json().catch(() => ({ error: 'Request failed — please try again' }));
        setRebaseError(body.error ?? 'Request failed — please try again');
        setRebaseState('error');
      }
    } catch {
      setRebaseError('Request failed — please try again');
      setRebaseState('error');
    }
  }, [lastRunId]);

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

  const latestExecutions = Array.from(
    executions.reduce((map, execution) => {
      const existing = map.get(execution.task_id);
      if (!existing || (execution.started_at ?? '') > (existing.started_at ?? '')) {
        map.set(execution.task_id, execution);
      }
      return map;
    }, new Map<string, TaskExecution>()).values(),
  );

  const selectedRun = runs.find((run) => run.run_id === selectedRunId) ?? null;
  const activeGraph: ScheduleGraph | null = resolveActiveGraph({
    mode, scheduleGraph: graph, liveRunGraph, selectedRunGraph: runGraph, selectedRunId,
  });
  const activeTasks = selectedRunId ? deriveHistoricalTasks(runGraph) : tasks;

  // Service-level grouping shared by the graph and the nodes table. A graph
  // with a single service always renders expanded — a lone service vertex
  // would carry no information.
  const services = useMemo(
    () => listServices(activeGraph?.nodes ?? [], activeTasks),
    [activeGraph, activeTasks],
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

  const handleSelectRun = (runId: string | null) => {
    setSelectedRunId(runId);
    setSelectedNodeId(null);
    setExpandedServices(new Set());
  };

  const driftLabel: string | null = (() => {
    if (!liveRunGraph) return null;
    const state = getDriftState(liveRunGraph.run_topology_generation, liveRunGraph.latest_topology_generation);
    if (state === 'fresh') return null;
    return getDriftBadge(
      state,
      Number(liveRunGraph.run_topology_generation ?? 0),
      Number(liveRunGraph.latest_topology_generation ?? 0),
    );
  })();

  const showRerunFailed = isRerunnableStatus(scheduler?.status) && Boolean(lastRunId);

  const handleRerunFailedSubmit = async (rerunMode: RerunFailedMode) => {
    setRerunModalOpen(false);
    if (rerunMode === 'this') {
      await handleRerunRun();
    } else {
      await handleRebaseRun();
    }
  };

  const triggerBtnClass = [
    'btn', 'btn--secondary',
    triggerState === 'loading' ? 'is-loading' : '',
    triggerState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');

  const rerunFailedBtnClass = [
    'btn', 'btn--secondary',
    (rerunState === 'loading' || rebaseState === 'loading') ? 'is-loading' : '',
    (rerunState === 'success' || rebaseState === 'success') ? 'is-success' : '',
  ].filter(Boolean).join(' ');

  const triggerLabel =
    triggerState === 'loading' ? 'Triggering…' :
    triggerState === 'success' ? 'Triggered' :
    '▶ Trigger run';

  const rerunFailedLabel =
    (rerunState === 'loading' || rebaseState === 'loading') ? 'Triggering…' :
    (rerunState === 'success' || rebaseState === 'success') ? 'Reran' :
    '↺ Rerun failed';

  const submittingRerun = rerunState === 'loading' || rebaseState === 'loading';

  const isLatest = mode === 'latest';
  const panelSpecs = isLatest
    ? []
    : [
        { slug: 'nodes', label: 'Nodes', count: activeTasks.length },
        { slug: 'runs', label: 'Past Runs', count: runs.length },
      ];
  const activePanel = useActiveTab('panel', 'nodes', panelSpecs.map(t => t.slug));

  return (
    <div className="page">
      <header className="page-header">
        <button className="detail-back-link" onClick={() => navigate('/')}>
          ← Back
        </button>
        <div className="detail-scheduler-name">{name ?? 'Loading…'}</div>
        <span className={`pill ${pillClass(selectedRun ? selectedRun.terminal_status : schedulerStatus)}`}>
          {selectedRun ? formatStatusLabel(selectedRun.terminal_status) : formatStatusLabel(schedulerStatus)}
        </span>
        {headerDriftLabel({ mode, driftLabel }) && (
          <span className="info-strip info-strip--warning info-strip--inline">
            <span className="info-strip__icon">⚠</span>
            {driftLabel}
          </span>
        )}
        {mode === 'latest' && topologyGeneration > 0 && (
          <span className="info-strip info-strip--info info-strip--inline" aria-label="topology generation">
            <span className="info-strip__icon">ⓘ</span>
            topology v{topologyGeneration}
          </span>
        )}
      </header>

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
              disabled={submittingRerun || rerunState === 'success' || rebaseState === 'success'}
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
      {rerunState === 'error' && rerunError && (
        <div className="info-strip info-strip--error">{rerunError}</div>
      )}
      {rebaseState === 'error' && rebaseError && (
        <div className="info-strip info-strip--error">{rebaseError}</div>
      )}

      {rerunModalOpen && (
        <RerunFailedModal
          driftLabel={driftLabel}
          submitting={submittingRerun}
          onClose={() => setRerunModalOpen(false)}
          onSubmit={handleRerunFailedSubmit}
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
                    tasks={isLatest ? [] : activeTasks}
                    selectedNodeId={selectedNodeId}
                    onNodeClick={handleNodeSelect}
                    colorByStatus={!isLatest}
                    serviceView={!isLatest}
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
                            { state: { from: { type: 'schedule', name, mode: mode === 'latest' ? 'latest' : 'run' } } },
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
            {isLatest ? (
              <>
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
              </>
            ) : (
              <>
                <Tabs variant="panel" param="panel" defaultSlug="nodes" tabs={panelSpecs} />
                {activePanel === 'nodes' && (
                  <div className="nodes-table-scroll">
                    {selectedRunId && !runGraph ? (
                      <p className="empty">Loading node snapshot…</p>
                    ) : lastRunId === null && runs.length === 0 && activeTasks.length === 0 ? (
                      <p className="empty">No runs yet.</p>
                    ) : (
                      <NodesPanel
                        tasks={activeTasks}
                        executions={selectedRunId ? [] : latestExecutions}
                        selectedNodeId={selectedNodeId}
                        onNodeSelect={handleNodeSelect}
                        expandedServices={effectiveExpandedServices}
                        onServiceToggle={handleServiceToggle}
                        serviceColors={serviceColors}
                      />
                    )}
                  </div>
                )}
                {activePanel === 'runs' && (
                  <PastRunsPanel
                    runs={runs}
                    liveRunId={liveRunExists ? lastRunId : null}
                    liveStatus={liveRunExists ? formatStatusLabel(liveSchedulerStatus) : null}
                    selectedRunId={selectedRunId}
                    onSelectRun={handleSelectRun}
                  />
                )}
              </>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}
