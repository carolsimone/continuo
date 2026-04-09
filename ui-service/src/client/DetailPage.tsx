import { useEffect, useRef, useState } from 'react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
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
import { resolveActiveGraph } from './detail-page-helpers';
import DAGPanel from './DAGPanel';
import NodesPanel from './NodesPanel';
import PastRunsPanel from './PastRunsPanel';

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

function pillClass(status: string | null | undefined): string {
  const normalized = normalizeStatus(status);
  if (normalized.includes('succeed')) return 'pill--succeeded';
  if (normalized.includes('fail')) return 'pill--failed';
  if (normalized.includes('cancel')) return 'pill--cancelled';
  if (normalized.includes('run') || normalized.includes('current')) return 'pill--running';
  return 'pill--pending';
}

function formatDate(value: string | null | undefined): string {
  if (!value) return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function isTerminalStatus(status: string | null | undefined): boolean {
  const normalized = normalizeStatus(status);
  return normalized.includes('succeed') || normalized.includes('fail') || normalized.includes('cancel');
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

export default function DetailPage() {
  const { name } = useParams<{ name: string }>();
  const navigate = useNavigate();
  const location = useLocation();

  const [lastRunId, setLastRunId] = useState<string | null | undefined>(() => initialLastRunId(location.state));
  const resolvedRef = useRef(false);
  const [scheduler, setScheduler] = useState<Scheduler | null>(null);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [graph, setGraph] = useState<ScheduleGraph | null>(null);
  const [executions, setExecutions] = useState<TaskExecution[]>([]);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [liveRunGraph, setLiveRunGraph] = useState<RunGraph | null>(null);
  const [runGraph, setRunGraph] = useState<RunGraph | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [graphState, setGraphState] = useState<'loading' | 'ready' | 'empty' | 'error'>('loading');

  useEffect(() => {
    resolvedRef.current = false;
    setLastRunId(initialLastRunId(location.state));
    setScheduler(null);
    setTasks([]);
    setGraph(null);
    setExecutions([]);
    setRuns([]);
    setSelectedRunId(null);
    setLiveRunGraph(null);
    setRunGraph(null);
    setSelectedNodeId(null);
    setGraphState('loading');
  }, [location.state, name]);

  useEffect(() => {
    if (!name) return;
    if (resolvedRef.current) return;
    if (location.state != null) {
      resolvedRef.current = true;
      return;
    }

    resolvedRef.current = true;
    fetch('/api/schedules')
      .then((response) => response.json())
      .then((data: { schedules: ScheduleSummary[] }) => {
        const match = (data.schedules || []).find((schedule) => schedule.schedule_name === name);
        setLastRunId(match?.last_run_id ?? null);
      })
      .catch(() => {
        setLastRunId(null);
      });
  }, [location.state, name]);

  useEffect(() => {
    if (!name) return;
    let cancelled = false;

    fetch(`/api/schedules/${name}/graph`)
      .then((response) => response.json())
      .then((data: ScheduleGraph) => {
        if (!cancelled) {
          setGraph(data);
          setGraphState(data.nodes.length > 0 ? 'ready' : 'empty');
        }
      })
      .catch(() => {
        if (!cancelled) {
          setGraph(null);
          setGraphState('error');
        }
      });

    return () => {
      cancelled = true;
    };
  }, [name]);

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
    let cancelled = false;
    let timer: number | undefined;

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

      fetch(`/api/schedulers/${lastRunId}/tasks`)
        .then((response) => response.json())
        .then((data: { tasks: Task[] }) => {
          if (!cancelled) setTasks(data.tasks || []);
        })
        .catch(() => {
          if (!cancelled) setTasks([]);
        });

      fetch(`/api/schedulers/${lastRunId}/executions`)
        .then((response) => response.json())
        .then((data: { executions: TaskExecution[] }) => {
          if (!cancelled) setExecutions(data.executions || []);
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
      if (timer !== undefined) {
        window.clearInterval(timer);
      }
    };
  }, [lastRunId, selectedRunId]);

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
    scheduleGraph: graph,
    liveRunGraph,
    selectedRunGraph: runGraph,
    selectedRunId,
  });
  const activeTasks = selectedRunId ? deriveHistoricalTasks(runGraph) : tasks;
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
  const graphBadgeLabel = selectedRun
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

  const handleSelectRun = (runId: string | null) => {
    setSelectedRunId(runId);
    setSelectedNodeId(null);
  };

  return (
    <div className="detail-page">
      <div className="detail-topbar">
        <button className="detail-back-link" onClick={() => navigate('/')}>
          ← Back
        </button>
        <div className="detail-scheduler-name">{name ?? 'Loading…'}</div>
        <span className={`pill ${pillClass(selectedRun ? selectedRun.terminal_status : schedulerStatus)}`}>
          {selectedRun ? formatStatusLabel(selectedRun.terminal_status) : formatStatusLabel(schedulerStatus)}
        </span>
      </div>

      {snapshotRun && (
        <div className="detail-snapshot-banner">
          <span>
            Viewing snapshot from {formatDate(snapshotRun.completed_at ?? snapshotRun.created_at)}.
          </span>
          <span>Status: {formatStatusLabel(snapshotRun.terminal_status)}</span>
          <button type="button" onClick={() => handleSelectRun(null)}>
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
              <ReactFlowProvider>
                <DAGPanel
                  graphNodes={activeGraph.nodes}
                  graphEdges={activeGraph.edges}
                  tasks={activeTasks}
                  selectedNodeId={selectedNodeId}
                  onNodeClick={setSelectedNodeId}
                />
              </ReactFlowProvider>
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
          <section className="detail-card detail-nodes-card">
            <div className="detail-card-header">
              Nodes
              <span className={`pill-sm ${pillClass(schedulerStatus)}`}>{formatStatusLabel(schedulerStatus)}</span>
            </div>
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
                  onNodeSelect={setSelectedNodeId}
                />
              )}
            </div>
          </section>

          <section className="detail-card detail-runs-card">
            <div className="detail-card-header">Past Runs</div>
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
    </div>
  );
}
