import { GraphNode, RunGraph, ScheduleGraph, Task, TaskAttempt } from './types';
import { formatDuration } from './node-helpers';

export function taskNodeId(task: Task): string {
  return `${task.service_name}.${task.schema_name}.${task.table_name}`;
}

export function toScheduleGraph(runGraph: RunGraph | null): ScheduleGraph | null {
  if (!runGraph) return null;

  return {
    nodes: runGraph.nodes.map((node) => ({
      node_id: node.node_id,
      node_type: node.node_type,
      schedule_name: node.schedule_name,
      status: node.status,
    })),
    edges: runGraph.edges,
  };
}

export function resolveNodeStatus(node: GraphNode, tasks: Task[]): string {
  const task = tasks.find((candidate) => taskNodeId(candidate) === node.node_id);
  return task?.status ?? node.status ?? 'external';
}

interface ResolveActiveGraphArgs {
  mode?: 'run' | 'latest';
  scheduleGraph: ScheduleGraph | null;
  liveRunGraph: RunGraph | null;
  selectedRunGraph: RunGraph | null;
  selectedRunId: string | null;
}

export function resolveActiveGraph({
  mode,
  scheduleGraph,
  liveRunGraph,
  selectedRunGraph,
  selectedRunId,
}: ResolveActiveGraphArgs): ScheduleGraph | null {
  if (mode === 'latest') return scheduleGraph;
  if (selectedRunId) {
    return toScheduleGraph(selectedRunGraph);
  }

  return toScheduleGraph(liveRunGraph) ?? scheduleGraph;
}

interface HeaderDriftLabelArgs {
  mode?: 'run' | 'latest';
  driftLabel: string | null;
}

export function headerDriftLabel({ mode, driftLabel }: HeaderDriftLabelArgs): string | null {
  if (mode === 'latest') return null;
  return driftLabel;
}

export function parseNodeId(nodeId: string): {
  service_name: string;
  schema_name: string;
  table_name: string;
} {
  const [service_name = '', schema_name = '', table_name = ''] = nodeId.split('.');
  return { service_name, schema_name, table_name };
}

// groupAttemptsByTask groups a run's attempts per task, each task's list in
// attempt order. A row repeated across two page reads (offset paging over a
// set that grew in between shifts later rows down by one) appears once: the
// later copy is the fresher read and replaces the earlier.
export function groupAttemptsByTask(attempts: TaskAttempt[]): Map<string, TaskAttempt[]> {
  const byTask = new Map<string, Map<number, TaskAttempt>>();
  for (const a of attempts) {
    let byNumber = byTask.get(a.task_id);
    if (!byNumber) {
      byNumber = new Map();
      byTask.set(a.task_id, byNumber);
    }
    byNumber.set(a.attempt, a);
  }
  const grouped = new Map<string, TaskAttempt[]>();
  for (const [taskId, byNumber] of byTask) {
    grouped.set(taskId, [...byNumber.values()].sort((x, y) => x.attempt - y.attempt));
  }
  return grouped;
}

// latestAttempt is a task's highest-numbered attempt, from a list in attempt
// order, or undefined for a task without attempts.
export function latestAttempt(attempts: TaskAttempt[] | undefined): TaskAttempt | undefined {
  return attempts && attempts.length > 0 ? attempts[attempts.length - 1] : undefined;
}

// attemptPillClass maps a task attempt's status to a .pill-sm variant. An
// attempt that is dispatched or queued has not started yet, so both read as
// pending; running, succeeded, failed and cancelled have pills of their own.
export function attemptPillClass(status: string): string {
  switch (status) {
    case 'running':   return 'pill-sm--running';
    case 'succeeded': return 'pill-sm--succeeded';
    case 'failed':    return 'pill-sm--failed';
    case 'cancelled': return 'pill-sm--cancelled';
  }
  return 'pill-sm--pending';
}

// attemptClassReason joins an attempt's failure class and reason, shows
// whichever one is known alone, and a dash when neither is.
export function attemptClassReason(a: TaskAttempt): string {
  const parts = [a.failure_class, a.failure_reason].filter((p): p is string => !!p);
  return parts.length > 0 ? parts.join(' · ') : '—';
}

// attemptWaitSeconds is how long an attempt waited between being queued and
// starting, or null when either time is unknown. The two times come from
// separate status reports; a start stamped before its queue time is clock
// skew and reads as no wait.
export function attemptWaitSeconds(a: TaskAttempt): number | null {
  if (!a.queued_at || !a.started_at) return null;
  const ms = new Date(a.started_at).getTime() - new Date(a.queued_at).getTime();
  if (Number.isNaN(ms)) return null;
  return Math.max(0, ms / 1000);
}

// formatAttemptSeconds renders a duration in whole seconds.
export function formatAttemptSeconds(sec: number | null): string {
  return sec === null ? '—' : formatDuration(Math.round(sec));
}
