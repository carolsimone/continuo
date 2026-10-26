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
      wait_reason: node.wait_reason,
      waiting_on_run_id: node.waiting_on_run_id,
      waiting_since: node.waiting_since,
    })),
    edges: runGraph.edges,
  };
}

// tasksByNodeId indexes a run's tasks by the graph node each one runs; when two
// tasks run one node the first is kept, as a search of the list finds it.
export function tasksByNodeId(tasks: Task[]): Map<string, Task> {
  const byNode = new Map<string, Task>();
  for (const task of tasks) {
    const id = taskNodeId(task);
    if (!byNode.has(id)) byNode.set(id, task);
  }
  return byNode;
}

// taskOfNode is the task that runs a graph node, from the run's tasks as a
// list or as indexed by tasksByNodeId.
function taskOfNode(node: GraphNode, tasks: Task[] | Map<string, Task>): Task | undefined {
  if (tasks instanceof Map) return tasks.get(node.node_id);
  return tasks.find((candidate) => taskNodeId(candidate) === node.node_id);
}

// resolveNodeStatus reads a graph node's status from its task when the run's
// tasks hold it, else from the graph. A caller resolving every node of a graph
// passes tasksByNodeId(tasks), so each node is one lookup.
export function resolveNodeStatus(node: GraphNode, tasks: Task[] | Map<string, Task>): string {
  return taskOfNode(node, tasks)?.status ?? node.status ?? 'external';
}

// LeaseWait is a pending task's wait for the write lease on its node:
// holderRunId is the run whose task writes the node now, null when no task
// holds it (the waiting task is about to be granted it), and since is when the
// wait began, null when unknown.
export interface LeaseWait {
  holderRunId: string | null;
  since: string | null;
}

interface WaitFields {
  wait_reason?: string | null;
  waiting_on_run_id?: string | null;
  waiting_since?: string | null;
}

// leaseWaitOf reads a task's (or a run graph node's) wait for its node's
// lease, or null when it waits for none.
export function leaseWaitOf(item: WaitFields | undefined): LeaseWait | null {
  if (item?.wait_reason !== 'node_lease') return null;
  return { holderRunId: item.waiting_on_run_id || null, since: item.waiting_since || null };
}

// resolveNodeWait reads a graph node's lease wait the way resolveNodeStatus
// reads its status: from the node's task when the run's tasks hold it, else
// from the run graph.
export function resolveNodeWait(node: GraphNode, tasks: Task[] | Map<string, Task>): LeaseWait | null {
  return leaseWaitOf(taskOfNode(node, tasks) ?? node);
}

// leaseWaitTitle is the hover title of a "waiting for node" pill: the run
// writing the node (its id's first 8 characters) and when the wait began.
export function leaseWaitTitle(wait: LeaseWait): string {
  const what = wait.holderRunId
    ? `waiting for run ${wait.holderRunId.slice(0, 8)} to finish writing this node`
    : 'waiting for its turn to write this node';
  if (!wait.since) return what;
  const at = new Date(wait.since);
  return `${what} since ${Number.isNaN(at.getTime()) ? wait.since : at.toLocaleString()}`;
}

// runPagePath is the address of a run's page by the run's id alone; the page
// at /run/:id opens it on its schedule's run page.
export function runPagePath(runId: string): string {
  return `/run/${encodeURIComponent(runId)}`;
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
// pending; running, succeeded, failed, cancelled and stopped (its Job is gone
// after a cancel) have pills of their own.
export function attemptPillClass(status: string): string {
  switch (status) {
    case 'running':   return 'pill-sm--running';
    case 'succeeded': return 'pill-sm--succeeded';
    case 'failed':    return 'pill-sm--failed';
    case 'cancelled': return 'pill-sm--cancelled';
    case 'stopped':   return 'pill-sm--stopped';
  }
  return 'pill-sm--pending';
}

// TASK_PILL_STATUSES are the task and run statuses that have a .pill-sm
// variant of their own; any other value reads as pending.
const TASK_PILL_STATUSES = new Set([
  'running', 'succeeded', 'failed', 'pending', 'cancelled', 'skipped',
  'dispatched', 'queued', 'stopping', 'cancelling',
]);

// taskPillClass maps a task's (or a run's) status to its .pill-sm variant.
export function taskPillClass(status: string): string {
  return TASK_PILL_STATUSES.has(status) ? `pill-sm--${status}` : 'pill-sm--pending';
}

// taskStatusTitle is the hover title of a status pill: a queued task has been
// accepted and waits for a slot to start in; a cancelling run waits for its
// Jobs to stop and a stopping task for its Job to be deleted. Other statuses
// need none.
export function taskStatusTitle(status: string | null | undefined): string | undefined {
  switch (status) {
    case 'queued':     return 'waiting for a slot';
    case 'cancelling': return 'cancelling — waiting for its Jobs to stop';
    case 'stopping':   return 'stopping — its Job is being deleted';
  }
  return undefined;
}

// runFailureLabel names why a run failed when no task of it failed (stalled,
// empty_selection, invalid_node_type), or null when the run names no reason.
export function runFailureLabel(reason: string | null | undefined): string | null {
  return reason ? `failed: ${reason.replace(/_/g, ' ')}` : null;
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
