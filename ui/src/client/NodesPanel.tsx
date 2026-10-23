import { useRef, useEffect } from 'react';
import { Task, TaskAttempt } from './types';
import { rollupStatus } from './service-helpers';
import { latestAttempt, taskNodeId } from './detail-page-helpers';
import NodeTableBody from './NodeTableBody';

interface Props {
  tasks: Task[];
  attemptsByTask: Map<string, TaskAttempt[]>; // each task's attempts in attempt order
  selectedNodeId: string | null;
  onNodeSelect: (nodeId: string | null) => void;
  expandedServices: Set<string>;
  onServiceToggle: (service: string) => void;
  serviceColors: Map<string, string>;
}

// Dispatched and queued tasks, which wait for execution-controller or for a
// slot, are listed just before the running ones, and stopping tasks (a cancel is
// deleting their Job) between the two. A status not listed sorts last.
const STATUS_ORDER: Record<string, number> = {
  failed: 0, dispatched: 1, queued: 2, stopping: 3, running: 4, pending: 5, skipped: 6, succeeded: 7, cancelled: 8,
};
const UNLISTED_STATUS_RANK = 8;

// A task finished when its highest-numbered attempt finished.
function completedAtMs(task: Task, attemptsByTask: Map<string, TaskAttempt[]>): number {
  const finishedAt = latestAttempt(attemptsByTask.get(task.task_id))?.finished_at;
  return finishedAt ? new Date(finishedAt).getTime() : 0;
}

// Ties within a status bucket (e.g. all succeeded) break by the finished_at of
// each task's latest attempt, descending — most recently finished first. Tasks
// whose latest attempt has no finished_at (pending/running) tie at 0 and keep
// their incoming order.
function sortTasks(tasks: Task[], attemptsByTask: Map<string, TaskAttempt[]>): Task[] {
  return [...tasks].sort((a, b) =>
    (STATUS_ORDER[a.status] ?? UNLISTED_STATUS_RANK) - (STATUS_ORDER[b.status] ?? UNLISTED_STATUS_RANK) ||
    completedAtMs(b, attemptsByTask) - completedAtMs(a, attemptsByTask)
  );
}

export interface ServiceGroup {
  service: string;
  status: string;
  tasks: Task[];
}

// Groups sort most-severe-first (same precedence as node rows), ties by name.
export function groupTasksByService(tasks: Task[], attemptsByTask: Map<string, TaskAttempt[]>): ServiceGroup[] {
  const byService = new Map<string, Task[]>();
  tasks.forEach((task) => {
    const bucket = byService.get(task.service_name);
    if (bucket) bucket.push(task);
    else byService.set(task.service_name, [task]);
  });

  return [...byService.entries()]
    .map(([service, groupTasks]) => ({
      service,
      status: rollupStatus(groupTasks.map((t) => t.status)),
      tasks: sortTasks(groupTasks, attemptsByTask),
    }))
    .sort((a, b) =>
      (STATUS_ORDER[a.status] ?? UNLISTED_STATUS_RANK) - (STATUS_ORDER[b.status] ?? UNLISTED_STATUS_RANK) ||
      a.service.localeCompare(b.service),
    );
}

export default function NodesPanel({
  tasks,
  attemptsByTask,
  selectedNodeId,
  onNodeSelect,
  expandedServices,
  onServiceToggle,
  serviceColors,
}: Props) {
  const rowRefs = useRef<Map<string, HTMLTableRowElement>>(new Map());
  const groupRefs = useRef<Map<string, HTMLTableRowElement>>(new Map());
  const prevExpandedRef = useRef<Set<string>>(expandedServices);

  // Auto-scroll to the selected / highlighted row
  useEffect(() => {
    if (!selectedNodeId) return;
    const match = tasks.find(t => taskNodeId(t) === selectedNodeId);
    if (match) {
      rowRefs.current.get(match.task_id)?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  }, [selectedNodeId, tasks]);

  // When a service is expanded from the graph side, bring its group into view.
  useEffect(() => {
    const prev = prevExpandedRef.current;
    prevExpandedRef.current = expandedServices;
    const added = [...expandedServices].filter((svc) => !prev.has(svc));
    if (added.length === 1) {
      groupRefs.current.get(added[0])?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  }, [expandedServices]);

  if (tasks.length === 0) {
    return <p style={{ padding: 16, color: '#94a3b8', fontSize: 12, textAlign: 'center', margin: 0 }}>No tasks yet.</p>;
  }

  const groups = groupTasksByService(tasks, attemptsByTask);

  return (
    <NodeTableBody
      groups={groups}
      attemptsByTask={attemptsByTask}
      edges={[]}
      startedAt={{}}
      forceExpanded={false}
      expandedServices={expandedServices}
      onServiceToggle={onServiceToggle}
      selectedNodeId={selectedNodeId}
      onNodeSelect={onNodeSelect}
      serviceColors={serviceColors}
      rowRefs={rowRefs}
      groupRefs={groupRefs}
    />
  );
}
