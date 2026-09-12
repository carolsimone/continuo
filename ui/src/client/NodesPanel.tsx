import { useRef, useEffect } from 'react';
import { Task, TaskExecution } from './types';
import { rollupStatus } from './service-helpers';
import { taskNodeId } from './detail-page-helpers';
import NodeTableBody from './NodeTableBody';

interface Props {
  tasks: Task[];
  executions: TaskExecution[];
  selectedNodeId: string | null;
  onNodeSelect: (nodeId: string | null) => void;
  expandedServices: Set<string>;
  onServiceToggle: (service: string) => void;
  serviceColors: Map<string, string>;
}

const STATUS_ORDER: Record<string, number> = {
  failed: 0, running: 1, pending: 2, skipped: 3, succeeded: 4, cancelled: 5,
};

function completedAtMs(task: Task, execByTaskId: Map<string, TaskExecution>): number {
  const completedAt = execByTaskId.get(task.task_id)?.completed_at;
  return completedAt ? new Date(completedAt).getTime() : 0;
}

// Ties within a status bucket (e.g. all succeeded) break by completed_at
// descending — most recently finished first. Tasks with no completed_at
// (pending/running) tie at 0 and keep their incoming order.
function sortTasks(tasks: Task[], execByTaskId: Map<string, TaskExecution>): Task[] {
  return [...tasks].sort((a, b) =>
    (STATUS_ORDER[a.status] ?? 5) - (STATUS_ORDER[b.status] ?? 5) ||
    completedAtMs(b, execByTaskId) - completedAtMs(a, execByTaskId)
  );
}

export interface ServiceGroup {
  service: string;
  status: string;
  tasks: Task[];
}

// Groups sort most-severe-first (same precedence as node rows), ties by name.
export function groupTasksByService(tasks: Task[], execByTaskId: Map<string, TaskExecution>): ServiceGroup[] {
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
      tasks: sortTasks(groupTasks, execByTaskId),
    }))
    .sort((a, b) =>
      (STATUS_ORDER[a.status] ?? 5) - (STATUS_ORDER[b.status] ?? 5) ||
      a.service.localeCompare(b.service),
    );
}

export default function NodesPanel({
  tasks,
  executions,
  selectedNodeId,
  onNodeSelect,
  expandedServices,
  onServiceToggle,
  serviceColors,
}: Props) {
  const execByTaskId = new Map(executions.map(e => [e.task_id, e]));
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

  const groups = groupTasksByService(tasks, execByTaskId);

  return (
    <NodeTableBody
      groups={groups}
      execByTaskId={execByTaskId}
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
