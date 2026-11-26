import { useRef, useEffect } from 'react';
import { Task, TaskAttempt } from './types';
import { rollupStatus } from './project-helpers';
import { latestAttempt, taskNodeId } from './detail-page-helpers';
import NodeTableBody from './NodeTableBody';

interface Props {
  tasks: Task[];
  attemptsByTask: Map<string, TaskAttempt[]>; // each task's attempts in attempt order
  selectedNodeId: string | null;
  onNodeSelect: (nodeId: string | null) => void;
  expandedProjects: Set<string>;
  onProjectToggle: (project: string) => void;
  projectColors: Map<string, string>;
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

export interface ProjectGroup {
  project: string;
  status: string;
  tasks: Task[];
}

// Groups sort most-severe-first (same precedence as node rows), ties by name.
export function groupTasksByProject(tasks: Task[], attemptsByTask: Map<string, TaskAttempt[]>): ProjectGroup[] {
  const byProject = new Map<string, Task[]>();
  tasks.forEach((task) => {
    const bucket = byProject.get(task.project_name);
    if (bucket) bucket.push(task);
    else byProject.set(task.project_name, [task]);
  });

  return [...byProject.entries()]
    .map(([project, groupTasks]) => ({
      project,
      status: rollupStatus(groupTasks.map((t) => t.status)),
      tasks: sortTasks(groupTasks, attemptsByTask),
    }))
    .sort((a, b) =>
      (STATUS_ORDER[a.status] ?? UNLISTED_STATUS_RANK) - (STATUS_ORDER[b.status] ?? UNLISTED_STATUS_RANK) ||
      a.project.localeCompare(b.project),
    );
}

export default function NodesPanel({
  tasks,
  attemptsByTask,
  selectedNodeId,
  onNodeSelect,
  expandedProjects,
  onProjectToggle,
  projectColors,
}: Props) {
  const rowRefs = useRef<Map<string, HTMLTableRowElement>>(new Map());
  const groupRefs = useRef<Map<string, HTMLTableRowElement>>(new Map());
  const prevExpandedRef = useRef<Set<string>>(expandedProjects);

  // Auto-scroll to the selected / highlighted row
  useEffect(() => {
    if (!selectedNodeId) return;
    const match = tasks.find(t => taskNodeId(t) === selectedNodeId);
    if (match) {
      rowRefs.current.get(match.task_id)?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  }, [selectedNodeId, tasks]);

  // When a project is expanded from the graph side, bring its group into view.
  useEffect(() => {
    const prev = prevExpandedRef.current;
    prevExpandedRef.current = expandedProjects;
    const added = [...expandedProjects].filter((svc) => !prev.has(svc));
    if (added.length === 1) {
      groupRefs.current.get(added[0])?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  }, [expandedProjects]);

  if (tasks.length === 0) {
    return <p style={{ padding: 16, color: '#94a3b8', fontSize: 12, textAlign: 'center', margin: 0 }}>No tasks yet.</p>;
  }

  const groups = groupTasksByProject(tasks, attemptsByTask);

  return (
    <NodeTableBody
      groups={groups}
      attemptsByTask={attemptsByTask}
      edges={[]}
      startedAt={{}}
      forceExpanded={false}
      expandedProjects={expandedProjects}
      onProjectToggle={onProjectToggle}
      selectedNodeId={selectedNodeId}
      onNodeSelect={onNodeSelect}
      projectColors={projectColors}
      rowRefs={rowRefs}
      groupRefs={groupRefs}
    />
  );
}
