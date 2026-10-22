import { useState } from 'react';
import type { Task, TaskAttempt, GraphEdge } from './types';
import { groupTasksByService, type ServiceGroup } from './NodesPanel';
import { latestAttempt, taskNodeId } from './detail-page-helpers';
import NodeTableBody from './NodeTableBody';

interface Props {
  tasks: Task[];
  attemptsByTask: Map<string, TaskAttempt[]>; // each task's attempts in attempt order
  edges: GraphEdge[]; // run graph edges, for intra-service order
  expandedServices: Set<string>;
  onServiceToggle: (service: string) => void;
}

type Filter = 'all' | 'running' | 'failed';

// `failed` also surfaces `skipped` rows: a node cascade-skipped downstream of
// a failure is part of the same "something needs attention" story as the
// failure itself.
function matchesFilter(status: string, filter: Filter): boolean {
  return filter === 'all'
    || (filter === 'running' && status === 'running')
    || (filter === 'failed' && (status === 'failed' || status === 'skipped'));
}

export default function RunNodeTable({
  tasks,
  attemptsByTask,
  edges,
  expandedServices,
  onServiceToggle,
}: Props) {
  const [filter, setFilter] = useState<Filter>('all');
  const [query, setQuery] = useState('');

  const startedAt: Record<string, string | null | undefined> = {};
  for (const t of tasks) {
    startedAt[taskNodeId(t)] = latestAttempt(attemptsByTask.get(t.task_id))?.started_at ?? null;
  }

  const normalizedQuery = query.trim().toLowerCase();
  const filteredTasks = tasks.filter(
    (t) => matchesFilter(t.status, filter)
      && (!normalizedQuery || t.table_name.toLowerCase().includes(normalizedQuery)),
  );
  const groups: ServiceGroup[] = groupTasksByService(filteredTasks, attemptsByTask);
  const isFiltering = filter !== 'all' || normalizedQuery !== '';

  const runningCount = tasks.filter((t) => t.status === 'running').length;
  // The Failed filter reveals failed AND skipped rows (see matchesFilter), so
  // its badge counts both — the number matches what clicking it shows.
  const failedCount = tasks.filter((t) => t.status === 'failed' || t.status === 'skipped').length;

  return (
    <div>
      <div className="run-filters">
        <button
          className={`rf-btn${filter === 'all' ? ' on' : ''}`}
          onClick={() => setFilter('all')}
        >
          All {tasks.length}
        </button>
        <button
          className={`rf-btn${filter === 'running' ? ' on' : ''}`}
          onClick={() => setFilter('running')}
        >
          Running {runningCount}
        </button>
        <button
          className={`rf-btn fail${filter === 'failed' ? ' on' : ''}`}
          onClick={() => setFilter('failed')}
        >
          Failed {failedCount}
        </button>
        <input
          className="run-search"
          placeholder="Filter nodes…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
      </div>
      <NodeTableBody
        groups={groups}
        attemptsByTask={attemptsByTask}
        edges={edges}
        startedAt={startedAt}
        forceExpanded={isFiltering}
        expandedServices={expandedServices}
        onServiceToggle={onServiceToggle}
      />
    </div>
  );
}
