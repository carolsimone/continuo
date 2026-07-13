import { Fragment, useRef, useEffect, useState } from 'react';
import { Task, TaskExecution } from './types';
import { rollupStatus } from './service-helpers';

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
  failed: 0, running: 1, pending: 2, succeeded: 3, cancelled: 4,
};

function sortTasks(tasks: Task[]): Task[] {
  return [...tasks].sort((a, b) =>
    (STATUS_ORDER[a.status] ?? 5) - (STATUS_ORDER[b.status] ?? 5)
  );
}

function pillClass(status: string): string {
  const map: Record<string, string> = {
    running: 'pill-sm--running', succeeded: 'pill-sm--succeeded',
    failed: 'pill-sm--failed', pending: 'pill-sm--pending', cancelled: 'pill-sm--cancelled',
  };
  return map[status] ?? 'pill-sm--pending';
}

function nodeId(task: Task): string {
  return `${task.service_name}.${task.schema_name}.${task.table_name}`;
}

interface ServiceGroup {
  service: string;
  status: string;
  tasks: Task[];
}

// Groups sort most-severe-first (same precedence as node rows), ties by name.
function groupTasksByService(tasks: Task[]): ServiceGroup[] {
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
      tasks: sortTasks(groupTasks),
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
    const match = tasks.find(t => nodeId(t) === selectedNodeId);
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

  const groups = groupTasksByService(tasks);

  return (
    <table className="nodes-table">
      <thead>
        <tr>
          <th>Node</th>
          <th>Status</th>
          <th>Attempt</th>
          <th>Error</th>
          <th>Started</th>
          <th>Logs</th>
        </tr>
      </thead>
      <tbody>
        {groups.map(group => {
          const isExpanded = expandedServices.has(group.service);
          return (
            <Fragment key={group.service}>
              <tr
                className={`nodes-group-row${isExpanded ? ' nodes-group-row--open' : ''}`}
                ref={el => { if (el) groupRefs.current.set(group.service, el); }}
                onClick={() => onServiceToggle(group.service)}
                tabIndex={0}
                role="button"
                aria-expanded={isExpanded}
                onKeyDown={e => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    onServiceToggle(group.service);
                  }
                }}
              >
                <td colSpan={6}>
                  <div className="nodes-group-header">
                    <span className="nodes-group-chevron">{isExpanded ? '▾' : '▸'}</span>
                    <span
                      className="nodes-group-dot"
                      style={{ background: serviceColors.get(group.service) ?? '#94a3b8' }}
                    />
                    <span className="nodes-group-name">{group.service}</span>
                    <span className="nodes-group-count">{group.tasks.length} nodes</span>
                    <span className={`pill-sm ${pillClass(group.status)}`}>{group.status}</span>
                  </div>
                </td>
              </tr>
              {isExpanded && group.tasks.map(task => {
                const exec = execByTaskId.get(task.task_id);
                const nid = nodeId(task);
                const isSelected = nid === selectedNodeId;
                const isRunning = task.status === 'running';
                const rowClass = [
                  isSelected ? 'nodes-row--selected' : '',
                  !isSelected && isRunning ? 'nodes-row--active' : '',
                ].filter(Boolean).join(' ');

                return (
                  <tr
                    key={task.task_id}
                    className={rowClass}
                    ref={el => { if (el) rowRefs.current.set(task.task_id, el); }}
                    onClick={() => onNodeSelect(isSelected ? null : nid)}
                    tabIndex={0}
                    role="button"
                    aria-pressed={isSelected}
                    onKeyDown={e => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        onNodeSelect(isSelected ? null : nid);
                      }
                    }}
                  >
                    <td>
                      <div className="nodes-node-name">{task.table_name}</div>
                      <div className="nodes-node-schema">{task.service_name} · {task.schema_name}</div>
                    </td>
                    <td>
                      <span className={`pill-sm ${pillClass(task.status)}`}>{task.status}</span>
                    </td>
                    <td>
                      <span className="nodes-attempt">
                        {task.max_retries > 0
                          ? `${task.retry_count + 1} / ${task.max_retries + 1}`
                          : `${task.retry_count + 1}`}
                      </span>
                    </td>
                    <td>
                      {exec?.error_message
                        ? <ErrorCell message={exec.error_message} />
                        : <span className="nodes-dash">—</span>}
                    </td>
                    <td>
                      {exec?.started_at
                        ? <span className="nodes-ts">{new Date(exec.started_at).toLocaleTimeString()}</span>
                        : <span className="nodes-ts nodes-ts--dim">—</span>}
                    </td>
                    <td>
                      {exec?.log_s3_key
                        ? (
                          <a
                            href={`/api/task-execution/${exec.id}/logs?key=${encodeURIComponent(exec.log_s3_key)}`}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="nodes-log-link"
                            onClick={e => e.stopPropagation()}
                            onKeyDown={e => e.stopPropagation()}
                          >
                            logs
                          </a>
                        )
                        : <span className="nodes-dash">—</span>}
                    </td>
                  </tr>
                );
              })}
            </Fragment>
          );
        })}
      </tbody>
    </table>
  );
}

function ErrorCell({ message }: { message: string }) {
  const [expanded, setExpanded] = useState(false);
  const isLong = message.length > 60;
  return (
    <div className="nodes-error-text">
      <span className={`nodes-error-short${expanded ? ' nodes-error-short--hidden' : ''}`}>
        {message}
      </span>
      <div className={`nodes-error-full${expanded ? ' nodes-error-full--visible' : ''}`}>
        {message}
      </div>
      {isLong && (
        <button
          className="nodes-error-toggle"
          onClick={e => { e.stopPropagation(); setExpanded(v => !v); }}
          onKeyDown={e => { e.stopPropagation(); }}
        >
          {expanded ? 'less' : 'more'}
        </button>
      )}
    </div>
  );
}
