import { Fragment, useRef, useState, type MutableRefObject } from 'react';
import type { Task, TaskExecution, GraphEdge } from './types';
import type { ServiceGroup } from './NodesPanel';
import { intraServiceOrder } from './run-graph-helpers';
import { taskNodeId } from './detail-page-helpers';

function pillClass(status: string): string {
  const map: Record<string, string> = {
    running: 'pill-sm--running', succeeded: 'pill-sm--succeeded',
    failed: 'pill-sm--failed', pending: 'pill-sm--pending', cancelled: 'pill-sm--cancelled',
    skipped: 'pill-sm--skipped',
  };
  return map[status] ?? 'pill-sm--pending';
}

interface Props {
  groups: ServiceGroup[];
  execByTaskId: Map<string, TaskExecution>;
  // Run graph edges and each node's started_at, used to order a group's rows
  // by intraServiceOrder. An empty edges array carries no dependency
  // information, so rows are left in the group's incoming (already
  // severity/recency sorted) order rather than falling through to
  // intraServiceOrder's alphabetical tiebreak.
  edges: GraphEdge[];
  startedAt: Record<string, string | null | undefined>;
  // A group is expanded when forceExpanded || expandedServices.has(service) —
  // callers that are actively filtering (RunNodeTable) force every remaining
  // group open regardless of the persisted expand/collapse state.
  forceExpanded: boolean;
  expandedServices: Set<string>;
  onServiceToggle: (service: string) => void;
  // Row selection is optional: NodesPanel wires it to the graph/detail view,
  // making each node row an interactive button; callers with no notion of a
  // "selected" node (RunNodeTable) omit it and rows render as plain,
  // non-interactive cells (no role/tabIndex/handlers).
  selectedNodeId?: string | null;
  onNodeSelect?: (nodeId: string | null) => void;
  serviceColors?: Map<string, string>;
  // Row/group DOM refs, populated as rows render. Optional so a caller with
  // no need to scroll a row/group into view (RunNodeTable) doesn't have to
  // supply one; NodeTableBody keeps its own map in that case.
  rowRefs?: MutableRefObject<Map<string, HTMLTableRowElement>>;
  groupRefs?: MutableRefObject<Map<string, HTMLTableRowElement>>;
}

export default function NodeTableBody({
  groups,
  execByTaskId,
  edges,
  startedAt,
  forceExpanded,
  expandedServices,
  onServiceToggle,
  selectedNodeId,
  onNodeSelect,
  serviceColors,
  rowRefs,
  groupRefs,
}: Props) {
  const fallbackRowRefs = useRef<Map<string, HTMLTableRowElement>>(new Map());
  const fallbackGroupRefs = useRef<Map<string, HTMLTableRowElement>>(new Map());
  const rowRefMap = rowRefs ?? fallbackRowRefs;
  const groupRefMap = groupRefs ?? fallbackGroupRefs;

  // Node rows are interactive only when a caller wires row selection
  // (NodesPanel). A caller that omits onNodeSelect (RunNodeTable) gets plain
  // rows — no role/tabIndex/aria-pressed and no click/keyboard handlers — so
  // the rows are not tabbable, do not announce as buttons, and carry no dead
  // affordance that clicks to nothing.
  const interactive = onNodeSelect !== undefined;

  return (
    <table className="nodes-table">
      <thead>
        <tr>
          <th>Node</th>
          <th>Status</th>
          <th>Attempt</th>
          <th>Error</th>
          <th>Started</th>
          <th>Completed</th>
          <th>Logs</th>
        </tr>
      </thead>
      <tbody>
        {groups.map(group => {
          const isExpanded = forceExpanded || expandedServices.has(group.service);
          const orderedIds = edges.length > 0
            ? intraServiceOrder(group.tasks.map(taskNodeId), edges, startedAt)
            : group.tasks.map(taskNodeId);
          const tasksById = new Map(group.tasks.map(t => [taskNodeId(t), t]));
          const orderedTasks = orderedIds.map(id => tasksById.get(id)!).filter(Boolean);

          return (
            <Fragment key={group.service}>
              <tr
                className={`nodes-group-row${isExpanded ? ' nodes-group-row--open' : ''}`}
                ref={el => { if (el) groupRefMap.current.set(group.service, el); }}
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
                <td colSpan={7}>
                  <div className="nodes-group-header">
                    <span className="nodes-group-chevron">{isExpanded ? '▾' : '▸'}</span>
                    <span
                      className="nodes-group-dot"
                      style={{ background: serviceColors?.get(group.service) ?? '#94a3b8' }}
                    />
                    <span className="nodes-group-name">{group.service}</span>
                    <span className="nodes-group-count">{group.tasks.length}</span>
                    <span className={`pill-sm ${pillClass(group.status)}`}>{group.status}</span>
                  </div>
                </td>
              </tr>
              {isExpanded && orderedTasks.map(task => {
                const exec = execByTaskId.get(task.task_id);
                const nid = taskNodeId(task);
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
                    ref={el => { if (el) rowRefMap.current.set(task.task_id, el); }}
                    {...(interactive
                      ? {
                        onClick: () => onNodeSelect?.(isSelected ? null : nid),
                        tabIndex: 0,
                        role: 'button',
                        'aria-pressed': isSelected,
                        onKeyDown: (e: React.KeyboardEvent) => {
                          if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            onNodeSelect?.(isSelected ? null : nid);
                          }
                        },
                      }
                      : {})}
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
                      {exec?.completed_at
                        ? <span className="nodes-ts">{new Date(exec.completed_at).toLocaleTimeString()}</span>
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
