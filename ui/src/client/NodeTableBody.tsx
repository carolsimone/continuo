import { Fragment, useRef, useState, type MutableRefObject } from 'react';
import type { Task, TaskAttempt, GraphEdge } from './types';
import type { ServiceGroup } from './NodesPanel';
import { intraServiceOrder } from './run-graph-helpers';
import {
  attemptClassReason,
  attemptPillClass,
  attemptWaitSeconds,
  formatAttemptSeconds,
  latestAttempt,
  taskNodeId,
} from './detail-page-helpers';

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
  // Each task's attempts in attempt order. A row's Error, Started, Completed
  // and Logs cells read the task's highest-numbered attempt.
  attemptsByTask: Map<string, TaskAttempt[]>;
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
  // making each node row a selection button; callers with no notion of a
  // "selected" node (RunNodeTable) omit it, and a row there expands into its
  // task's attempts instead — or, for a task without attempts, renders as
  // plain, non-interactive cells (no role/tabIndex/handlers).
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
  attemptsByTask,
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
  const [openTasks, setOpenTasks] = useState<Set<string>>(new Set());
  const toggleTask = (taskId: string) => setOpenTasks(prev => {
    const next = new Set(prev);
    if (next.has(taskId)) next.delete(taskId);
    else next.add(taskId);
    return next;
  });

  // Node rows select a node only when a caller wires row selection
  // (NodesPanel). A caller that omits onNodeSelect (RunNodeTable) gets rows
  // that expand into their task's attempts; a task without attempts gets a
  // plain row — no role/tabIndex and no click/keyboard handlers — so it is
  // not tabbable, does not announce as a button, and carries no dead
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
                const attempts = attemptsByTask.get(task.task_id) ?? [];
                const latest = latestAttempt(attempts);
                const nid = taskNodeId(task);
                const isSelected = nid === selectedNodeId;
                const isRunning = task.status === 'running';
                const expandable = !interactive && attempts.length > 0;
                const isOpen = expandable && openTasks.has(task.task_id);
                const rowClass = [
                  isSelected ? 'nodes-row--selected' : '',
                  !isSelected && isRunning ? 'nodes-row--active' : '',
                  expandable ? 'remediation-group' : '',
                  isOpen ? 'remediation-group--open nodes-row--no-border' : '',
                  !interactive && !expandable ? 'nodes-row--static' : '',
                ].filter(Boolean).join(' ');

                const rowHandlers = interactive
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
                  : expandable
                    ? {
                      onClick: () => toggleTask(task.task_id),
                      tabIndex: 0,
                      role: 'button',
                      'aria-expanded': isOpen,
                      onKeyDown: (e: React.KeyboardEvent) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          toggleTask(task.task_id);
                        }
                      },
                    }
                    : {};

                const nameBlock = (
                  <>
                    <div className="nodes-node-name">{task.table_name}</div>
                    <div className="nodes-node-schema">{task.service_name} · {task.schema_name}</div>
                  </>
                );

                return (
                  <Fragment key={task.task_id}>
                    <tr
                      className={rowClass}
                      ref={el => { if (el) rowRefMap.current.set(task.task_id, el); }}
                      {...rowHandlers}
                    >
                      <td>
                        {interactive ? nameBlock : (
                          <div className="nodes-node-toggle">
                            {expandable
                              ? <span className={`remediation-chevron${isOpen ? ' remediation-chevron--open' : ''}`} aria-hidden="true">▸</span>
                              : <span className="nodes-node-toggle__spacer" aria-hidden="true" />}
                            <div>{nameBlock}</div>
                          </div>
                        )}
                      </td>
                      <td>
                        <span className={`pill-sm ${pillClass(task.status)}`}>{task.status}</span>
                      </td>
                      <td>
                        <span className="nodes-attempt">
                          {task.max_retries > 0
                            ? `${task.attempt + 1} / ${task.max_retries + 1}`
                            : `${task.attempt + 1}`}
                        </span>
                      </td>
                      <td>
                        {latest?.error_message
                          ? <ErrorCell message={latest.error_message} />
                          : <span className="nodes-dash">—</span>}
                      </td>
                      <td>
                        {latest?.started_at
                          ? <span className="nodes-ts">{new Date(latest.started_at).toLocaleTimeString()}</span>
                          : <span className="nodes-ts nodes-ts--dim">—</span>}
                      </td>
                      <td>
                        {latest?.finished_at
                          ? <span className="nodes-ts">{new Date(latest.finished_at).toLocaleTimeString()}</span>
                          : <span className="nodes-ts nodes-ts--dim">—</span>}
                      </td>
                      <td>
                        {latest?.log_s3_key
                          ? <LogLink attempt={latest} label="logs" />
                          : <span className="nodes-dash">—</span>}
                      </td>
                    </tr>
                    {isOpen && (
                      <tr className="remediation-group__body nodes-row--static">
                        <td colSpan={7}>
                          <div className="remediation-attempts">
                            <AttemptRows attempts={attempts} />
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </Fragment>
          );
        })}
      </tbody>
    </table>
  );
}

// The log route reads the object by key; the path segment names the task.
function LogLink({ attempt, label }: { attempt: TaskAttempt; label: string }) {
  return (
    <a
      href={`/api/task-execution/${attempt.task_id}/logs?key=${encodeURIComponent(attempt.log_s3_key ?? '')}`}
      target="_blank"
      rel="noopener noreferrer"
      className="nodes-log-link"
      onClick={e => e.stopPropagation()}
      onKeyDown={e => e.stopPropagation()}
    >
      {label}
    </a>
  );
}

// AttemptRows lists every attempt of one task, in attempt order, inside the
// task row's expansion block.
function AttemptRows({ attempts }: { attempts: TaskAttempt[] }) {
  return (
    <table className="nodes-table remediation-attempts__table">
      <thead>
        <tr>
          <th>#</th>
          <th>Status</th>
          <th>Class · reason</th>
          <th>Wait</th>
          <th>Run time</th>
          <th>Log</th>
        </tr>
      </thead>
      <tbody>
        {attempts.map(a => {
          const classReason = attemptClassReason(a);
          return (
            <tr key={a.attempt} className="nodes-row--static">
              <td className="nodes-attempt">{a.attempt + 1}</td>
              <td><span className={`pill-sm ${attemptPillClass(a.status)}`}>{a.status}</span></td>
              <td>
                {classReason === '—'
                  ? <span className="nodes-dash">—</span>
                  : <span className="nodes-reason">{classReason}</span>}
              </td>
              <td><span className="nodes-ts">{formatAttemptSeconds(attemptWaitSeconds(a))}</span></td>
              <td><span className="nodes-ts">{formatAttemptSeconds(a.execution_seconds)}</span></td>
              <td>
                {a.log_s3_key
                  ? <LogLink attempt={a} label="open log ↗" />
                  : <span className="nodes-dash">—</span>}
              </td>
            </tr>
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
