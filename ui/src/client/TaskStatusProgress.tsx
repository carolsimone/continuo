import { taskStatusTitle } from './detail-page-helpers';
import { formatCount } from './node-helpers';
import type { Task } from './types';

// The statuses a run's tasks are counted under, in the order the bar, the
// counts and the swimlane legend read. Dispatched (sent, not yet acknowledged),
// queued (accepted, waiting for a slot) and stopping (a cancel asked its Job to
// stop, which is still being deleted) come just before running, and none of
// them has ended. Cancelled is an outcome (an operator stopped the run), so it
// is counted on its own and toward completion.
const BUCKETS = ['succeeded', 'dispatched', 'queued', 'stopping', 'running', 'failed', 'skipped', 'cancelled', 'pending'] as const;
type Bucket = (typeof BUCKETS)[number];

// A status outside the closed set (one the backend has not reported before)
// counts as pending, so a task is never dropped from the counts.
function bucketOf(status: string): Bucket {
  return (BUCKETS as readonly string[]).includes(status) ? (status as Bucket) : 'pending';
}

export interface TaskSummary {
  counts: Record<Bucket, number>;
  total: number;
  done: number;     // the tasks that have ended: succeeded, failed, skipped or cancelled
  // done as a whole percentage of total, and 100 only once every task has ended:
  // 1,999 of 2,000 rounds to 100 but reads 99. A run with no tasks reads 0.
  percent: number;
}

export function summarizeTasks(tasks: Task[]): TaskSummary {
  const counts: Record<Bucket, number> = {
    succeeded: 0, dispatched: 0, queued: 0, stopping: 0, running: 0, failed: 0, skipped: 0, cancelled: 0, pending: 0,
  };
  for (const task of tasks) counts[bucketOf(task.status)] += 1;
  const total = tasks.length;
  const done = counts.succeeded + counts.failed + counts.skipped + counts.cancelled;
  const percent = total === 0 ? 0 : done === total ? 100 : Math.min(99, Math.round((100 * done) / total));
  return { counts, total, done, percent };
}

const nodeNoun = (n: number) => (n === 1 ? 'node' : 'nodes');

// "28 of 41 nodes complete"
export function nodesComplete({ done, total }: TaskSummary): string {
  return `${formatCount(done)} of ${formatCount(total)} ${nodeNoun(total)} complete`;
}

// "41 nodes"
export function nodeTotal({ total }: TaskSummary): string {
  return `${formatCount(total)} ${nodeNoun(total)}`;
}

// The run's tasks as one bar, a segment for each status that has any. The bar
// repeats the counts beside it, so assistive technology reads only the counts.
export function TaskStatusBar({ summary }: { summary: TaskSummary }) {
  const { counts, total } = summary;
  return (
    <div className="run-progress-bar" aria-hidden="true">
      {BUCKETS.filter(b => counts[b] > 0).map(b => (
        <span
          key={b}
          className={`rp-seg rp-${b}`}
          style={{ width: `${(100 * counts[b]) / total}%` }}
          title={`${formatCount(counts[b])} ${b}`}
        />
      ))}
    </div>
  );
}

// One count per status, each with the dot that paints it in the bar. hideZero
// leaves out the statuses with no tasks. A failed count stands out once a task
// has failed.
export function TaskStatusCounts({ summary, hideZero = false }: { summary: TaskSummary; hideZero?: boolean }) {
  const { counts } = summary;
  const shown = hideZero ? BUCKETS.filter(b => counts[b] > 0) : BUCKETS;
  return (
    <div className="run-progress-counts">
      {shown.map(b => (
        <span
          key={b}
          className={b === 'failed' && counts[b] > 0 ? 'rp-count rp-count--alert' : 'rp-count'}
          title={taskStatusTitle(b)}
        >
          <span className={`rp-dot rp-${b}`} />
          <b data-testid={`count-${b}`}>{formatCount(counts[b])}</b> {b}
        </span>
      ))}
    </div>
  );
}
