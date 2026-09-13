import type { Task } from './types';

// Status buckets a run's tasks fall into. `cancelled` is a terminal outcome
// (an operator stopped the run), so it is counted on its own and toward
// completion — never folded into `pending`, which would make a finished run
// look like it still has outstanding work. Any status not in this closed set
// (e.g. a value the backend has not yet reported) is treated as "pending" so
// the header never silently drops a task from its counts.
const BUCKETS = ['succeeded', 'running', 'failed', 'skipped', 'cancelled', 'pending'] as const;
type Bucket = (typeof BUCKETS)[number];

function bucketOf(status: string): Bucket {
  return (BUCKETS as readonly string[]).includes(status) ? (status as Bucket) : 'pending';
}

// Live summary for a run's task list: percent-complete plus a segmented bar
// and count chips broken down by status bucket. Purely presentational — the
// caller supplies the current Task[] (e.g. from a polling/live feed) and this
// component only aggregates and renders it.
export default function RunProgressHeader({ tasks }: { tasks: Task[] }) {
  const counts: Record<Bucket, number> = { succeeded: 0, running: 0, failed: 0, skipped: 0, cancelled: 0, pending: 0 };
  for (const task of tasks) counts[bucketOf(task.status)] += 1;
  const total = tasks.length || 1;
  const done = counts.succeeded + counts.failed + counts.skipped + counts.cancelled;
  const pct = Math.round((100 * done) / total);
  return (
    <div className="run-progress">
      <div className="run-progress-top">
        <span className="run-progress-pct" data-testid="run-progress-pct">{pct}%</span>
        <span className="run-progress-of">{done} of {tasks.length} nodes complete</span>
      </div>
      <div className="run-progress-bar">
        {BUCKETS.map((b) => (
          <span key={b} className={`rp-seg rp-${b}`} style={{ width: `${(100 * counts[b]) / total}%` }} />
        ))}
      </div>
      <div className="run-progress-counts">
        {BUCKETS.map((b) => (
          <span key={b} className="rp-count">
            <span className={`rp-dot rp-${b}`} />
            <b data-testid={`count-${b}`}>{counts[b]}</b> {b}
          </span>
        ))}
      </div>
    </div>
  );
}
