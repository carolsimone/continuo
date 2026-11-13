import { Link } from 'react-router';
import { nodesComplete, summarizeTasks, TaskStatusBar, TaskStatusCounts } from './TaskStatusProgress';
import type { Task } from './types';

// Live summary for a run's task list: percent-complete plus a segmented bar
// and count chips broken down by status, and a link to the release the run
// executes on when the caller knows it. Purely presentational — the caller
// supplies the current Task[] (e.g. from a polling/live feed) and this
// component only aggregates and renders it.
export default function RunProgressHeader({ tasks, releaseId }: { tasks: Task[]; releaseId?: string | null }) {
  const summary = summarizeTasks(tasks);
  return (
    <div className="run-progress">
      <div className="run-progress-top">
        <span className="run-progress-pct" data-testid="run-progress-pct">{summary.percent}%</span>
        <span className="run-progress-of">{nodesComplete(summary)}</span>
        {releaseId && (
          <span className="run-progress-of">
            Release <Link to={`/releases/${releaseId}`}>{releaseId}</Link>
          </span>
        )}
      </div>
      <TaskStatusBar summary={summary} />
      <TaskStatusCounts summary={summary} />
    </div>
  );
}
