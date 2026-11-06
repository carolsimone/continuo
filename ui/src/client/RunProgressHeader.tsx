import { nodesComplete, summarizeTasks, TaskStatusBar, TaskStatusCounts } from './TaskStatusProgress';
import type { Task } from './types';

// Live summary for a run's task list: percent-complete plus a segmented bar
// and count chips broken down by status. Purely presentational — the caller
// supplies the current Task[] (e.g. from a polling/live feed) and this
// component only aggregates and renders it.
export default function RunProgressHeader({ tasks }: { tasks: Task[] }) {
  const summary = summarizeTasks(tasks);
  return (
    <div className="run-progress">
      <div className="run-progress-top">
        <span className="run-progress-pct" data-testid="run-progress-pct">{summary.percent}%</span>
        <span className="run-progress-of">{nodesComplete(summary)}</span>
      </div>
      <TaskStatusBar summary={summary} />
      <TaskStatusCounts summary={summary} />
    </div>
  );
}
