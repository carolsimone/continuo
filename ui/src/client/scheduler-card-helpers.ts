import { Task } from './types';

// A task is done once it has ended: it succeeded, failed, was skipped or was
// cancelled. A stopping task has not ended: its Job is still being deleted.
function isTerminalStatus(status: string): boolean {
  return status === 'succeeded' || status === 'failed' || status === 'skipped' || status === 'cancelled';
}

export function getCompletedCount(tasks: Task[]): number {
  return tasks.filter((task) => isTerminalStatus(task.status)).length;
}

export function getRunningCount(tasks: Task[]): number {
  return tasks.filter((task) => task.status === 'running').length;
}

export function getScheduleProgressLabel(tasks: Task[]): string {
  return `Completed: ${getCompletedCount(tasks)}/${tasks.length}`;
}

export function getScheduleProgressPercent(tasks: Task[]): number {
  if (tasks.length === 0) return 0;
  return Math.round((getCompletedCount(tasks) / tasks.length) * 100);
}
