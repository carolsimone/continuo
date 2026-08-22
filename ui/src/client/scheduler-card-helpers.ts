import { Task } from './types';

function isTerminalStatus(status: string): boolean {
  return status === 'succeeded' || status === 'failed' || status === 'cancelled';
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
