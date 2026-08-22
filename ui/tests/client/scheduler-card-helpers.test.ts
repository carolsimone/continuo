import { describe, expect, it } from 'vitest';
import {
  getRunningCount,
  getScheduleProgressLabel,
  getScheduleProgressPercent,
} from '../../src/client/scheduler-card-helpers';
import { Task } from '../../src/client/types';

function makeTask(status: string, table: string): Task {
  return {
    task_id: table,
    service_name: 'svc',
    schema_name: 'analytics',
    table_name: table,
    job_name: '',
    status,
    retry_count: 0,
    max_retries: 3,
    created_at: null,
  };
}

describe('scheduler card helpers', () => {
  it('shows completed nodes out of total instead of running nodes', () => {
    const tasks = [
      makeTask('succeeded', 'a'),
      makeTask('failed', 'b'),
      makeTask('cancelled', 'c'),
      makeTask('running', 'd'),
      makeTask('pending', 'e'),
    ];

    expect(getScheduleProgressLabel(tasks)).toBe('Completed: 3/5');
  });

  it('computes progress percent from terminal nodes', () => {
    const tasks = [
      makeTask('succeeded', 'a'),
      makeTask('failed', 'b'),
      makeTask('running', 'c'),
      makeTask('pending', 'd'),
    ];

    expect(getScheduleProgressPercent(tasks)).toBe(50);
  });

  it('counts only running nodes', () => {
    const tasks = [
      makeTask('running', 'a'),
      makeTask('running', 'b'),
      makeTask('succeeded', 'c'),
      makeTask('failed', 'd'),
      makeTask('pending', 'e'),
    ];

    expect(getRunningCount(tasks)).toBe(2);
  });

  it('returns 0 running when no nodes are running', () => {
    const tasks = [
      makeTask('succeeded', 'a'),
      makeTask('failed', 'b'),
      makeTask('pending', 'c'),
    ];

    expect(getRunningCount(tasks)).toBe(0);
  });
});
