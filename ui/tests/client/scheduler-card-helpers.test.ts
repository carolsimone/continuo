import { describe, expect, it } from 'vitest';
import {
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
    attempt: 0,
    max_retries: 3,
    created_at: null,
  };
}

describe('scheduler card helpers', () => {
  it('does not count a stopping task as completed: its Job is still being deleted', () => {
    const tasks = [makeTask('succeeded', 'a'), makeTask('stopping', 'b'), makeTask('cancelled', 'c')];
    expect(getScheduleProgressLabel(tasks)).toBe('Completed: 2/3');
  });

  it('counts a skipped task as completed, so a failed run whose failure skipped the tasks below it reads 100%', () => {
    const tasks = [makeTask('succeeded', 'a'), makeTask('failed', 'b'), makeTask('skipped', 'c'), makeTask('skipped', 'd')];
    expect(getScheduleProgressLabel(tasks)).toBe('Completed: 4/4');
    expect(getScheduleProgressPercent(tasks)).toBe(100);
  });

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

  it('counts a dispatched or queued node as not completed', () => {
    const tasks = [
      makeTask('succeeded', 'a'),
      makeTask('dispatched', 'b'),
      makeTask('queued', 'c'),
      makeTask('running', 'd'),
    ];

    expect(getScheduleProgressLabel(tasks)).toBe('Completed: 1/4');
    expect(getScheduleProgressPercent(tasks)).toBe(25);
  });
});
