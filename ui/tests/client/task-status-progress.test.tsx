// @vitest-environment jsdom
import { afterEach, describe, expect, it } from 'vitest';
import { cleanup, render } from '@testing-library/react';
import { nodeTotal, nodesComplete, summarizeTasks, TaskStatusBar, TaskStatusCounts } from '../../src/client/TaskStatusProgress';
import type { Task } from '../../src/client/types';

function tasksOf(...statuses: string[]): Task[] {
  return statuses.map((status, i) => ({
    task_id: `t-${i}`,
    service_name: 'svc',
    schema_name: 'analytics',
    table_name: `table_${i}`,
    job_name: '',
    status,
    attempt: 0,
    max_retries: 3,
    created_at: null,
  }));
}

function repeat(status: string, n: number): string[] {
  return Array.from({ length: n }, () => status);
}

afterEach(cleanup);

describe('summarizeTasks', () => {
  it('does not count a stopping task as completed: its Job is still being deleted', () => {
    const { done, total } = summarizeTasks(tasksOf('succeeded', 'stopping', 'cancelled'));
    expect([done, total]).toEqual([2, 3]);
  });

  it('counts a skipped task as completed, so a failed run whose failure skipped the tasks below it reads 100%', () => {
    const { done, total, percent } = summarizeTasks(tasksOf('succeeded', 'failed', 'skipped', 'skipped'));
    expect([done, total, percent]).toEqual([4, 4, 100]);
  });

  it('counts only the tasks that have ended as completed', () => {
    const { done, total, percent } = summarizeTasks(tasksOf('succeeded', 'failed', 'running', 'pending'));
    expect([done, total, percent]).toEqual([2, 4, 50]);
  });

  it('counts a dispatched or queued task as not completed', () => {
    const { done, total, percent } = summarizeTasks(tasksOf('succeeded', 'dispatched', 'queued', 'running'));
    expect([done, total, percent]).toEqual([1, 4, 25]);
  });

  it('counts a status it does not know as pending, so the counts still add up to the total', () => {
    const { counts, total } = summarizeTasks(tasksOf('succeeded', 'brand_new_status'));
    expect(counts.pending).toBe(1);
    expect(Object.values(counts).reduce((sum, n) => sum + n, 0)).toBe(total);
  });

  it('reads 58% for 23 of 40: rounding the fraction first would read 57%', () => {
    const tasks = tasksOf(...repeat('succeeded', 23), ...repeat('pending', 17));
    expect(summarizeTasks(tasks).percent).toBe(58);
  });

  it('reads 99%, not 100%, while a task is unfinished, however large the run', () => {
    const tasks = tasksOf(...repeat('succeeded', 1999), 'running');
    expect(summarizeTasks(tasks).percent).toBe(99);
  });

  it('reads 100% once every task has ended', () => {
    const tasks = tasksOf(...repeat('succeeded', 1999), 'failed');
    expect(summarizeTasks(tasks).percent).toBe(100);
  });

  it('reads 0% for a run with no tasks', () => {
    const { total, done, percent } = summarizeTasks([]);
    expect([total, done, percent]).toEqual([0, 0, 0]);
  });
});

describe('the words for a run\'s nodes', () => {
  it('groups thousands and says node for one', () => {
    expect(nodesComplete(summarizeTasks(tasksOf(...repeat('succeeded', 1200), ...repeat('running', 800))))).toBe('1,200 of 2,000 nodes complete');
    expect(nodesComplete(summarizeTasks(tasksOf('running')))).toBe('0 of 1 node complete');
    expect(nodeTotal(summarizeTasks(tasksOf(...repeat('succeeded', 2000))))).toBe('2,000 nodes');
    expect(nodeTotal(summarizeTasks(tasksOf('succeeded')))).toBe('1 node');
  });
});

describe('TaskStatusBar', () => {
  it('draws one segment per status that has tasks, sized by its share of the run', () => {
    const summary = summarizeTasks(tasksOf('succeeded', 'succeeded', 'succeeded', 'failed'));
    const { container } = render(<TaskStatusBar summary={summary} />);
    const segments = [...container.querySelectorAll<HTMLElement>('.rp-seg')];
    expect(segments.map(s => s.className)).toEqual(['rp-seg rp-succeeded', 'rp-seg rp-failed']);
    expect(segments.map(s => s.style.width)).toEqual(['75%', '25%']);
    expect(segments.map(s => s.title)).toEqual(['3 succeeded', '1 failed']);
  });

  it('groups the thousands of a segment\'s title, as the count beside it does', () => {
    const summary = summarizeTasks(tasksOf(...repeat('failed', 2000)));
    const { container } = render(<TaskStatusBar summary={summary} />);
    expect(container.querySelector<HTMLElement>('.rp-seg')!.title).toBe('2,000 failed');
  });

  it('draws an empty track for a run whose tasks are not known yet', () => {
    const { container } = render(<TaskStatusBar summary={summarizeTasks([])} />);
    expect(container.querySelector('.run-progress-bar')).not.toBeNull();
    expect(container.querySelector('.rp-seg')).toBeNull();
  });
});

describe('TaskStatusCounts', () => {
  const countsShown = (container: HTMLElement) =>
    [...container.querySelectorAll('.rp-count')].map(el => el.textContent);

  it('shows every status in the order the swimlane legend reads, zeros included, by default', () => {
    const { container } = render(<TaskStatusCounts summary={summarizeTasks(tasksOf('running'))} />);
    expect(countsShown(container)).toEqual([
      '0 succeeded', '0 dispatched', '0 queued', '0 stopping', '1 running',
      '0 failed', '0 skipped', '0 cancelled', '0 pending',
    ]);
  });

  it('leaves out a status with no tasks when asked to', () => {
    const summary = summarizeTasks(tasksOf('succeeded', 'succeeded', 'queued', 'pending'));
    const { container } = render(<TaskStatusCounts summary={summary} hideZero />);
    expect(countsShown(container)).toEqual(['2 succeeded', '1 queued', '1 pending']);
  });

  it('groups the thousands of a large count', () => {
    const summary = summarizeTasks(tasksOf(...repeat('succeeded', 1999), 'failed'));
    const { container } = render(<TaskStatusCounts summary={summary} hideZero />);
    expect(countsShown(container)).toEqual(['1,999 succeeded', '1 failed']);
  });

  it('marks the failed count only when something failed', () => {
    const none = render(<TaskStatusCounts summary={summarizeTasks(tasksOf('running'))} />);
    expect(none.container.querySelector('.rp-count--alert')).toBeNull();
    cleanup();

    const some = render(<TaskStatusCounts summary={summarizeTasks(tasksOf('failed', 'running'))} />);
    expect(some.container.querySelector('.rp-count--alert')).toHaveTextContent('1 failed');
  });

  it('explains a queued and a stopping count on hover', () => {
    const summary = summarizeTasks(tasksOf('queued', 'stopping'));
    const { container } = render(<TaskStatusCounts summary={summary} hideZero />);
    const titles = [...container.querySelectorAll<HTMLElement>('.rp-count')].map(el => el.title);
    expect(titles).toEqual(['waiting for a slot', 'stopping — its Job is being deleted']);
  });
});
