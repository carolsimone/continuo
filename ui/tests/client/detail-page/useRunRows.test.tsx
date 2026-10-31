// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useRunRows } from '../../../src/client/detail-page/useRunRows';
import type { Scheduler } from '../../../src/client/types';

// Only the interval is faked, and the reads run through act: waitFor polls
// through setInterval, so it cannot be used here.
const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

const runAt = (id: string, status: string) => ({
  run_id: id, schedule_name: 's', status,
  created_at: null, started_at: null, completed_at: null, cancelled_at: null, cancelled_by: '',
});
const tasksOf = (table: string, status = 'running') => ({
  total_count: 1,
  tasks: [{
    task_id: `t-${table}`, service_name: 'svc', schema_name: 'public', table_name: table,
    job_name: table, status, attempt: 0, max_retries: 0, created_at: null,
  }],
});
const attemptsOf = (taskId: string) => ({
  total_count: 1,
  attempts: [{
    task_id: taskId, attempt: 0, job_name: taskId, status: 'running',
    failure_class: null, failure_reason: null,
    dispatched_at: null, queued_at: null, started_at: null, finished_at: null,
    execution_seconds: null, error_message: null, log_s3_key: null, run_results_uri: null,
  }],
});

// Answers each request by its path, without the query string.
function stubFetch(handlers: Record<string, () => unknown>) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const handler = handlers[String(input).split('?')[0]];
    const body = handler ? await handler() : {};
    return { ok: true, status: 200, json: async () => body } as Response;
  });
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}
const callsTo = (fetchMock: ReturnType<typeof vi.fn>, path: string) =>
  fetchMock.mock.calls.filter((c) => String(c[0]).split('?')[0] === path).length;

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
}

describe('useRunRows', () => {
  beforeEach(() => { vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] }); });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('reads a selected past run once and does not poll', async () => {
    const fetchMock = stubFetch({
      '/api/runs/r2/tasks': () => tasksOf('old_orders', 'failed'),
      '/api/runs/r2/attempts': () => attemptsOf('t-old_orders'),
    });
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: 'r2', mode: 'run', onScheduler: vi.fn(),
    }));
    await advance(0);
    expect(result.current.tasks.map((t) => t.table_name)).toEqual(['old_orders']);
    expect(result.current.attempts).toHaveLength(1);

    await advance(15000);
    expect(callsTo(fetchMock, '/api/runs/r2/tasks')).toBe(1);
    expect(callsTo(fetchMock, '/api/runs/r2/attempts')).toBe(1);
    expect(callsTo(fetchMock, '/api/runs/r2')).toBe(0);
    expect(callsTo(fetchMock, '/api/runs/r1/tasks')).toBe(0);
  });

  it('reads nothing in latest mode', async () => {
    const fetchMock = stubFetch({});
    renderHook(() => useRunRows({ lastRunId: 'r1', selectedRunId: null, mode: 'latest', onScheduler: vi.fn() }));
    await advance(15000);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('polls the latest run every 5 s until it is terminal, then reads the rows once more', async () => {
    let status = 'running';
    const fetchMock = stubFetch({
      '/api/runs/r1': () => runAt('r1', status),
      '/api/runs/r1/tasks': () => tasksOf('orders', status),
      '/api/runs/r1/attempts': () => attemptsOf('t-orders'),
    });
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: null, mode: 'run', onScheduler: vi.fn(),
    }));
    await advance(0);
    expect(callsTo(fetchMock, '/api/runs/r1/tasks')).toBe(1);

    await advance(5000);
    expect(callsTo(fetchMock, '/api/runs/r1/tasks')).toBe(2);

    status = 'succeeded';
    await advance(5000);
    // The poll that sees the run end reads the rows, and the read after it
    // returns the rows the run ended with.
    expect(callsTo(fetchMock, '/api/runs/r1/tasks')).toBe(4);
    expect(result.current.tasks[0].status).toBe('succeeded');

    await advance(15000);
    expect(callsTo(fetchMock, '/api/runs/r1/tasks')).toBe(4);
  });

  it('calls onScheduler with each polled run status', async () => {
    let status = 'running';
    stubFetch({
      '/api/runs/r1': () => runAt('r1', status),
      '/api/runs/r1/tasks': () => tasksOf('orders'),
      '/api/runs/r1/attempts': () => attemptsOf('t-orders'),
    });
    const onScheduler = vi.fn<(s: Scheduler) => void>();
    renderHook(() => useRunRows({ lastRunId: 'r1', selectedRunId: null, mode: 'run', onScheduler }));
    await advance(0);
    status = 'failed';
    await advance(5000);

    expect(onScheduler.mock.calls.map((c) => c[0].status)).toEqual(['running', 'failed']);
  });

  it('ignores the rows of a run that is no longer shown', async () => {
    const firstTasks = deferred<unknown>();
    stubFetch({
      '/api/runs/r2/tasks': () => firstTasks.promise,
      '/api/runs/r2/attempts': () => attemptsOf('t-second'),
      '/api/runs/r3/tasks': () => tasksOf('third', 'failed'),
      '/api/runs/r3/attempts': () => attemptsOf('t-third'),
    });
    const { result, rerender } = renderHook(
      ({ selectedRunId }) => useRunRows({ lastRunId: 'r1', selectedRunId, mode: 'run', onScheduler: vi.fn() }),
      { initialProps: { selectedRunId: 'r2' } },
    );
    await advance(0);

    rerender({ selectedRunId: 'r3' });
    await advance(0);
    firstTasks.resolve(tasksOf('second', 'failed'));
    await advance(0);

    expect(result.current.tasks.map((t) => t.table_name)).toEqual(['third']);
    expect(result.current.attempts.map((a) => a.task_id)).toEqual(['t-third']);
  });

  it('empties tasks and attempts with clearRows', async () => {
    stubFetch({
      '/api/runs/r2/tasks': () => tasksOf('old_orders', 'failed'),
      '/api/runs/r2/attempts': () => attemptsOf('t-old_orders'),
    });
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: 'r2', mode: 'run', onScheduler: vi.fn(),
    }));
    await advance(0);
    expect(result.current.tasks).toHaveLength(1);

    act(() => result.current.clearRows());
    expect(result.current.tasks).toEqual([]);
    expect(result.current.attempts).toEqual([]);
  });
});
