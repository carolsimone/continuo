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
    expect(result.current.tasksRead).toBe(false);
  });

  it('reports that the tasks were read for a run that has none', async () => {
    stubFetch({
      '/api/runs/r1': () => runAt('r1', 'failed'),
      '/api/runs/r1/tasks': () => ({ total_count: 0, tasks: [] }),
      '/api/runs/r1/attempts': () => ({ total_count: 0, attempts: [] }),
    });
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: null, mode: 'run', onScheduler: vi.fn(),
    }));
    expect(result.current.tasksRead).toBe(false);
    await advance(0);
    expect(result.current.tasks).toEqual([]);
    expect(result.current.tasksRead).toBe(true);
  });
});

describe('useRunRows when a read fails', () => {
  beforeEach(() => { vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] }); });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  const ok = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });
  // What the ui server answers when run-controller cannot be reached.
  const backendDown = () => new Response(JSON.stringify({ error: '14 UNAVAILABLE' }), { status: 500 });
  // What the ingress answers when no ui pod can take the request.
  const badGateway = () => new Response('Bad Gateway', { status: 502 });

  // Answers each path from the handlers, or with `failure` while one is set.
  function stubBackend(handlers: Record<string, () => unknown>, failure: () => Response | null) {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const failed = failure();
      if (failed) return failed;
      const handler = handlers[String(input).split('?')[0]];
      return ok(handler ? handler() : {});
    });
    vi.stubGlobal('fetch', fetchMock);
    return fetchMock;
  }

  const latestRun = (status: () => string = () => 'running') => ({
    '/api/runs/r1': () => runAt('r1', status()),
    '/api/runs/r1/tasks': () => tasksOf('orders', status()),
    '/api/runs/r1/attempts': () => attemptsOf('t-orders'),
  });

  it('keeps the rows and the status of the last good read while polls fail, then picks up again', async () => {
    let down = false;
    stubBackend(latestRun(), () => (down ? backendDown() : null));
    const onScheduler = vi.fn<(s: Scheduler) => void>();
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: null, mode: 'run', onScheduler,
    }));
    await advance(0);
    expect(result.current.tasks).toHaveLength(1);
    expect(result.current.readFailed).toBe(false);

    down = true;
    await advance(5000);
    expect(result.current.tasks).toHaveLength(1);
    expect(result.current.attempts).toHaveLength(1);
    expect(result.current.readFailed).toBe(true);
    // An error body is not a run: only real statuses reach the page.
    expect(onScheduler.mock.calls.map((c) => c[0].status)).toEqual(['running']);

    down = false;
    await advance(5000);
    expect(result.current.readFailed).toBe(false);
    expect(onScheduler.mock.calls.map((c) => c[0].status)).toEqual(['running', 'running']);
  });

  it.each([
    ['a backend error', backendDown],
    ['a non-JSON answer', badGateway],
    ['a rejected request', () => { throw new TypeError('Failed to fetch'); }],
  ])('keeps polling after %s on the first read, and recovers', async (_name, fail) => {
    let down = true;
    const fetchMock = stubBackend(latestRun(), () => (down ? fail() : null));
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: null, mode: 'run', onScheduler: vi.fn(),
    }));
    await advance(0);
    expect(result.current.tasks).toEqual([]);
    expect(result.current.tasksRead).toBe(false);
    expect(result.current.readFailed).toBe(true);

    down = false;
    await advance(5000);
    expect(result.current.tasks).toHaveLength(1);
    expect(result.current.tasksRead).toBe(true);
    expect(result.current.readFailed).toBe(false);

    // The run is still running, so the page goes on polling it.
    const before = callsTo(fetchMock, '/api/runs/r1/tasks');
    await advance(5000);
    expect(callsTo(fetchMock, '/api/runs/r1/tasks')).toBe(before + 1);
  });

  it('reads the rows again when the run ended but the rows read failed on arrival', async () => {
    let rowsDown = true;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input).split('?')[0];
      if (path === '/api/runs/r1') return ok(runAt('r1', 'succeeded'));
      if (rowsDown) return backendDown();
      return ok(path.endsWith('/tasks') ? tasksOf('orders', 'succeeded') : attemptsOf('t-orders'));
    });
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: null, mode: 'run', onScheduler: vi.fn(),
    }));
    await advance(0);
    expect(result.current.readFailed).toBe(true);

    rowsDown = false;
    await advance(5000);
    expect(result.current.tasks.map((t) => t.status)).toEqual(['succeeded']);
    expect(result.current.readFailed).toBe(false);

    const before = fetchMock.mock.calls.length;
    await advance(15000);
    expect(fetchMock.mock.calls.length).toBe(before);
  });

  it('reads the rows the run ended with until a read lands', async () => {
    let status = 'running';
    let rowsDown = false;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input).split('?')[0];
      if (path === '/api/runs/r1') return ok(runAt('r1', status));
      if (rowsDown) return backendDown();
      return ok(path.endsWith('/tasks') ? tasksOf('orders', status) : attemptsOf('t-orders'));
    });
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: null, mode: 'run', onScheduler: vi.fn(),
    }));
    await advance(0);
    expect(result.current.tasks[0].status).toBe('running');

    status = 'succeeded';
    rowsDown = true;
    await advance(5000);
    expect(result.current.tasks[0].status).toBe('running');
    expect(result.current.readFailed).toBe(true);

    rowsDown = false;
    await advance(5000);
    expect(result.current.tasks[0].status).toBe('succeeded');
    expect(result.current.readFailed).toBe(false);

    const before = fetchMock.mock.calls.length;
    await advance(15000);
    expect(fetchMock.mock.calls.length).toBe(before);
  });

  it('ignores a status that lands after the poll that saw the run end', async () => {
    const slowStatus = deferred<Response>();
    let statusReads = 0;
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input).split('?')[0];
      if (path === '/api/runs/r1') {
        statusReads += 1;
        if (statusReads === 1) return ok(runAt('r1', 'running'));
        if (statusReads === 2) return slowStatus.promise;
        return ok(runAt('r1', 'succeeded'));
      }
      return ok(path.endsWith('/tasks') ? tasksOf('orders', 'running') : attemptsOf('t-orders'));
    }));
    const onScheduler = vi.fn<(s: Scheduler) => void>();
    renderHook(() => useRunRows({ lastRunId: 'r1', selectedRunId: null, mode: 'run', onScheduler }));
    await advance(10000);
    expect(onScheduler.mock.calls.at(-1)?.[0].status).toBe('succeeded');

    slowStatus.resolve(ok(runAt('r1', 'running')));
    await advance(0);
    expect(onScheduler.mock.calls.at(-1)?.[0].status).toBe('succeeded');
  });

  it('ignores rows that land after the rows the run ended with, and stays settled', async () => {
    const slowTasks = deferred<Response>();
    let status = 'running';
    let taskReads = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input).split('?')[0];
      if (path === '/api/runs/r1') return ok(runAt('r1', status));
      if (path.endsWith('/attempts')) return ok(attemptsOf('t-orders'));
      taskReads += 1;
      if (taskReads === 2) return slowTasks.promise;
      return ok(tasksOf('orders', status));
    });
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: null, mode: 'run', onScheduler: vi.fn(),
    }));
    await advance(5000);

    status = 'succeeded';
    await advance(5000);
    expect(result.current.tasks[0].status).toBe('succeeded');

    slowTasks.resolve(ok(tasksOf('orders', 'running')));
    await advance(0);
    expect(result.current.tasks[0].status).toBe('succeeded');
    expect(result.current.readFailed).toBe(false);

    const before = fetchMock.mock.calls.length;
    await advance(15000);
    expect(fetchMock.mock.calls.length).toBe(before);
  });

  it('stays settled when a poll in flight fails after the run\'s last read landed', async () => {
    const slowTasks = deferred<Response>();
    let status = 'running';
    let taskReads = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input).split('?')[0];
      if (path === '/api/runs/r1') return ok(runAt('r1', status));
      if (path.endsWith('/attempts')) return ok(attemptsOf('t-orders'));
      taskReads += 1;
      if (taskReads === 2) return slowTasks.promise;
      return ok(tasksOf('orders', status));
    });
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: null, mode: 'run', onScheduler: vi.fn(),
    }));
    await advance(5000);

    status = 'succeeded';
    await advance(5000);
    expect(result.current.readFailed).toBe(false);

    slowTasks.resolve(backendDown());
    await advance(0);
    expect(result.current.readFailed).toBe(false);

    const before = fetchMock.mock.calls.length;
    await advance(15000);
    expect(fetchMock.mock.calls.length).toBe(before);
  });

  it('reads a chosen past run again until a read lands, then stops', async () => {
    let down = true;
    const fetchMock = stubBackend({
      '/api/runs/r2/tasks': () => tasksOf('old_orders', 'failed'),
      '/api/runs/r2/attempts': () => attemptsOf('t-old_orders'),
    }, () => (down ? backendDown() : null));
    const { result } = renderHook(() => useRunRows({
      lastRunId: 'r1', selectedRunId: 'r2', mode: 'run', onScheduler: vi.fn(),
    }));
    await advance(0);
    expect(result.current.tasks).toEqual([]);
    expect(result.current.readFailed).toBe(true);

    down = false;
    await advance(5000);
    expect(result.current.tasks.map((t) => t.table_name)).toEqual(['old_orders']);
    expect(result.current.readFailed).toBe(false);

    const before = callsTo(fetchMock, '/api/runs/r2/tasks');
    await advance(15000);
    expect(callsTo(fetchMock, '/api/runs/r2/tasks')).toBe(before);
    expect(callsTo(fetchMock, '/api/runs/r2')).toBe(0);
  });

  it('stops retrying the previous run when another is chosen', async () => {
    const fetchMock = stubBackend({
      '/api/runs/r3/tasks': () => tasksOf('third', 'failed'),
      '/api/runs/r3/attempts': () => attemptsOf('t-third'),
    }, () => null);
    fetchMock.mockImplementation(async (input: RequestInfo | URL) => {
      const path = String(input).split('?')[0];
      if (path.startsWith('/api/runs/r2')) return backendDown();
      return ok(path.endsWith('/tasks') ? tasksOf('third', 'failed') : attemptsOf('t-third'));
    });
    const { result, rerender } = renderHook(
      ({ selectedRunId }) => useRunRows({ lastRunId: 'r1', selectedRunId, mode: 'run', onScheduler: vi.fn() }),
      { initialProps: { selectedRunId: 'r2' } },
    );
    await advance(0);
    expect(result.current.readFailed).toBe(true);

    rerender({ selectedRunId: 'r3' });
    await advance(0);
    expect(result.current.readFailed).toBe(false);

    const before = callsTo(fetchMock, '/api/runs/r2/tasks');
    await advance(15000);
    expect(callsTo(fetchMock, '/api/runs/r2/tasks')).toBe(before);
  });
});
