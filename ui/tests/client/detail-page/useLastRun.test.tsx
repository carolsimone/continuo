// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useLastRun } from '../../../src/client/detail-page/useLastRun';

const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

const ok = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });
const backendDown = () => new Response(JSON.stringify({ error: '14 UNAVAILABLE' }), { status: 500 });

const run = { run_id: 'r1', schedule_name: 's', status: 'running' };
const graph = { nodes: [], edges: [], run_promotion_seq: 1, latest_promotion_seq: 1 };

function stubBackend(isDown: () => boolean) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    if (isDown()) return backendDown();
    const url = String(input);
    if (url === '/api/schedules') return ok({ schedules: [{ schedule_name: 's', last_run_id: 'r1' }] });
    if (url === '/api/runs/r1') return ok(run);
    if (url === '/api/runs/r1/graph') return ok(graph);
    return ok({});
  });
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

describe('useLastRun', () => {
  beforeEach(() => { vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] }); });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('skips the schedule list when the dashboard passed the last run', async () => {
    const fetchMock = stubBackend(() => false);
    const { result } = renderHook(() => useLastRun('s', { last_run_id: 'r1' }));
    await advance(0);

    expect(fetchMock.mock.calls.some(([u]) => String(u) === '/api/schedules')).toBe(false);
    expect(result.current.scheduler?.status).toBe('running');
    expect(result.current.liveRunGraph).toEqual(graph);
    expect(result.current.readFailed).toBe(false);
  });

  it('takes a null last run from the dashboard as a schedule that never ran', async () => {
    const fetchMock = stubBackend(() => false);
    const { result } = renderHook(() => useLastRun('s', { last_run_id: null }));
    await advance(0);

    expect(result.current.lastRunId).toBeNull();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('resolves the last run from the schedule list when opened without one', async () => {
    stubBackend(() => false);
    const { result } = renderHook(() => useLastRun('s', null));
    expect(result.current.lastRunId).toBeUndefined();
    await advance(0);
    expect(result.current.lastRunId).toBe('r1');
  });

  it('does not take a failed read of the schedule list as a schedule that never ran', async () => {
    let down = true;
    stubBackend(() => down);
    const { result } = renderHook(() => useLastRun('s', null));
    await advance(0);

    expect(result.current.lastRunId).toBeUndefined();
    expect(result.current.readFailed).toBe(true);

    down = false;
    await advance(5000);
    expect(result.current.lastRunId).toBe('r1');
  });

  it('does not replace a status the page already holds with the arrival read landing late', async () => {
    let answerStatus: (response: Response) => void = () => {};
    const slowStatus = new Promise<Response>((resolve) => { answerStatus = resolve; });
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => (
      String(input) === '/api/runs/r1' ? slowStatus : ok(graph)
    )));
    const { result } = renderHook(() => useLastRun('s', { last_run_id: 'r1' }));
    await advance(0);

    act(() => result.current.setScheduler({ ...run, status: 'succeeded' } as never));
    answerStatus(ok(run));
    await advance(0);
    expect(result.current.scheduler?.status).toBe('succeeded');
  });

  it('never stores an error body as the run or its graph, and reads both again', async () => {
    let down = true;
    stubBackend(() => down);
    const { result } = renderHook(() => useLastRun('s', { last_run_id: 'r1' }));
    await advance(0);

    expect(result.current.scheduler).toBeNull();
    expect(result.current.liveRunGraph).toBeNull();
    expect(result.current.readFailed).toBe(true);

    down = false;
    await advance(5000);
    expect(result.current.scheduler?.status).toBe('running');
    expect(result.current.liveRunGraph).toEqual(graph);
    expect(result.current.readFailed).toBe(false);
  });
});
