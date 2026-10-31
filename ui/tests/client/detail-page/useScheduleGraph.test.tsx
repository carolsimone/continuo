// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { useScheduleGraph } from '../../../src/client/detail-page/useScheduleGraph';

function stubGraphFetch(reply: () => Promise<unknown>) {
  const fetchMock = vi.fn(async () => ({ ok: true, status: 200, json: reply }) as Response);
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

// Fake timers stop waitFor from polling (it polls through setInterval), so a
// test that fakes them lets the timers and the mocked reads run through act.
const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

const NODE = { node_id: 'svc.sch.a', node_type: 'dbt-model', schedule_name: 's' };

describe('useScheduleGraph', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('reads the graph once in run mode and every 5 s in latest mode', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    const fetchMock = stubGraphFetch(async () => ({ nodes: [NODE], edges: [] }));

    const run = renderHook(() => useScheduleGraph('s', 'run'));
    await advance(0);
    expect(run.result.current.graph).not.toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith('/api/schedules/s/graph');
    await advance(5000);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    run.unmount();

    fetchMock.mockClear();
    const latest = renderHook(() => useScheduleGraph('s', 'latest'));
    await advance(0);
    expect(latest.result.current.graph).not.toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await advance(5000);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    latest.unmount();
  });

  it("reports 'empty' for a graph with no nodes and 'error' for a failed read", async () => {
    stubGraphFetch(async () => ({ nodes: [], edges: [] }));
    const empty = renderHook(() => useScheduleGraph('s', 'run'));
    expect(empty.result.current.graphState).toBe('loading');
    await waitFor(() => expect(empty.result.current.graphState).toBe('empty'));

    stubGraphFetch(async () => { throw new Error('read failed'); });
    const failed = renderHook(() => useScheduleGraph('s', 'run'));
    await waitFor(() => expect(failed.result.current.graphState).toBe('error'));
    expect(failed.result.current.graph).toBeNull();
  });

  it('reports promotionSeq from the response', async () => {
    stubGraphFetch(async () => ({ nodes: [NODE], edges: [], promotion_seq: 5 }));
    const { result } = renderHook(() => useScheduleGraph('s', 'run'));
    expect(result.current.promotionSeq).toBe(0);
    await waitFor(() => expect(result.current.promotionSeq).toBe(5));
    expect(result.current.graphState).toBe('ready');
  });

  it('reads nothing without a schedule name', () => {
    const fetchMock = stubGraphFetch(async () => ({ nodes: [], edges: [] }));
    renderHook(() => useScheduleGraph(undefined, 'run'));
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
