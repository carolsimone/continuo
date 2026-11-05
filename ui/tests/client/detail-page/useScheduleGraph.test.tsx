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

  it('treats a non-2xx answer as a failed read, and reads again in run mode until one lands', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    let down = true;
    const fetchMock = vi.fn(async () => (down
      ? new Response(JSON.stringify({ error: '14 UNAVAILABLE' }), { status: 500 })
      : new Response(JSON.stringify({ nodes: [NODE], edges: [] }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);

    const { result } = renderHook(() => useScheduleGraph('s', 'run'));
    await advance(0);
    expect(result.current.graphState).toBe('error');
    expect(result.current.graph).toBeNull();

    down = false;
    await advance(5000);
    expect(result.current.graphState).toBe('ready');
    expect(result.current.graph?.nodes).toHaveLength(1);

    const before = fetchMock.mock.calls.length;
    await advance(15000);
    expect(fetchMock.mock.calls.length).toBe(before);
  });

  it('stays ready in run mode when a slower read fails after another one landed', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    let answerFirstRead: (response: Response) => void = () => {};
    const firstRead = new Promise<Response>((resolve) => { answerFirstRead = resolve; });
    const fetchMock = vi.fn()
      .mockImplementationOnce(() => firstRead)
      .mockImplementation(async () => new Response(JSON.stringify({ nodes: [NODE], edges: [] }), { status: 200 }));
    vi.stubGlobal('fetch', fetchMock);

    const { result } = renderHook(() => useScheduleGraph('s', 'run'));
    await advance(5000);
    expect(result.current.graphState).toBe('ready');

    answerFirstRead(new Response(JSON.stringify({ error: '14 UNAVAILABLE' }), { status: 500 }));
    await advance(0);
    expect(result.current.graphState).toBe('ready');
  });

  it('keeps the last graph when a poll of latest mode fails', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    let down = false;
    vi.stubGlobal('fetch', vi.fn(async () => (down
      ? new Response(JSON.stringify({ error: '14 UNAVAILABLE' }), { status: 500 })
      : new Response(JSON.stringify({ nodes: [NODE], edges: [] }), { status: 200 }))));

    const { result } = renderHook(() => useScheduleGraph('s', 'latest'));
    await advance(0);
    expect(result.current.graphState).toBe('ready');

    down = true;
    await advance(5000);
    expect(result.current.graphState).toBe('error');
    expect(result.current.graph?.nodes).toHaveLength(1);

    down = false;
    await advance(5000);
    expect(result.current.graphState).toBe('ready');
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
