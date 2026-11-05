// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useSelectedRunGraph } from '../../../src/client/detail-page/useSelectedRunGraph';

const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

const ok = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });
const backendDown = () => new Response(JSON.stringify({ error: '14 UNAVAILABLE' }), { status: 500 });
const graphOf = (run: string) => ({ nodes: [], edges: [], run_promotion_seq: 1, latest_promotion_seq: 1, run });

describe('useSelectedRunGraph', () => {
  beforeEach(() => { vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] }); });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('reads nothing with no run chosen', async () => {
    const fetchMock = vi.fn(async () => ok({}));
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useSelectedRunGraph(null));
    await advance(0);
    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current).toEqual({ runGraph: null, readFailed: false });
  });

  it('empties the graph while another chosen run is read', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => ok(graphOf(String(input).split('/')[3]))));
    const { result, rerender } = renderHook(
      ({ id }) => useSelectedRunGraph(id),
      { initialProps: { id: 'r1' } },
    );
    await advance(0);
    expect(result.current.runGraph).toMatchObject({ run: 'r1' });

    rerender({ id: 'r2' });
    expect(result.current.runGraph).toBeNull();
    await advance(0);
    expect(result.current.runGraph).toMatchObject({ run: 'r2' });
  });

  it('never stores an error body as the graph, and reads again until one lands', async () => {
    let down = true;
    vi.stubGlobal('fetch', vi.fn(async () => (down ? backendDown() : ok(graphOf('r1')))));
    const { result } = renderHook(() => useSelectedRunGraph('r1'));
    await advance(0);
    expect(result.current).toEqual({ runGraph: null, readFailed: true });

    down = false;
    await advance(5000);
    expect(result.current.runGraph).toMatchObject({ run: 'r1' });
    expect(result.current.readFailed).toBe(false);
  });
});
