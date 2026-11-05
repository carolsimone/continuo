// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useRetriedRead } from '../../../src/client/detail-page/useRetriedRead';

// Only the interval is faked, and the reads run through act: waitFor polls
// through setInterval, so it cannot be used here.
const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

const ok = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });
const backendDown = () => new Response(JSON.stringify({ error: '14 UNAVAILABLE' }), { status: 500 });

describe('useRetriedRead', () => {
  beforeEach(() => { vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] }); });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('reads once when the first read lands', async () => {
    const fetchMock = vi.fn(async () => ok({ n: 1 }));
    vi.stubGlobal('fetch', fetchMock);
    const onData = vi.fn();
    const { result } = renderHook(() => useRetriedRead('/x', onData));
    await advance(0);

    expect(onData).toHaveBeenCalledWith({ n: 1 });
    expect(result.current).toBe(false);
    await advance(15000);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('reads nothing for a null url', async () => {
    const fetchMock = vi.fn(async () => ok({}));
    vi.stubGlobal('fetch', fetchMock);
    renderHook(() => useRetriedRead(null, vi.fn()));
    await advance(15000);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('reports a failed read, reads again every 5 s, and stops once one lands', async () => {
    let down = true;
    const fetchMock = vi.fn(async () => (down ? backendDown() : ok({ n: 1 })));
    vi.stubGlobal('fetch', fetchMock);
    const onData = vi.fn();
    const { result } = renderHook(() => useRetriedRead('/x', onData));
    await advance(0);

    expect(result.current).toBe(true);
    expect(onData).not.toHaveBeenCalled();

    await advance(5000);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(result.current).toBe(true);

    down = false;
    await advance(5000);
    expect(onData).toHaveBeenCalledTimes(1);
    expect(result.current).toBe(false);

    await advance(15000);
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it('retries a read that the network rejected', async () => {
    let down = true;
    vi.stubGlobal('fetch', vi.fn(async () => {
      if (down) throw new TypeError('Failed to fetch');
      return ok({ n: 1 });
    }));
    const onData = vi.fn();
    const { result } = renderHook(() => useRetriedRead('/x', onData));
    await advance(0);
    expect(result.current).toBe(true);

    down = false;
    await advance(5000);
    expect(onData).toHaveBeenCalledWith({ n: 1 });
    expect(result.current).toBe(false);
  });

  it('stays settled when a slower read fails after another one landed', async () => {
    let failSlowRead: (response: Response) => void = () => {};
    const slowRead = new Promise<Response>((resolve) => { failSlowRead = resolve; });
    const fetchMock = vi.fn()
      .mockImplementationOnce(async () => backendDown())
      .mockImplementationOnce(() => slowRead)
      .mockImplementation(async () => ok({ n: 1 }));
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderHook(() => useRetriedRead('/x', vi.fn()));
    await advance(5000);
    await advance(5000);
    expect(result.current).toBe(false);

    failSlowRead(backendDown());
    await advance(0);
    expect(result.current).toBe(false);
  });

  it('starts over for a new url', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => (
      String(input) === '/a' ? backendDown() : ok({ url: String(input) })
    ));
    vi.stubGlobal('fetch', fetchMock);
    const onData = vi.fn();
    const { result, rerender } = renderHook(
      ({ url }) => useRetriedRead(url, onData),
      { initialProps: { url: '/a' } },
    );
    await advance(0);
    expect(result.current).toBe(true);

    rerender({ url: '/b' });
    await advance(0);
    expect(onData).toHaveBeenCalledWith({ url: '/b' });
    expect(result.current).toBe(false);

    await advance(15000);
    expect(fetchMock.mock.calls.filter(([u]) => String(u) === '/a')).toHaveLength(1);
  });

  it('stops reading when unmounted', async () => {
    const fetchMock = vi.fn(async () => backendDown());
    vi.stubGlobal('fetch', fetchMock);
    const { unmount } = renderHook(() => useRetriedRead('/x', vi.fn()));
    await advance(0);
    unmount();
    await advance(15000);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
