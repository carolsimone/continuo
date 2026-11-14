// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useSingleNodeRun } from '../../src/client/useSingleNodeRun';

const mockFetch = vi.fn();
beforeEach(() => {
  mockFetch.mockReset();
  vi.stubGlobal('fetch', mockFetch);
  // Only the 3 s that return a started press to idle are driven by hand.
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
});
afterEach(() => { vi.useRealTimers(); });

const started = () => ({ ok: true, status: 200, json: async () => ({ run_id: 'r' }) } as Response);

function renderOnTable(table: string) {
  return renderHook(
    ({ table }) => useSingleNodeRun({ service: 'svc', schema: 'schema', table }, { operation: 'run' }),
    { initialProps: { table } },
  );
}

describe('useSingleNodeRun — press state', () => {
  it('does not let the idle timer of an earlier press end a later press that is in flight', async () => {
    let answerSecond!: (r: Response) => void;
    mockFetch
      .mockImplementationOnce(() => Promise.resolve(started()))
      .mockImplementationOnce(() => new Promise<Response>(res => { answerSecond = res; }));
    const { result } = renderOnTable('a');

    await act(async () => { await result.current.postRun({}); });
    expect(result.current.runState).toBe('success');

    let second!: Promise<string | null>;
    act(() => { second = result.current.postRun({}); });
    expect(result.current.runState).toBe('loading');

    await act(async () => { await vi.advanceTimersByTimeAsync(3000); });
    expect(result.current.runState).toBe('loading');

    await act(async () => { answerSecond(started()); await second; });
    expect(result.current.runState).toBe('success');
  });

  it('shows idle for a node with no press, and the press of a node again once the component comes back to it', async () => {
    mockFetch.mockImplementation(() => Promise.resolve(started()));
    const { result, rerender } = renderOnTable('a');

    await act(async () => { await result.current.postRun({}); });
    expect(result.current.runState).toBe('success');

    rerender({ table: 'b' });
    expect(result.current.runState).toBe('idle');

    await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
    rerender({ table: 'a' });
    // Its success has 2 s left.
    expect(result.current.runState).toBe('success');

    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(result.current.runState).toBe('idle');
  });
});

describe('useSingleNodeRun — request id', () => {
  // The request id each POST carried, in order.
  const sentIds = () => (mockFetch.mock.calls as unknown as [string, RequestInit][])
    .map(([, init]) => JSON.parse(String(init.body)).request_id as string);

  it('sends a new id for a press made after a started press, since the answer settled the first', async () => {
    mockFetch.mockImplementation(() => Promise.resolve(started()));
    const { result } = renderOnTable('a');

    await act(async () => { await result.current.postRun({}); });
    await act(async () => { await vi.advanceTimersByTimeAsync(3000); });
    expect(result.current.runState).toBe('idle');
    await act(async () => { await result.current.postRun({}); });

    const ids = sentIds();
    expect(ids).toHaveLength(2);
    expect(ids[1]).not.toBe(ids[0]);
  });

  it('sends a new id for a press made after a started press when the component showed another node in between', async () => {
    mockFetch.mockImplementation(() => Promise.resolve(started()));
    const { result, rerender } = renderOnTable('a');

    await act(async () => { await result.current.postRun({}); });
    rerender({ table: 'b' });
    await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
    rerender({ table: 'a' });
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(result.current.runState).toBe('idle');
    await act(async () => { await result.current.postRun({}); });

    const ids = sentIds();
    expect(ids).toHaveLength(2);
    expect(ids[1]).not.toBe(ids[0]);
  });

  it('settles the id of a press whose started answer comes while another node is shown, so the next press of its node is a new action', async () => {
    let answerFirst!: (r: Response) => void;
    mockFetch
      .mockImplementationOnce(() => new Promise<Response>(res => { answerFirst = res; }))
      .mockImplementation(() => Promise.resolve(started()));
    const { result, rerender } = renderOnTable('a');

    let first!: Promise<string | null>;
    act(() => { first = result.current.postRun({}); });
    rerender({ table: 'b' });
    await act(async () => { answerFirst(started()); await first; });
    rerender({ table: 'a' });
    await act(async () => { await vi.advanceTimersByTimeAsync(3000); });
    expect(result.current.runState).toBe('idle');
    await act(async () => { await result.current.postRun({}); });

    const ids = sentIds();
    expect(ids).toHaveLength(2);
    expect(ids[1]).not.toBe(ids[0]);
  });

  it('keeps the id of a press the server answered with a 5xx, for the press repeated on the same node', async () => {
    mockFetch
      .mockImplementationOnce(() => Promise.resolve({ ok: false, status: 502, json: async () => ({ error: 'bad gateway' }) } as Response))
      .mockImplementation(() => Promise.resolve(started()));
    const { result } = renderOnTable('a');

    await act(async () => { await result.current.postRun({}); });
    await act(async () => { await result.current.postRun({}); });

    const ids = sentIds();
    expect(ids).toHaveLength(2);
    expect(ids[1]).toBe(ids[0]);
  });
});
