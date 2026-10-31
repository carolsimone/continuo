// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useTriggerRun } from '../../../src/client/detail-page/useTriggerRun';
import { useActionRequestId } from '../../../src/client/request-id';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function reply(status: number, json: () => Promise<unknown>) {
  return { ok: status >= 200 && status < 300, status, json } as Response;
}

// The hook is used with the request id of a page that outlives it.
function renderTrigger(name: string | undefined, observation: string | null | undefined) {
  return renderHook(() => useTriggerRun(name, observation, useActionRequestId()));
}

const press = (trigger: () => Promise<void>) => act(async () => { await trigger(); });
const sentIds = (fetchMock: ReturnType<typeof vi.fn>) =>
  fetchMock.mock.calls.map((c) => JSON.parse(String((c[1] as RequestInit).body)).request_id as string);

describe('useTriggerRun', () => {
  beforeEach(() => { vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] }); });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('posts the request id and reports success, then idle after 3 s', async () => {
    const fetchMock = vi.fn(async () => reply(200, async () => ({})));
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderTrigger('hourly events', 'run-1');
    expect(result.current.triggerState).toBe('idle');

    await press(result.current.trigger);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe('/api/schedules/hourly%20events/trigger');
    expect(init.method).toBe('POST');
    expect(init.headers).toEqual({ 'Content-Type': 'application/json' });
    expect(sentIds(fetchMock)[0]).toMatch(UUID);
    expect(result.current.triggerState).toBe('success');
    expect(result.current.triggerError).toBeNull();

    await act(async () => { await vi.advanceTimersByTimeAsync(3000); });
    expect(result.current.triggerState).toBe('idle');
  });

  it("reports the server's error text", async () => {
    vi.stubGlobal('fetch', vi.fn(async () => reply(409, async () => ({ error: 'a run is already active' }))));
    const { result } = renderTrigger('s', 'run-1');

    await press(result.current.trigger);

    expect(result.current.triggerState).toBe('error');
    expect(result.current.triggerError).toBe('a run is already active');
  });

  it('reports the generic text when the response is not JSON', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => reply(502, async () => { throw new Error('not json'); })));
    const { result } = renderTrigger('s', 'run-1');

    await press(result.current.trigger);

    expect(result.current.triggerState).toBe('error');
    expect(result.current.triggerError).toBe('Request failed — please try again');
  });

  it('reports the generic text when the request cannot be sent', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => { throw new Error('offline'); }));
    const { result } = renderTrigger('s', 'run-1');

    await press(result.current.trigger);

    expect(result.current.triggerState).toBe('error');
    expect(result.current.triggerError).toBe('Request failed — please try again');
  });

  it('sends the same request id for a repeat press while no newer run was observed', async () => {
    const answers = [reply(503, async () => ({})), reply(503, async () => ({})), reply(200, async () => ({}))];
    const fetchMock = vi.fn(async () => answers.shift() as Response);
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderTrigger('s', 'run-1');

    // A 503 may be a timed-out call whose command committed: the retry repeats the id.
    await press(result.current.trigger);
    await press(result.current.trigger);
    await press(result.current.trigger);

    const [first, second, third] = sentIds(fetchMock);
    expect(second).toBe(first);
    expect(third).toBe(first);
  });

  it('sends a new request id once the previous press was answered', async () => {
    const fetchMock = vi.fn(async () => reply(200, async () => ({})));
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderTrigger('s', 'run-1');

    await press(result.current.trigger);
    await act(async () => { await vi.advanceTimersByTimeAsync(3000); });
    await press(result.current.trigger);

    const [first, second] = sentIds(fetchMock);
    expect(second).not.toBe(first);
  });

  it('sends nothing without a schedule name', async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    const { result } = renderTrigger(undefined, undefined);

    await press(result.current.trigger);

    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current.triggerState).toBe('idle');
  });
});
