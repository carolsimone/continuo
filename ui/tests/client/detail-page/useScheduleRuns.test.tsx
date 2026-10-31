// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { RUNS_PAGE_SIZE, useScheduleRuns } from '../../../src/client/detail-page/useScheduleRuns';

const run = (id: string) => ({ run_id: id, schedule_name: 's', status: 'succeeded', created_at: null, completed_at: null });

function stubRunsFetch(reply: (url: string) => unknown) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => (
    { ok: true, status: 200, json: async () => reply(String(input)) } as Response
  ));
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

describe('useScheduleRuns', () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it('reads the first page and counts the full history from total_count', async () => {
    const fetchMock = stubRunsFetch(() => ({ runs: [run('r1')], total_count: 25 }));
    const { result } = renderHook(() => useScheduleRuns('s'));
    await waitFor(() => expect(result.current.runs).toHaveLength(1));
    expect(fetchMock).toHaveBeenCalledWith(`/api/schedules/s/runs?limit=${RUNS_PAGE_SIZE}&offset=0`);
    expect(result.current.runsTotal).toBe(25);
    expect(result.current.runsCount).toBe(25);
  });

  it('counts the rows read when the answer carries no total_count', async () => {
    stubRunsFetch(() => ({ runs: [run('r1'), run('r2')] }));
    const { result } = renderHook(() => useScheduleRuns('s'));
    await waitFor(() => expect(result.current.runs).toHaveLength(2));
    expect(result.current.runsTotal).toBeNull();
    expect(result.current.runsCount).toBe(2);
  });

  it('reads the next page when the page changes', async () => {
    const fetchMock = stubRunsFetch(() => ({ runs: [run('r1')], total_count: 25 }));
    const { result } = renderHook(() => useScheduleRuns('s'));
    await waitFor(() => expect(result.current.runs).toHaveLength(1));
    act(() => result.current.setRunsPage(1));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(`/api/schedules/s/runs?limit=${RUNS_PAGE_SIZE}&offset=${RUNS_PAGE_SIZE}`));
    expect(result.current.runsPage).toBe(1);
  });

  it('shows no runs when the read fails', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => { throw new Error('read failed'); }));
    const { result } = renderHook(() => useScheduleRuns('s'));
    await act(async () => { await new Promise((r) => setTimeout(r, 0)); });
    expect(result.current.runs).toEqual([]);
    expect(result.current.runsCount).toBe(0);
  });
});
