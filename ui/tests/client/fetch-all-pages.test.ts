import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { fetchAllPages, PAGE_SIZE, MAX_PAGES } from '../../src/client/fetch-all-pages';

const mockFetch = vi.fn();

function page(items: number, total: number) {
  return {
    ok: true,
    json: async () => ({
      total_count: total,
      tasks: Array.from({ length: items }, (_, i) => ({ id: i })),
    }),
  };
}

beforeEach(() => {
  mockFetch.mockReset();
  global.fetch = mockFetch as any;
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('fetchAllPages', () => {
  it('issues exactly one request when total_count fits in one page', async () => {
    mockFetch.mockResolvedValueOnce(page(10, 10));

    const out = await fetchAllPages<{ id: number }>('/api/x', 'tasks');

    expect(out).toHaveLength(10);
    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).toHaveBeenCalledWith(
      `/api/x?limit=${PAGE_SIZE}&offset=0`,
      expect.objectContaining({ signal: undefined })
    );
  });

  it('walks pages until total_count is reached', async () => {
    mockFetch
      .mockResolvedValueOnce(page(200, 412))
      .mockResolvedValueOnce(page(200, 412))
      .mockResolvedValueOnce(page(12, 412));

    const out = await fetchAllPages<{ id: number }>('/api/x', 'tasks');

    expect(out).toHaveLength(412);
    expect(mockFetch).toHaveBeenCalledTimes(3);
    expect(mockFetch.mock.calls[0][0]).toBe('/api/x?limit=200&offset=0');
    expect(mockFetch.mock.calls[1][0]).toBe('/api/x?limit=200&offset=200');
    expect(mockFetch.mock.calls[2][0]).toBe('/api/x?limit=200&offset=400');
  });

  it('stops when a page returns zero rows even if total_count disagrees', async () => {
    mockFetch
      .mockResolvedValueOnce(page(200, 9999))
      .mockResolvedValueOnce(page(0, 9999));

    const out = await fetchAllPages<{ id: number }>('/api/x', 'tasks');

    expect(out).toHaveLength(200);
    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it('honors the page cap when total_count is absurdly large', async () => {
    mockFetch.mockResolvedValue(page(200, 1_000_000));

    const out = await fetchAllPages<{ id: number }>('/api/x', 'tasks');

    expect(mockFetch).toHaveBeenCalledTimes(MAX_PAGES);
    expect(out).toHaveLength(MAX_PAGES * PAGE_SIZE);
  });

  it('stops fetching once the signal is aborted', async () => {
    const controller = new AbortController();
    mockFetch.mockImplementation(async () => {
      controller.abort();
      return page(200, 412);
    });

    const out = await fetchAllPages<{ id: number }>('/api/x', 'tasks', {
      signal: controller.signal,
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(out).toHaveLength(200);
  });

  it('throws when a response is not ok', async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 404, json: async () => ({}) });

    await expect(fetchAllPages('/api/x', 'tasks')).rejects.toThrow('404');
  });
});
