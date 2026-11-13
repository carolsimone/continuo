// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { fetchNodeServices } from './node-api';

describe('fetchNodeServices', () => {
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ services: ['billing', 'ledger'] }),
    });
    global.fetch = mockFetch as unknown as typeof fetch;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('resolves to the services array from /api/nodes/services', async () => {
    const result = await fetchNodeServices();
    expect(mockFetch).toHaveBeenCalledWith('/api/nodes/services');
    expect(result).toEqual(['billing', 'ledger']);
  });

  it('defaults a missing services field to an empty array', async () => {
    mockFetch.mockResolvedValue({ ok: true, json: () => Promise.resolve({}) });
    expect(await fetchNodeServices()).toEqual([]);
  });

  it('rejects on a non-ok response', async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 500, json: () => Promise.resolve({}) });
    await expect(fetchNodeServices()).rejects.toThrow('HTTP 500');
  });
});
