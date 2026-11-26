// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { fetchNodeProjects } from './node-api';

describe('fetchNodeProjects', () => {
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ projects: ['billing', 'ledger'] }),
    });
    global.fetch = mockFetch as unknown as typeof fetch;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('resolves to the projects array from /api/nodes/projects', async () => {
    const result = await fetchNodeProjects();
    expect(mockFetch).toHaveBeenCalledWith('/api/nodes/projects');
    expect(result).toEqual(['billing', 'ledger']);
  });

  it('defaults a missing projects field to an empty array', async () => {
    mockFetch.mockResolvedValue({ ok: true, json: () => Promise.resolve({}) });
    expect(await fetchNodeProjects()).toEqual([]);
  });

  it('rejects on a non-ok response', async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 500, json: () => Promise.resolve({}) });
    await expect(fetchNodeProjects()).rejects.toThrow('HTTP 500');
  });
});
