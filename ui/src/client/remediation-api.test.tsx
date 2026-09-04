// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { fetchProposals, fetchNodeServices } from './remediation-api';

describe('fetchProposals — query string', () => {
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ proposals: [] }),
    });
    global.fetch = mockFetch as unknown as typeof fetch;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('fetches all proposals when called with no filter', async () => {
    await fetchProposals();
    expect(mockFetch).toHaveBeenCalledWith('/api/remediation/proposals');
  });

  it('adds pr_state to the query string', async () => {
    await fetchProposals({ pr_state: 'open' });
    expect(mockFetch).toHaveBeenCalledWith('/api/remediation/proposals?pr_state=open');
  });

  it('adds status to the query string', async () => {
    await fetchProposals({ status: 'proposed' });
    expect(mockFetch).toHaveBeenCalledWith('/api/remediation/proposals?status=proposed');
  });

  it('adds service to the query string', async () => {
    await fetchProposals({ service: 'billing' });
    expect(mockFetch).toHaveBeenCalledWith('/api/remediation/proposals?service=billing');
  });

  it('combines service with other filters', async () => {
    await fetchProposals({ status: 'proposed', service: 'ledger' });
    expect(mockFetch).toHaveBeenCalledWith('/api/remediation/proposals?status=proposed&service=ledger');
  });

  it('resolves to the proposals array', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ proposals: [{ id: 'p1' }] }),
    });
    const result = await fetchProposals({ pr_state: 'open' });
    expect(result).toEqual([{ id: 'p1' }]);
  });
});

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
