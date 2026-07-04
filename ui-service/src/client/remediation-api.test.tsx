// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { fetchProposals } from './remediation-api';

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

  it('resolves to the proposals array', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ proposals: [{ id: 'p1' }] }),
    });
    const result = await fetchProposals({ pr_state: 'open' });
    expect(result).toEqual([{ id: 'p1' }]);
  });
});
