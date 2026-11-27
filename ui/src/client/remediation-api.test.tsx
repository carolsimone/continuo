// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { fetchFixes } from './remediation-api';

describe('fetchFixes — query string', () => {
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ fixes: [] }),
    });
    global.fetch = mockFetch as unknown as typeof fetch;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('fetches all fixes when called with no filter', async () => {
    await fetchFixes();
    expect(mockFetch).toHaveBeenCalledWith('/api/remediation/fixes');
  });

  it('adds pr_state to the query string', async () => {
    await fetchFixes({ pr_state: 'open' });
    expect(mockFetch).toHaveBeenCalledWith('/api/remediation/fixes?pr_state=open');
  });

  it('adds status to the query string', async () => {
    await fetchFixes({ status: 'proposed' });
    expect(mockFetch).toHaveBeenCalledWith('/api/remediation/fixes?status=proposed');
  });

  it('adds project to the query string', async () => {
    await fetchFixes({ project: 'billing' });
    expect(mockFetch).toHaveBeenCalledWith('/api/remediation/fixes?project=billing');
  });

  it('combines project with other filters', async () => {
    await fetchFixes({ status: 'proposed', project: 'ledger' });
    expect(mockFetch).toHaveBeenCalledWith('/api/remediation/fixes?status=proposed&project=ledger');
  });

  it('resolves to the fixes array', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ fixes: [{ id: 'p1' }] }),
    });
    const result = await fetchFixes({ pr_state: 'open' });
    expect(result).toEqual([{ id: 'p1' }]);
  });
});
