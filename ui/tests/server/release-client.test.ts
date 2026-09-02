import { describe, it, expect, vi, afterEach } from 'vitest';
import { createReleaseClient } from '../../src/server/release-client';

afterEach(() => vi.restoreAllMocks());

describe('release-client', () => {
  it('lists releases with query params', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ releases: [], next_cursor: '' }), { status: 200 }),
    );
    vi.stubGlobal('fetch', fetchMock);
    const client = createReleaseClient('http://rc:8088');
    const out = await client.listReleases({ status: 'promoted', limit: '5' });
    expect(out.releases).toEqual([]);
    const calledUrl = fetchMock.mock.calls[0][0] as string;
    expect(calledUrl).toContain('http://rc:8088/releases?');
    expect(calledUrl).toContain('status=promoted');
    expect(calledUrl).toContain('limit=5');
  });

  it('throws with upstream status on non-2xx', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('nope', { status: 404 })));
    const client = createReleaseClient('http://rc:8088');
    await expect(client.getRelease('missing')).rejects.toThrow(/404/);
  });

  it('posts to retry-remediation and passes status and body through, even on 4xx', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ error: 'rounds_exhausted' }), { status: 409 }),
    );
    vi.stubGlobal('fetch', fetchMock);
    const client = createReleaseClient('http://rc:8088');
    const out = await client.retryRemediation('rel-1');
    expect(out).toEqual({ status: 409, body: { error: 'rounds_exhausted' } });
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe('http://rc:8088/releases/rel-1/retry-remediation');
    expect(init).toEqual({ method: 'POST' });
  });

  it('reports invalid_response when retry-remediation answers with a non-JSON body', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('not json', { status: 202 })));
    const client = createReleaseClient('http://rc:8088');
    const out = await client.retryRemediation('rel-1');
    expect(out).toEqual({ status: 202, body: { error: 'invalid_response' } });
  });

  it('reads a verification run, a release\'s verification runs, and the pipeline', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ run_id: 'verify-1', status: 'passed' }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ runs: [] }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ active: null }), { status: 200 }));
    vi.stubGlobal('fetch', fetchMock);
    const client = createReleaseClient('http://rc:8088');
    expect((await client.getVerificationRun('verify-1')).status).toBe('passed');
    expect((await client.listVerificationRuns('rel-1')).runs).toEqual([]);
    expect((await client.getPipeline()).active).toBeNull();
    expect(fetchMock.mock.calls[0][0]).toBe('http://rc:8088/verification-runs/verify-1');
    expect(fetchMock.mock.calls[1][0]).toBe('http://rc:8088/verification-runs?verifies=rel-1');
    expect(fetchMock.mock.calls[2][0]).toBe('http://rc:8088/pipeline');
  });
});
