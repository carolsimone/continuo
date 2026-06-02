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
});
