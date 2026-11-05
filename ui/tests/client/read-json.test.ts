import { describe, it, expect, vi, afterEach } from 'vitest';
import { readJson } from '../../src/client/read-json';

const answer = (response: Response) => vi.stubGlobal('fetch', vi.fn(async () => response));

describe('readJson', () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it('returns the body of a 2xx answer', async () => {
    answer(new Response(JSON.stringify({ status: 'running' }), { status: 200 }));
    await expect(readJson('/api/runs/r1')).resolves.toEqual({ status: 'running' });
  });

  it('rejects a non-2xx answer rather than returning its error body', async () => {
    answer(new Response(JSON.stringify({ error: '14 UNAVAILABLE' }), { status: 500 }));
    await expect(readJson('/api/runs/r1')).rejects.toThrow('/api/runs/r1 responded 500');
  });

  it('rejects a 2xx answer that is not JSON', async () => {
    answer(new Response('<html></html>', { status: 200 }));
    await expect(readJson('/api/runs/r1')).rejects.toThrow();
  });
});
