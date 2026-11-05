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

  // A fetch that answers only by rejecting when its request is aborted.
  const neverAnswers = () => vi.fn((_url: string, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
    init?.signal?.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')));
  }));

  it('aborts and rejects a read that has not answered after 10 s', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    try {
      const fetchMock = neverAnswers();
      vi.stubGlobal('fetch', fetchMock);
      const settled = vi.fn();
      const read = readJson('/api/runs/r1').then(settled, settled);

      await vi.advanceTimersByTimeAsync(9999);
      expect(settled).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      await read;
      expect(settled.mock.calls[0][0]).toBeInstanceOf(Error);
      expect(fetchMock.mock.calls[0][1]?.signal?.aborted).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it('aborts the read when the caller aborts', async () => {
    vi.stubGlobal('fetch', neverAnswers());
    const controller = new AbortController();
    const read = readJson('/api/runs/r1', controller.signal);
    controller.abort();
    await expect(read).rejects.toThrow();
  });

  it('rejects a 2xx answer that is not JSON', async () => {
    answer(new Response('<html></html>', { status: 200 }));
    await expect(readJson('/api/runs/r1')).rejects.toThrow();
  });
});
