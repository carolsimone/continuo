import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import request from 'supertest';
import { createReleasesRouter } from '../../src/server/routes/releases';

function appWith(deps: any) {
  const app = express();
  app.use(express.json());
  app.use('/api/releases', createReleasesRouter(deps.client, deps.getLog));
  return app;
}

describe('releases router', () => {
  it('proxies the list', async () => {
    const client = { listReleases: vi.fn().mockResolvedValue({ releases: [{ release_id: 'r1' }], next_cursor: '' }) };
    const app = appWith({ client, getLog: vi.fn() });
    const res = await request(app).get('/api/releases?status=promoted');
    expect(res.status).toBe(200);
    expect(res.body.releases[0].release_id).toBe('r1');
    expect(client.listReleases).toHaveBeenCalledWith(expect.objectContaining({ status: 'promoted' }));
  });

  it('streams a per-node log by key', async () => {
    const getLog = vi.fn().mockResolvedValue('LOG CONTENT');
    const app = appWith({ client: {}, getLog });
    const res = await request(app).get('/api/releases/log?key=k/a.log');
    expect(res.status).toBe(200);
    expect(res.text).toBe('LOG CONTENT');
    expect(getLog).toHaveBeenCalledWith('k/a.log');
  });

  it('400s when the log key is missing', async () => {
    const app = appWith({ client: {}, getLog: vi.fn() });
    const res = await request(app).get('/api/releases/log');
    expect(res.status).toBe(400);
  });

  it('proxies release detail by id', async () => {
    const client = { getRelease: vi.fn().mockResolvedValue({ release_id: 'rX', status: 'rejected' }) };
    const app = appWith({ client, getLog: vi.fn() });
    const res = await request(app).get('/api/releases/rX');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('rejected');
    expect(client.getRelease).toHaveBeenCalledWith('rX');
  });

  it('maps upstream error status (404) to the response', async () => {
    const err: any = new Error('release-controller /releases/missing -> 404');
    err.status = 404;
    const client = { getRelease: vi.fn().mockRejectedValue(err) };
    const app = appWith({ client, getLog: vi.fn() });
    const res = await request(app).get('/api/releases/missing');
    expect(res.status).toBe(404);
    expect(res.body.error).not.toContain('release-controller /releases'); // no internal detail leaked
  });

  it('POST /api/releases/:id/retry-remediation passes release-controller status and body through', async () => {
    const client = { retryRemediation: vi.fn().mockResolvedValue({ status: 409, body: { error: 'proposal_open', pr_url: 'https://x/pr/7' } }) };
    const app = appWith({ client, getLog: vi.fn() });
    const res = await request(app).post('/api/releases/rel-1/retry-remediation');
    expect(res.status).toBe(409);
    expect(res.body).toEqual({ error: 'proposal_open', pr_url: 'https://x/pr/7' });
    expect(client.retryRemediation).toHaveBeenCalledWith('rel-1');
  });

  it('POST /api/releases/:id/retry-remediation answers 502 when release-controller is unreachable', async () => {
    const client = { retryRemediation: vi.fn().mockRejectedValue(new Error('ECONNREFUSED')) };
    const app = appWith({ client, getLog: vi.fn() });
    const res = await request(app).post('/api/releases/rel-1/retry-remediation');
    expect(res.status).toBe(502);
  });
});
