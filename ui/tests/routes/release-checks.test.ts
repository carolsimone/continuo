import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import request from 'supertest';
import { createReleaseChecksRouter } from '../../src/server/routes/release-checks';

function appWith(client: any) {
  const app = express();
  app.use('/api/release-checks', createReleaseChecksRouter(client));
  return app;
}

describe('release checks router', () => {
  it('proxies a release check', async () => {
    const client = { getReleaseCheck: vi.fn().mockResolvedValue({ check_id: 'verify-1', status: 'running' }) };
    const res = await request(appWith(client)).get('/api/release-checks/verify-1');
    expect(res.status).toBe(200);
    expect(res.body.check_id).toBe('verify-1');
    expect(client.getReleaseCheck).toHaveBeenCalledWith('verify-1');
  });

  it('passes the upstream status through', async () => {
    const client = { getReleaseCheck: vi.fn().mockRejectedValue(Object.assign(new Error('nf'), { status: 404 })) };
    const res = await request(appWith(client)).get('/api/release-checks/nope');
    expect(res.status).toBe(404);
  });
});
