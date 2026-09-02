import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import request from 'supertest';
import { createVerificationsRouter } from '../../src/server/routes/verifications';

function appWith(client: any) {
  const app = express();
  app.use('/api/verifications', createVerificationsRouter(client));
  return app;
}

describe('verifications router', () => {
  it('proxies a verification run', async () => {
    const client = { getVerificationRun: vi.fn().mockResolvedValue({ run_id: 'verify-1', status: 'running' }) };
    const res = await request(appWith(client)).get('/api/verifications/verify-1');
    expect(res.status).toBe(200);
    expect(res.body.run_id).toBe('verify-1');
    expect(client.getVerificationRun).toHaveBeenCalledWith('verify-1');
  });

  it('passes the upstream status through', async () => {
    const client = { getVerificationRun: vi.fn().mockRejectedValue(Object.assign(new Error('nf'), { status: 404 })) };
    const res = await request(appWith(client)).get('/api/verifications/nope');
    expect(res.status).toBe(404);
  });
});
