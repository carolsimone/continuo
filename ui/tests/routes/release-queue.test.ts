import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import request from 'supertest';
import { createReleaseQueueRouter } from '../../src/server/routes/release-queue';

describe('release queue router', () => {
  it('proxies the active release check', async () => {
    const client = { getReleaseQueue: vi.fn().mockResolvedValue({ active: { check_id: 'verify-1', check_kind: 'fix', status: 'compiling' } }) };
    const app = express();
    app.use('/api/release-queue', createReleaseQueueRouter(client as any));
    const res = await request(app).get('/api/release-queue');
    expect(res.status).toBe(200);
    expect(res.body.active.check_kind).toBe('fix');
  });
});
