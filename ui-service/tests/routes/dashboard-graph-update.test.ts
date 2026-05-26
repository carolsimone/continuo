import request from 'supertest';
import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import { createDashboardGraphRouter, UPDATE_GRAPH_STREAM } from '../../src/server/routes/graph';

function makeRedisStub() {
  return { xadd: vi.fn().mockResolvedValue('1-0') };
}

function buildApp(redis: any) {
  const app = express();
  app.use(express.json());
  app.use('/api/dashboard', createDashboardGraphRouter(redis as any));
  return app;
}

describe('POST /api/dashboard/graph-update', () => {
  it('publishes update.graph:v1 when Sec-Fetch-Site: same-origin', async () => {
    const redis = makeRedisStub();
    const app = buildApp(redis);

    const res = await request(app)
      .post('/api/dashboard/graph-update')
      .set('Sec-Fetch-Site', 'same-origin')
      .send({ source: 's3' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true, source: 's3' });
    expect(redis.xadd).toHaveBeenCalledWith(UPDATE_GRAPH_STREAM, '*', 'source', 's3');
  });

  it('rejects with 403 when Sec-Fetch-Site header is missing', async () => {
    const redis = makeRedisStub();
    const app = buildApp(redis);

    const res = await request(app)
      .post('/api/dashboard/graph-update')
      .send({ source: 's3' });

    expect(res.status).toBe(403);
    expect(redis.xadd).not.toHaveBeenCalled();
  });

  it('rejects with 403 when Sec-Fetch-Site is cross-site', async () => {
    const redis = makeRedisStub();
    const app = buildApp(redis);

    const res = await request(app)
      .post('/api/dashboard/graph-update')
      .set('Sec-Fetch-Site', 'cross-site')
      .send({ source: 's3' });

    expect(res.status).toBe(403);
    expect(redis.xadd).not.toHaveBeenCalled();
  });

  it('returns 503 when Redis is not configured', async () => {
    const app = buildApp(null);

    const res = await request(app)
      .post('/api/dashboard/graph-update')
      .set('Sec-Fetch-Site', 'same-origin')
      .send({ source: 's3' });

    expect(res.status).toBe(503);
  });

  it('rejects non-s3/local source values', async () => {
    const redis = makeRedisStub();
    const app = buildApp(redis);

    const res = await request(app)
      .post('/api/dashboard/graph-update')
      .set('Sec-Fetch-Site', 'same-origin')
      .send({ source: 'nonsense' });

    expect(res.status).toBe(400);
    expect(redis.xadd).not.toHaveBeenCalled();
  });
});
