import request from 'supertest';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import express from 'express';
import { createGraphRouter } from '../../src/server/routes/graph';

function makeRedisStub() {
  return { xadd: vi.fn().mockResolvedValue('1-0') };
}

function buildApp(redis: any, token?: string) {
  const prev = process.env.GRAPH_UPDATE_TOKEN;
  if (token === undefined) {
    delete process.env.GRAPH_UPDATE_TOKEN;
  } else {
    process.env.GRAPH_UPDATE_TOKEN = token;
  }
  const app = express();
  app.use(express.json());
  app.use('/api/graph', createGraphRouter(redis as any));
  return { app, restore: () => {
    if (prev === undefined) delete process.env.GRAPH_UPDATE_TOKEN;
    else process.env.GRAPH_UPDATE_TOKEN = prev;
  }};
}

describe('POST /api/graph/update', () => {
  let restore: () => void;
  beforeEach(() => { restore = () => {}; });
  afterEach(() => restore());

  it('publishes update.graph:v1 with default source=s3 when no token configured', async () => {
    const redis = makeRedisStub();
    const built = buildApp(redis);
    restore = built.restore;

    const res = await request(built.app)
      .post('/api/graph/update')
      .send({ source: 's3' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true, source: 's3' });
    expect(redis.xadd).toHaveBeenCalledWith('update.graph:v1', '*', 'source', 's3');
  });

  it('rejects non-s3/local source values', async () => {
    const redis = makeRedisStub();
    const built = buildApp(redis);
    restore = built.restore;

    const res = await request(built.app)
      .post('/api/graph/update')
      .send({ source: 'nonsense' });

    expect(res.status).toBe(400);
    expect(redis.xadd).not.toHaveBeenCalled();
  });

  it('returns 503 when Redis is not configured', async () => {
    const built = buildApp(null);
    restore = built.restore;

    const res = await request(built.app)
      .post('/api/graph/update')
      .send({ source: 's3' });

    expect(res.status).toBe(503);
  });

  describe('with GRAPH_UPDATE_TOKEN configured', () => {
    it('accepts a correct bearer token and publishes', async () => {
      const redis = makeRedisStub();
      const built = buildApp(redis, 'secret-abc');
      restore = built.restore;

      const res = await request(built.app)
        .post('/api/graph/update')
        .set('Authorization', 'Bearer secret-abc')
        .send({ source: 's3' });

      expect(res.status).toBe(200);
      expect(redis.xadd).toHaveBeenCalledTimes(1);
    });

    it('rejects a missing Authorization header with 401', async () => {
      const redis = makeRedisStub();
      const built = buildApp(redis, 'secret-abc');
      restore = built.restore;

      const res = await request(built.app)
        .post('/api/graph/update')
        .send({ source: 's3' });

      expect(res.status).toBe(401);
      expect(redis.xadd).not.toHaveBeenCalled();
    });

    it('rejects a wrong bearer token with 401', async () => {
      const redis = makeRedisStub();
      const built = buildApp(redis, 'secret-abc');
      restore = built.restore;

      const res = await request(built.app)
        .post('/api/graph/update')
        .set('Authorization', 'Bearer wrong')
        .send({ source: 's3' });

      expect(res.status).toBe(401);
      expect(redis.xadd).not.toHaveBeenCalled();
    });

    it('treats empty GRAPH_UPDATE_TOKEN as unset (endpoint open)', async () => {
      const redis = makeRedisStub();
      const built = buildApp(redis, '');
      restore = built.restore;

      const res = await request(built.app)
        .post('/api/graph/update')
        .send({ source: 's3' });

      expect(res.status).toBe(200);
      expect(redis.xadd).toHaveBeenCalledTimes(1);
    });
  });
});
