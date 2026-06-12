import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import express from 'express';
import request from 'supertest';
import {
  devIdentity,
  sessionAuth,
  requireApiAuth,
  csrfOriginCheck,
  auditMutations,
  authErrorHandler,
} from '../../src/server/auth/middleware';
import { SessionStore } from '../../src/server/auth/session';
import { FakeRedis } from './fake-redis';
import { SESSION_COOKIE, type AuthUser } from '../../src/server/auth/types';

const viewer: AuthUser = { userId: 'idp|v', email: 'v@b.com', name: 'V', role: 'viewer' };
const operator: AuthUser = { userId: 'idp|o', email: 'o@b.com', name: 'O', role: 'operator' };

function apiApp(...pre: express.RequestHandler[]) {
  const app = express();
  app.use(...pre);
  app.use('/api', requireApiAuth());
  app.get('/api/thing', (_req, res) => res.json({ ok: true }));
  app.post('/api/thing', (_req, res) => res.status(202).json({ ok: true }));
  app.use(authErrorHandler());
  return app;
}

describe('gating matrix', () => {
  it('401 unauthenticated on /api reads and writes', async () => {
    const app = apiApp((_req, _res, next) => next());
    expect((await request(app).get('/api/thing')).status).toBe(401);
    expect((await request(app).post('/api/thing')).status).toBe(401);
  });

  it('viewer: reads pass, mutations 403', async () => {
    const app = apiApp((req, _res, next) => { req.user = viewer; next(); });
    expect((await request(app).get('/api/thing')).status).toBe(200);
    const res = await request(app).post('/api/thing');
    expect(res.status).toBe(403);
    expect(res.body.error.code).toBe('forbidden');
  });

  it('operator: reads and mutations pass', async () => {
    const app = apiApp((req, _res, next) => { req.user = operator; next(); });
    expect((await request(app).get('/api/thing')).status).toBe(200);
    expect((await request(app).post('/api/thing')).status).toBe(202);
  });

  it('devIdentity attaches the operator dev user', async () => {
    const app = apiApp(devIdentity());
    expect((await request(app).post('/api/thing')).status).toBe(202);
  });
});

describe('sessionAuth', () => {
  it('attaches the user from a valid session cookie', async () => {
    const redis = new FakeRedis();
    const store = new SessionStore(redis, 3600, 7200);
    const id = await store.create(operator);
    const app = apiApp(sessionAuth(store));
    const res = await request(app).get('/api/thing').set('Cookie', `${SESSION_COOKIE}=${id}`);
    expect(res.status).toBe(200);
  });

  it('ignores an unknown session id (401 downstream)', async () => {
    const store = new SessionStore(new FakeRedis(), 3600, 7200);
    const app = apiApp(sessionAuth(store));
    const res = await request(app).get('/api/thing').set('Cookie', `${SESSION_COOKIE}=forged`);
    expect(res.status).toBe(401);
  });

  it('fails closed with 503 when the session backend throws', async () => {
    const broken = {
      get: () => Promise.reject(new Error('redis down')),
      set: () => Promise.reject(new Error('redis down')),
      del: () => Promise.reject(new Error('redis down')),
      expire: () => Promise.reject(new Error('redis down')),
    } as unknown as import('../../src/server/auth/session').RedisLike;
    const store = new SessionStore(broken, 3600, 7200);
    const app = apiApp(sessionAuth(store));
    const res = await request(app).get('/api/thing').set('Cookie', `${SESSION_COOKIE}=any`);
    expect(res.status).toBe(503);
    expect(res.body.error.code).toBe('auth_unavailable');
  });
});

describe('csrfOriginCheck', () => {
  const origin = 'https://continuo.example.com';

  function appWithCsrf() {
    const app = express();
    app.use((req, _res, next) => { req.user = operator; next(); });
    app.use('/api', csrfOriginCheck(origin), requireApiAuth());
    app.post('/api/thing', (_req, res) => res.status(202).json({ ok: true }));
    return app;
  }

  it('allows same-origin mutations', async () => {
    const res = await request(appWithCsrf()).post('/api/thing').set('Origin', origin);
    expect(res.status).toBe(202);
  });

  it('rejects cross-origin mutations', async () => {
    const res = await request(appWithCsrf()).post('/api/thing').set('Origin', 'https://evil.example.com');
    expect(res.status).toBe(403);
    expect(res.body.error.code).toBe('csrf_rejected');
  });

  it('allows mutations without an Origin header (non-browser clients)', async () => {
    const res = await request(appWithCsrf()).post('/api/thing');
    expect(res.status).toBe(202);
  });
});

describe('auditMutations', () => {
  let lines: string[] = [];
  beforeEach(() => {
    lines = [];
    vi.spyOn(console, 'log').mockImplementation((msg: string) => { lines.push(msg); });
  });
  afterEach(() => vi.restoreAllMocks());

  it('emits one structured line per mutating call with the outcome status', async () => {
    const app = express();
    app.use((req, _res, next) => { req.user = operator; next(); });
    app.use('/api', auditMutations(), requireApiAuth());
    app.post('/api/thing', (_req, res) => res.status(202).json({ ok: true }));
    app.get('/api/thing', (_req, res) => res.json({ ok: true }));

    await request(app).post('/api/thing');
    await request(app).get('/api/thing'); // reads are not audited

    const audits = lines.map((l) => JSON.parse(l)).filter((l) => l.audit);
    expect(audits).toHaveLength(1);
    expect(audits[0]).toMatchObject({
      event: 'api_mutation',
      user_id: 'idp|o',
      role: 'operator',
      method: 'POST',
      path: '/api/thing',
      outcome: 202,
    });
  });
});
