import { describe, it, expect } from 'vitest';
import request from 'supertest';
import { createApp } from '../../src/server/app';
import { buildAuth } from '../../src/server/auth';
import { sessionAuth, requireApiAuth, csrfOriginCheck, auditMutations, authErrorHandler } from '../../src/server/auth/middleware';
import { createDevAuthRouter } from '../../src/server/auth/routes';
import { SessionStore } from '../../src/server/auth/session';
import { FakeRedis } from './fake-redis';
import type { AppAuth, AuthUser } from '../../src/server/auth/types';
import { SESSION_COOKIE } from '../../src/server/auth/types';
import type { GrpcClient } from '../../src/server/grpc-client';
import type { GrpcGraphClient } from '../../src/server/grpc-graph-client';

const fakeGrpc = {} as unknown as GrpcClient;
const fakeGraph = {} as unknown as GrpcGraphClient;

function oidcModeAuth(store: SessionStore): AppAuth {
  return {
    authn: [sessionAuth(store)],
    apiGuards: [csrfOriginCheck('http://app.local:8090'), requireApiAuth(), auditMutations()],
    router: createDevAuthRouter(), // /auth internals are covered in routes.test.ts
    errorHandler: authErrorHandler(),
  };
}

describe('createApp gating', () => {
  it('healthz is public', async () => {
    const auth = await buildAuth({ mode: 'dev' });
    const app = createApp(fakeGrpc, fakeGraph, auth.app);
    const res = await request(app).get('/healthz');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });

  it('dev mode: API reads and writes pass (existing behavior preserved)', async () => {
    const auth = await buildAuth({ mode: 'dev' });
    const app = createApp(fakeGrpc, fakeGraph, auth.app);
    expect((await request(app).get('/api/features')).status).toBe(200);
  });

  it('oidc mode: unauthenticated /api is 401, /auth and /healthz are public', async () => {
    const store = new SessionStore(new FakeRedis(), 3600, 7200);
    const app = createApp(fakeGrpc, fakeGraph, oidcModeAuth(store));
    expect((await request(app).get('/api/features')).status).toBe(401);
    expect((await request(app).get('/healthz')).status).toBe(200);
    expect((await request(app).get('/auth/me')).status).toBe(200); // dev router stand-in
  });

  it('oidc mode: viewer reads pass, viewer mutations 403, operator mutations pass the gate', async () => {
    const store = new SessionStore(new FakeRedis(), 3600, 7200);
    const viewer: AuthUser = { userId: 'i|v', email: 'v@c.com', name: 'V', role: 'viewer' };
    const operator: AuthUser = { userId: 'i|o', email: 'o@c.com', name: 'O', role: 'operator' };
    const viewerSid = await store.create(viewer);
    const operatorSid = await store.create(operator);
    const app = createApp(fakeGrpc, fakeGraph, oidcModeAuth(store));

    const read = await request(app).get('/api/features').set('Cookie', `${SESSION_COOKIE}=${viewerSid}`);
    expect(read.status).toBe(200);

    const denied = await request(app).post('/api/anything').set('Cookie', `${SESSION_COOKIE}=${viewerSid}`);
    expect(denied.status).toBe(403);

    // 404 (unknown route), not 401/403: the operator cleared every guard.
    const passed = await request(app).post('/api/anything').set('Cookie', `${SESSION_COOKIE}=${operatorSid}`);
    expect(passed.status).toBe(404);
  });
});
