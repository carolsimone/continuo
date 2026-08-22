import { describe, it, expect, afterEach } from 'vitest';
import express from 'express';
import request from 'supertest';
import { createAuthRouter, createDevAuthRouter, safeReturnTo } from '../../src/server/auth/routes';
import { sessionAuth, authErrorHandler } from '../../src/server/auth/middleware';
import { discoverOidc } from '../../src/server/auth/oidc';
import { SessionStore } from '../../src/server/auth/session';
import { FakeRedis } from './fake-redis';
import { startStubIdp, type StubIdp } from './stub-idp';
import { PENDING_COOKIE, SESSION_COOKIE } from '../../src/server/auth/types';
import type { OidcAuthConfig } from '../../src/server/auth/config';

let idp: StubIdp | undefined;
afterEach(() => idp?.close());

async function makeApp(over: Partial<OidcAuthConfig> = {}) {
  idp = await startStubIdp();
  const cfg: OidcAuthConfig = {
    mode: 'oidc',
    issuerUrl: idp.issuer,
    clientId: 'continuo-ui',
    clientSecret: 'test-secret',
    publicUrl: 'http://app.local:8090',
    scopes: 'openid email profile',
    groupsClaim: 'groups',
    roleMapping: new Map([['ops-team', 'operator' as const]]),
    operatorEmails: new Set<string>(),
    viewerEmails: new Set<string>(),
    defaultRole: 'none',
    sessionIdleTtlSeconds: 3600,
    sessionMaxTtlSeconds: 7200,
    redisUrl: 'redis://unused',
    ...over,
  };
  const redis = new FakeRedis();
  const sessions = new SessionStore(redis, cfg.sessionIdleTtlSeconds, cfg.sessionMaxTtlSeconds);
  const flow = await discoverOidc(cfg);
  const app = express();
  app.use(sessionAuth(sessions));
  app.use('/auth', createAuthRouter({ flow, sessions, cfg }));
  app.use(authErrorHandler());
  return { app, idp: idp!, redis };
}

function cookieValue(res: request.Response, name: string): string {
  const raw = (res.headers['set-cookie'] as unknown as string[] | undefined) ?? [];
  const hit = raw.find((c) => c.startsWith(`${name}=`));
  if (!hit) throw new Error(`no ${name} cookie in ${JSON.stringify(raw)}`);
  return hit.split(';')[0].split('=').slice(1).join('=');
}

async function completeLogin(app: express.Express, stub: StubIdp, claims: Record<string, unknown>) {
  const login = await request(app).get('/auth/login?returnTo=/schedule/daily');
  expect(login.status).toBe(302);
  const pendingCookie = cookieValue(login, PENDING_COOKIE);
  const pending = JSON.parse(Buffer.from(pendingCookie, 'base64url').toString());
  stub.setNextClaims({ ...claims, nonce: pending.nonce });
  return request(app)
    .get(`/auth/callback?code=fake-code&state=${pending.state}`)
    .set('Cookie', `${PENDING_COOKIE}=${pendingCookie}`);
}

describe('/auth routes', () => {
  it('login redirects to the IdP and stores pending state in a cookie', async () => {
    const { app, idp: stub } = await makeApp();
    const res = await request(app).get('/auth/login?returnTo=/x');
    expect(res.status).toBe(302);
    expect(res.headers.location).toContain(`${stub.issuer}/authorize`);
    const pending = JSON.parse(Buffer.from(cookieValue(res, PENDING_COOKIE), 'base64url').toString());
    expect(pending.returnTo).toBe('/x');
    expect(pending.codeVerifier).toBeTruthy();
  });

  it('callback creates a session and redirects to returnTo', async () => {
    const { app, idp: stub } = await makeApp();
    const res = await completeLogin(app, stub, { sub: 'u1', email: 'ana@corp.com', name: 'Ana', groups: ['ops-team'] });
    expect(res.status).toBe(302);
    expect(res.headers.location).toBe('/schedule/daily');
    const sessionId = cookieValue(res, SESSION_COOKIE);
    const me = await request(app).get('/auth/me').set('Cookie', `${SESSION_COOKIE}=${sessionId}`);
    expect(me.status).toBe(200);
    expect(me.body).toMatchObject({ email: 'ana@corp.com', role: 'operator' });
  });

  it('callback denies a user resolving to no role', async () => {
    const { app, idp: stub } = await makeApp();
    const res = await completeLogin(app, stub, { sub: 'u2', email: 'stranger@corp.com', groups: ['unmapped'] });
    expect(res.status).toBe(302);
    expect(res.headers.location).toBe('/?auth_error=no_role');
    const raw = (res.headers['set-cookie'] as unknown as string[]) ?? [];
    expect(raw.some((c) => c.startsWith(`${SESSION_COOKIE}=`) && !c.includes(`${SESSION_COOKIE}=;`))).toBe(false);
  });

  it('callback without the pending cookie fails safely', async () => {
    const { app } = await makeApp();
    const res = await request(app).get('/auth/callback?code=x&state=y');
    expect(res.status).toBe(302);
    expect(res.headers.location).toBe('/?auth_error=login_failed');
  });

  it('callback with a tampered state fails safely', async () => {
    const { app, idp: stub } = await makeApp();
    const login = await request(app).get('/auth/login?returnTo=/');
    const pendingCookie = cookieValue(login, PENDING_COOKIE);
    const pending = JSON.parse(Buffer.from(pendingCookie, 'base64url').toString());
    stub.setNextClaims({ sub: 'u1', email: 'a@b.c', nonce: pending.nonce });
    const res = await request(app)
      .get('/auth/callback?code=fake-code&state=TAMPERED')
      .set('Cookie', `${PENDING_COOKIE}=${pendingCookie}`);
    expect(res.status).toBe(302);
    expect(res.headers.location).toBe('/?auth_error=login_failed');
  });

  it('logout destroys the session and returns the IdP end-session redirect', async () => {
    const { app, idp: stub } = await makeApp();
    const login = await completeLogin(app, stub, { sub: 'u1', email: 'ana@corp.com', groups: ['ops-team'] });
    const sessionId = cookieValue(login, SESSION_COOKIE);
    const out = await request(app).post('/auth/logout').set('Cookie', `${SESSION_COOKIE}=${sessionId}`);
    expect(out.status).toBe(200);
    expect(out.body.redirectTo).toContain(`${stub.issuer}/logout`);
    const me = await request(app).get('/auth/me').set('Cookie', `${SESSION_COOKIE}=${sessionId}`);
    expect(me.status).toBe(401);
  });

  it('logout rejects a cross-origin browser request', async () => {
    const { app } = await makeApp();
    const res = await request(app).post('/auth/logout').set('Origin', 'https://evil.example.com');
    expect(res.status).toBe(403);
  });

  it('me without a session is 401', async () => {
    const { app } = await makeApp();
    expect((await request(app).get('/auth/me')).status).toBe(401);
  });
});

describe('safeReturnTo', () => {
  it('accepts only same-origin relative paths', () => {
    expect(safeReturnTo('/schedule/x')).toBe('/schedule/x');
    expect(safeReturnTo('https://evil.com')).toBe('/');
    expect(safeReturnTo('//evil.com')).toBe('/');
    expect(safeReturnTo('/\\evil.com')).toBe('/');
    expect(safeReturnTo('/\\/evil.com')).toBe('/');
    expect(safeReturnTo(undefined)).toBe('/');
  });
});

describe('dev auth router', () => {
  it('serves the fixed identity on /me', async () => {
    const app = express();
    app.use('/auth', createDevAuthRouter());
    const me = await request(app).get('/auth/me');
    expect(me.status).toBe(200);
    expect(me.body).toMatchObject({ userId: 'dev|local', role: 'operator' });
    const out = await request(app).post('/auth/logout');
    expect(out.body).toEqual({ redirectTo: '/' });
  });
});
