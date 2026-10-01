import { describe, it, expect, afterEach, vi } from 'vitest';
import express from 'express';
import request from 'supertest';
import { bearerAuth, requireApiAuth, sessionAuth, devIdentity, authErrorHandler } from '../../src/server/auth/middleware';
import { createBearerVerifier } from '../../src/server/auth/bearer';
import { buildAuth } from '../../src/server/auth';
import { SessionStore } from '../../src/server/auth/session';
import { SESSION_COOKIE } from '../../src/server/auth/types';
import { parseCiAuthConfig } from '../../src/server/auth/ci-config';
import { principalOf } from '../../src/server/auth/request-principal';
import { FakeRedis } from './fake-redis';
import { startStubIssuer, githubClaims, type StubIssuer } from './stub-issuer';

const AUD = 'https://continuo.example.com';
const CLIENT_ID = 'continuo-ui';
let ciIssuer: StubIssuer | undefined;
let loginIssuer: StubIssuer | undefined;
afterEach(async () => { await ciIssuer?.close(); await loginIssuer?.close(); ciIssuer = loginIssuer = undefined; });

async function oidcApp() {
  ciIssuer = await startStubIssuer(AUD);
  loginIssuer = await startStubIssuer(CLIENT_ID);
  const ci = parseCiAuthConfig({ issuer: ciIssuer.issuer, audience: AUD, bindings: { core: [{ repositoryId: '812345678' }] } }, { allowInsecureIssuer: true });
  const verifier = createBearerVerifier([{ issuer: ci.issuer, audience: AUD }, { issuer: loginIssuer.issuer, audience: CLIENT_ID }], { cooldownMs: 0 });
  const store = new SessionStore(new FakeRedis(), 3600, 7200);
  const roles = { groupsClaim: 'groups', roleMapping: new Map(), operatorEmails: new Set(['op@corp.com']), viewerEmails: new Set(['view@corp.com']), defaultRole: 'none' as const };
  const app = express();
  app.use(express.json());
  app.use(bearerAuth({ verifier, ci, login: { issuer: loginIssuer.issuer, roles } }), sessionAuth(store));
  app.use('/api', requireApiAuth());
  app.get('/api/v1/current-prod', (req, res) => res.json(principalOf(req)));
  app.post('/api/v1/releases', (_req, res) => res.json({ ok: true }));
  app.get('/api/v1/releases/:id', (_req, res) => res.json({ ok: true }));
  app.get('/api/features', (_req, res) => res.json({ ok: true }));
  app.post('/api/things', (_req, res) => res.json({ ok: true }));
  app.get('/auth/me', (req, res) => (req.user ? res.json(req.user) : res.status(401).json({ code: 'unauthenticated' })));
  app.get('/', (req, res) => res.json({ user: req.user ?? null }));
  app.use(authErrorHandler());
  const operatorSid = await store.create({ userId: 'i|o', email: 'op@corp.com', name: 'O', role: 'operator' });
  return { app, operatorSid };
}

describe('bearer authentication', () => {
  it('a valid CI token becomes a ci principal with its granted services', async () => {
    const { app } = await oidcApp();
    const t = await ciIssuer!.sign(githubClaims());
    const res = await request(app).get('/api/v1/current-prod').set('Authorization', `Bearer ${t}`);
    expect(res.status).toBe(200);
    expect(res.body.kind).toBe('ci');
    expect(res.body.claims.repositoryId).toBe('812345678');
  });

  it('a CI token passes the guard on the per-action release routes only', async () => {
    const { app } = await oidcApp();
    const t = await ciIssuer!.sign(githubClaims());
    expect((await request(app).post('/api/v1/releases').set('Authorization', `Bearer ${t}`)).status).toBe(200);
    expect((await request(app).get('/api/v1/releases/x').set('Authorization', `Bearer ${t}`)).status).toBe(200);
    for (const [method, path] of [['get', '/api/v1/anything-else'], ['post', '/api/v1/anything-else'], ['get', '/api/v1/releases/x/y'], ['delete', '/api/v1/releases/x']] as const) {
      const res = await request(app)[method](path).set('Authorization', `Bearer ${t}`);
      expect([method, path, res.status, res.body.code]).toEqual([method, path, 403, 'forbidden']);
    }
  });

  it('accepts a lowercase scheme and trailing whitespace', async () => {
    const { app } = await oidcApp();
    const t = await ciIssuer!.sign(githubClaims());
    expect((await request(app).get('/api/v1/current-prod').set('Authorization', `bearer ${t}  `)).status).toBe(200);
  });

  it('CI tokens are denied on every non-v1 /api route', async () => {
    const { app } = await oidcApp();
    const t = await ciIssuer!.sign(githubClaims());
    expect((await request(app).get('/api/features').set('Authorization', `Bearer ${t}`)).status).toBe(403);
    expect((await request(app).post('/api/things').set('Authorization', `Bearer ${t}`)).status).toBe(403);
  });

  it('an invalid bearer is 401 even with a valid operator session cookie (no fallback)', async () => {
    const { app, operatorSid } = await oidcApp();
    const res = await request(app).get('/api/features').set('Authorization', 'Bearer garbage').set('Cookie', `${SESSION_COOKIE}=${operatorSid}`);
    expect(res.status).toBe(401);
    expect(res.body.code).toBe('invalid_token');
    expect(res.headers['www-authenticate']).toMatch(/^Bearer error="invalid_token"/);
  });

  it('a non-Bearer Authorization header on /api is 401 and audited as bearer_rejected', async () => {
    const { app } = await oidcApp();
    const lines: string[] = [];
    const spy = vi.spyOn(console, 'log').mockImplementation((msg: string) => { lines.push(msg); });
    try {
      const res = await request(app).get('/api/features').set('Authorization', 'Basic dXNlcjpwdw==');
      expect(res.status).toBe(401);
      expect(res.body.code).toBe('invalid_token');
    } finally { spy.mockRestore(); }
    const audited = lines.map((l) => JSON.parse(l)).filter((l) => l.event === 'bearer_rejected');
    expect(audited).toEqual([expect.objectContaining({ path: '/api/features', reason: 'malformed authorization header', outcome: 'unauthorized' })]);
  });

  it('outside /api the Authorization header is not a credential: the session cookie applies', async () => {
    const { app, operatorSid } = await oidcApp();
    // e.g. an ingress basic-auth or oauth2-proxy header reaching the ui
    const me = await request(app).get('/auth/me').set('Authorization', 'Basic dXNlcjpwdw==').set('Cookie', `${SESSION_COOKIE}=${operatorSid}`);
    expect(me.status).toBe(200);
    expect(me.body).toMatchObject({ email: 'op@corp.com', role: 'operator' });
    const spa = await request(app).get('/').set('Authorization', 'Bearer garbage').set('Cookie', `${SESSION_COOKIE}=${operatorSid}`);
    expect(spa.status).toBe(200);
    expect(spa.body.user).toMatchObject({ email: 'op@corp.com' });
    // The same header on /api is the only credential considered.
    const api = await request(app).get('/api/features').set('Authorization', 'Basic dXNlcjpwdw==').set('Cookie', `${SESSION_COOKIE}=${operatorSid}`);
    expect(api.status).toBe(401);
  });

  it('a valid bearer outside /api does not authenticate the request', async () => {
    const { app } = await oidcApp();
    const op = await loginIssuer!.sign({ sub: 'u1', email: 'op@corp.com', email_verified: true });
    expect((await request(app).get('/auth/me').set('Authorization', `Bearer ${op}`)).status).toBe(401);
  });

  it('a login-issuer token resolves role via resolveRole', async () => {
    const { app } = await oidcApp();
    const op = await loginIssuer!.sign({ sub: 'u1', email: 'op@corp.com', email_verified: true });
    const res = await request(app).get('/api/v1/current-prod').set('Authorization', `Bearer ${op}`);
    expect(res.body).toMatchObject({ kind: 'human', user: { role: 'operator', email: 'op@corp.com' } });
    expect((await request(app).post('/api/things').set('Authorization', `Bearer ${op}`)).status).toBe(200);
  });

  it('an operator-listed email that is not verified is not an operator', async () => {
    const { app } = await oidcApp();
    const t = await loginIssuer!.sign({ sub: 'u1', email: 'op@corp.com', email_verified: false });
    expect((await request(app).get('/api/features').set('Authorization', `Bearer ${t}`)).status).toBe(403);
  });

  it('a login-issuer token with no role is 403', async () => {
    const { app } = await oidcApp();
    const t = await loginIssuer!.sign({ sub: 'u2', email: 'stranger@corp.com', email_verified: true });
    const res = await request(app).get('/api/features').set('Authorization', `Bearer ${t}`);
    expect(res.status).toBe(403);
    expect(res.body.code).toBe('forbidden');
  });

  it('an unreachable issuer answers 503 auth_unavailable', async () => {
    const { app } = await oidcApp();
    const t = await ciIssuer!.sign(githubClaims());
    await ciIssuer!.close();
    ciIssuer = undefined;
    const res = await request(app).get('/api/v1/current-prod').set('Authorization', `Bearer ${t}`);
    expect(res.status).toBe(503);
    expect(res.body.code).toBe('auth_unavailable');
  });
});

describe('dev mode with CI config', () => {
  it('no Authorization header keeps the dev operator; a CI bearer is verified; a bad bearer is 401', async () => {
    ciIssuer = await startStubIssuer(AUD);
    const ci = parseCiAuthConfig({ issuer: ciIssuer.issuer, audience: AUD, bindings: { core: [{ repositoryId: '812345678' }] } }, { allowInsecureIssuer: true });
    const auth = await buildAuth({ mode: 'dev' }, ci);
    const app = express();
    app.use(...(auth.app.authn as [express.RequestHandler]));
    app.use('/api', ...auth.app.apiGuards);
    app.get('/api/v1/current-prod', (req, res) => res.json(principalOf(req)));
    app.use(auth.app.errorHandler);
    expect((await request(app).get('/api/v1/current-prod')).body).toMatchObject({ kind: 'human', user: { role: 'operator' } });
    const t = await ciIssuer.sign(githubClaims());
    expect((await request(app).get('/api/v1/current-prod').set('Authorization', `Bearer ${t}`)).body.kind).toBe('ci');
    expect((await request(app).get('/api/v1/current-prod').set('Authorization', 'Bearer nope')).status).toBe(401);
  });

  it('devIdentity leaves an /api request carrying Authorization to bearerAuth', async () => {
    const app = express();
    app.use(devIdentity());
    app.get('/api/x', (req, res) => res.json({ user: req.user ?? null }));
    app.get('/x', (req, res) => res.json({ user: req.user ?? null }));
    expect((await request(app).get('/api/x').set('Authorization', 'Bearer t')).body.user).toBeNull();
    expect((await request(app).get('/x').set('Authorization', 'Basic dXNlcjpwdw==')).body.user).toMatchObject({ role: 'operator' });
  });

  it('a CI config with no bindings does not trust the CI issuer', async () => {
    ciIssuer = await startStubIssuer(AUD);
    const ci = parseCiAuthConfig({ issuer: ciIssuer.issuer, audience: AUD, bindings: {} }, { allowInsecureIssuer: true });
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const log = vi.spyOn(console, 'log').mockImplementation(() => {});
    let auth: Awaited<ReturnType<typeof buildAuth>>;
    try {
      auth = await buildAuth({ mode: 'dev' }, ci);
      expect(log.mock.calls.map((c) => String(c[0]))).toContainEqual(expect.stringMatching(/CI auth is configured but has no bindings/));
    } finally { warn.mockRestore(); log.mockRestore(); }
    const app = express();
    app.use(...(auth.app.authn as [express.RequestHandler]));
    app.use('/api', ...auth.app.apiGuards);
    app.get('/api/v1/current-prod', (req, res) => res.json(principalOf(req)));
    app.use(auth.app.errorHandler);
    const t = await ciIssuer.sign(githubClaims());
    const res = await request(app).get('/api/v1/current-prod').set('Authorization', `Bearer ${t}`);
    expect(res.status).toBe(401);
    expect(res.body).toEqual({ error: 'invalid bearer token: untrusted issuer', code: 'invalid_token' });
    expect(ciIssuer.discoveryFetches()).toBe(0);
  });
});

describe('buildAuth', () => {
  it('refuses a CI issuer equal to the login issuer', async () => {
    const ci = parseCiAuthConfig({ issuer: 'https://idp.example.com', audience: AUD, bindings: {} }, { allowInsecureIssuer: false });
    await expect(buildAuth({
      mode: 'oidc', issuerUrl: 'https://idp.example.com', clientId: 'c', clientSecret: 's', publicUrl: 'https://x', scopes: 'openid',
      groupsClaim: 'groups', roleMapping: new Map(), operatorEmails: new Set(), viewerEmails: new Set(), defaultRole: 'none',
      sessionIdleTtlSeconds: 1, sessionMaxTtlSeconds: 2, redisUrl: 'redis://unused',
    }, ci)).rejects.toThrow(/must differ/);
  });
});
