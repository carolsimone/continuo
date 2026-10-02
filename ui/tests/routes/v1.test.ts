import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import request from 'supertest';
import type { Server } from 'node:http';
import { once } from 'node:events';
import { createV1Router, projectCurrentProd, projectRelease, READ_RATE_LIMIT_PER_MINUTE, SUBMIT_RATE_LIMIT_PER_MINUTE } from '../../src/server/routes/v1';
import { HttpError, type ReleaseClient } from '../../src/server/release-client';
import type { Principal } from '../../src/server/auth/principal';
import { githubClaimsFrom } from '../../src/server/auth/bearer';
import { githubClaims } from '../auth/stub-issuer';

const operator: Principal = { kind: 'human', user: { userId: 'i|o', email: 'o@c.com', name: 'O', role: 'operator' } };
const viewer: Principal = { kind: 'human', user: { userId: 'i|v', email: 'v@c.com', name: 'V', role: 'viewer' } };
const ci = (grants: Record<string, boolean> = { core: false }): Principal => ({
  kind: 'ci', subject: 's', claims: githubClaimsFrom(githubClaims()),
  grants: new Map(Object.entries(grants).map(([s, b]) => [s, { allowBootstrap: b }])),
});

function fakeClient(over: Partial<ReleaseClient> = {}): ReleaseClient {
  return {
    listReleases: vi.fn(), getCurrentProd: vi.fn(), retryRemediation: vi.fn(),
    getVerificationRun: vi.fn(), listVerificationRuns: vi.fn(), getPipeline: vi.fn(),
    submitRelease: vi.fn(async (b) => ({ status: 202, text: JSON.stringify({ release_id: b.release_id, status: 'received' }) })),
    getRelease: vi.fn(async () => { throw new HttpError(404, 'not found'); }),
    ...over,
  } as ReleaseClient;
}

function auditLines(spy: ReturnType<typeof vi.spyOn>): Record<string, unknown>[] {
  return spy.mock.calls
    .map((c) => { try { return JSON.parse(String(c[0])); } catch { return null; } })
    .filter((l): l is Record<string, unknown> => l !== null && l.audit === true && l.event === 'release_submit');
}

function appAs(p: Principal, client: ReleaseClient, publicUrl = 'https://continuo.example.com') {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => { req.principal = p; next(); });
  app.use('/api/v1', createV1Router(client, publicUrl));
  return app;
}

const body = { release_id: 'rel-1', service: 'core', image_tag: 'abc' };

// Runs fn against one listening server, so a test that sends hundreds of
// requests opens one listener rather than one per request.
async function withServer(app: express.Express, fn: (server: Server) => Promise<void>): Promise<void> {
  const server = app.listen(0);
  await once(server, 'listening');
  try {
    await fn(server);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

describe('POST /api/v1/releases', () => {
  it('a bound CI identity submits; repo and commit_sha are filled from the token', async () => {
    const client = fakeClient();
    const res = await request(appAs(ci(), client)).post('/api/v1/releases').send(body);
    expect(res.status).toBe(202);
    expect(res.body).toEqual({ release_id: 'rel-1', status: 'received' });
    expect(client.submitRelease).toHaveBeenCalledWith({ ...body, repo: 'carolsimone/continuo-demo', commit_sha: 'abc1234def' });
  });

  it.each([
    ['not JSON', 'accepted'],
    ['a JSON array', '[]'],
    ['wrong-typed fields', JSON.stringify({ release_id: 1, status: null })],
  ])('a 202 whose body is %s answers the submitted id and "received"', async (_n, text) => {
    const client = fakeClient({ submitRelease: vi.fn(async () => ({ status: 202, text })) });
    const res = await request(appAs(ci(), client)).post('/api/v1/releases').send(body);
    expect(res.status).toBe(202);
    expect(res.body).toEqual({ release_id: 'rel-1', status: 'received' });
  });

  it('forwards only the submission fields that were sent', async () => {
    const client = fakeClient();
    await request(appAs(operator, client)).post('/api/v1/releases').send(body);
    expect(Object.keys((client.submitRelease as ReturnType<typeof vi.fn>).mock.calls[0][0]).sort()).toEqual(['image_tag', 'release_id', 'service']);
  });

  it('claim_mismatch when the body contradicts the token', async () => {
    const client = fakeClient();
    const res = await request(appAs(ci(), client)).post('/api/v1/releases').send({ ...body, repo: 'someone/else' });
    expect(res.status).toBe(403);
    expect(res.body.code).toBe('claim_mismatch');
    expect(client.submitRelease).not.toHaveBeenCalled();
  });

  it('a matching repo/commit_sha in the body is accepted', async () => {
    const res = await request(appAs(ci(), fakeClient())).post('/api/v1/releases').send({ ...body, repo: 'carolsimone/continuo-demo', commit_sha: 'abc1234def' });
    expect(res.status).toBe(202);
  });

  it('unbound service is forbidden; bootstrap without allowBootstrap is bootstrap_not_allowed', async () => {
    const app = appAs(ci({ core: false }), fakeClient());
    expect((await request(app).post('/api/v1/releases').send({ ...body, service: 'marketing' })).body.code).toBe('forbidden');
    expect((await request(app).post('/api/v1/releases').send({ ...body, bootstrap: true })).body.code).toBe('bootstrap_not_allowed');
    // A repository that matches no binding at all.
    const none = await request(appAs(ci({}), fakeClient())).post('/api/v1/releases').send(body);
    expect([none.status, none.body.code]).toEqual([403, 'forbidden']);
  });

  it('humans keep their body provenance; viewers cannot submit', async () => {
    const client = fakeClient();
    const human = { ...body, repo: 'me/proj', commit_sha: 'deadbeef', bootstrap: true };
    expect((await request(appAs(operator, client)).post('/api/v1/releases').send(human)).status).toBe(202);
    expect(client.submitRelease).toHaveBeenCalledWith(human);
    expect((await request(appAs(viewer, fakeClient())).post('/api/v1/releases').send(body)).status).toBe(403);
  });

  it.each([
    ['unknown field', { ...body, verifies_release_id: 'x' }, /unknown field/],
    ['missing service', { release_id: 'r', image_tag: 't' }, /service/],
    ['string bootstrap', { ...body, bootstrap: 'true' }, /bootstrap must be a boolean/],
    ['array body', [body], /JSON object/],
  ])('400 on %s', async (_n, b, msg) => {
    const client = fakeClient();
    const res = await request(appAs(operator, client)).post('/api/v1/releases').send(b as object);
    expect(res.status).toBe(400);
    expect(res.body.code).toBe('bad_request');
    expect(res.body.error).toMatch(msg);
    expect(client.submitRelease).not.toHaveBeenCalled();
  });

  it('maps upstream 409 and 400, and an unreachable upstream to 503', async () => {
    const conflict = fakeClient({ submitRelease: vi.fn(async () => ({ status: 409, text: 'run kind conflict' })) });
    expect((await request(appAs(operator, conflict)).post('/api/v1/releases').send(body)).body.code).toBe('release_kind_conflict');
    const invalid = fakeClient({ submitRelease: vi.fn(async () => ({ status: 400, text: 'image_tag required\n' })) });
    const r400 = await request(appAs(operator, invalid)).post('/api/v1/releases').send(body);
    expect(r400.status).toBe(400);
    expect(r400.body).toEqual({ error: 'image_tag required', code: 'bad_request' });
    const down = fakeClient({ submitRelease: vi.fn(async () => { throw new TypeError('fetch failed'); }) });
    expect((await request(appAs(operator, down)).post('/api/v1/releases').send(body)).status).toBe(503);
  });

  it("passes release-controller's 409 through with its message to a person, and asks it nothing else", async () => {
    const msg = 'release id already names a different candidate: "rel-1" has a different service, image tag, kind, bootstrap flag or source change\n';
    const client = fakeClient({ submitRelease: vi.fn(async () => ({ status: 409, text: msg })) });
    const res = await request(appAs(operator, client)).post('/api/v1/releases').send(body);
    expect(res.status).toBe(409);
    expect(res.body).toEqual({ error: msg.trim(), code: 'release_kind_conflict' });
    expect(client.getRelease).not.toHaveBeenCalled();
  });

  it('answers a CI identity 409 with a generic message that names no other release or run kind', async () => {
    for (const msg of ['run id already names a run of another kind: rel-1 is a verification\n', 'release id already names a different candidate: "rel-1" has a different service\n']) {
      const client = fakeClient({ submitRelease: vi.fn(async () => ({ status: 409, text: msg })) });
      const res = await request(appAs(ci(), client)).post('/api/v1/releases').send(body);
      expect(res.status).toBe(409);
      expect(res.body).toEqual({ error: 'release id already exists with different content', code: 'release_kind_conflict' });
    }
  });

  it.each([
    ['a dot-segment id', '..'],
    ['a slash', 'rel/1'],
    ['a leading dot', '.rel'],
    ['a leading dash', '-rel'],
    ['a space', 'rel 1'],
    ['a 129-character id', `a${'b'.repeat(128)}`],
  ])('400 on a release_id with %s', async (_n, id) => {
    const client = fakeClient();
    const res = await request(appAs(operator, client)).post('/api/v1/releases').send({ ...body, release_id: id });
    expect(res.status).toBe(400);
    expect(res.body.code).toBe('bad_request');
    expect(res.body.error).toMatch(/release_id/);
    expect(client.submitRelease).not.toHaveBeenCalled();
  });

  it('accepts release ids of letters, digits, dot, dash and underscore up to 128 characters', async () => {
    const app = appAs(operator, fakeClient());
    for (const id of ['rel-abc1234-17', 'e2e-rel-1a2b3c4d', 'auth-e2e-submit', 'v1.2.3_build', `a${'b'.repeat(127)}`, '0']) {
      expect((await request(app).post('/api/v1/releases').send({ ...body, release_id: id, repo: 'o/r', commit_sha: 'c' })).status).toBe(202);
    }
  });

  it('rate-limits submissions per principal', async () => {
    await withServer(appAs(ci(), fakeClient()), async (server) => {
      for (let i = 0; i < SUBMIT_RATE_LIMIT_PER_MINUTE; i++) {
        expect((await request(server).post('/api/v1/releases').send({ ...body, release_id: `r${i}` })).status).toBe(202);
      }
      const res = await request(server).post('/api/v1/releases').send(body);
      expect(res.status).toBe(429);
      expect(res.body.code).toBe('rate_limited');
    });
  });
});

describe('release_submit audit', () => {
  it('audits a 202 CI submit with the repository identity, service and bootstrap', async () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      await request(appAs(ci(), fakeClient())).post('/api/v1/releases').send(body);
      const lines = auditLines(spy);
      expect(lines).toHaveLength(1);
      expect(lines[0]).toMatchObject({
        principal: 'ci', repository: 'carolsimone/continuo-demo', repository_id: expect.any(String),
        service: 'core', release_id: 'rel-1', bootstrap: false, outcome: 202,
      });
    } finally { spy.mockRestore(); }
  });

  it('audits a 400 submit, recording only string service and release_id', async () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      await request(appAs(operator, fakeClient())).post('/api/v1/releases').send({ ...body, service: 'core', release_id: 7, bootstrap: 'true' });
      const lines = auditLines(spy);
      expect(lines).toHaveLength(1);
      expect(lines[0]).toMatchObject({ principal: 'human', user_id: 'i|o', service: 'core', bootstrap: false, outcome: 400 });
      expect(lines[0].release_id).toBeUndefined();
    } finally { spy.mockRestore(); }
  });

  it('audits a rate-limited submit with the principal and outcome 429', async () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      await withServer(appAs(ci(), fakeClient()), async (server) => {
        for (let i = 0; i < SUBMIT_RATE_LIMIT_PER_MINUTE; i++) {
          await request(server).post('/api/v1/releases').send({ ...body, release_id: `r${i}` });
        }
        await request(server).post('/api/v1/releases').send(body);
      });
      const lines = auditLines(spy);
      expect(lines).toHaveLength(SUBMIT_RATE_LIMIT_PER_MINUTE + 1);
      expect(lines[lines.length - 1]).toMatchObject({ principal: 'ci', repository: 'carolsimone/continuo-demo', service: 'core', outcome: 429 });
    } finally { spy.mockRestore(); }
  });

  it('audits a 409 conflict outcome', async () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      const client = fakeClient({ submitRelease: vi.fn(async () => ({ status: 409, text: 'conflict' })) });
      await request(appAs(ci(), client)).post('/api/v1/releases').send(body);
      expect(auditLines(spy).map((l) => l.outcome)).toEqual([409]);
    } finally { spy.mockRestore(); }
  });
});

describe('GET /api/v1/releases/:id', () => {
  const rel = { release_id: 'r.1', status: 'rejected', changed_service: 'core', bootstrap: false, repo: 'carolsimone/continuo-demo', commit_sha: 'abc', reject_reason: 'validation_failed', reject_detail: 'x', per_node_results: [{}], failing_nodes: ['a'] };

  it('returns the v1 projection only, with terminal and ui_url', async () => {
    const client = fakeClient({ getRelease: vi.fn(async () => rel) });
    const res = await request(appAs(ci(), client)).get('/api/v1/releases/r.1');
    expect(res.status).toBe(200);
    expect(client.getRelease).toHaveBeenCalledWith('r.1');
    expect(res.body).toEqual({
      release_id: 'r.1', service: 'core', status: 'rejected', terminal: true, bootstrap: false,
      repo: 'carolsimone/continuo-demo', commit_sha: 'abc', reject_reason: 'validation_failed', reject_detail: 'x',
      ui_url: 'https://continuo.example.com/releases/r.1',
    });
  });

  it('a release of a service the CI identity is not bound to reads as not found, without naming the service', async () => {
    const other = fakeClient({ getRelease: vi.fn(async () => ({ ...rel, changed_service: 'marketing' })) });
    const res = await request(appAs(ci(), other)).get('/api/v1/releases/x');
    expect(res.status).toBe(404);
    expect(res.body).toEqual({ error: 'release not found', code: 'not_found' });
    // A person reads every release.
    expect((await request(appAs(viewer, other)).get('/api/v1/releases/x')).status).toBe(200);
  });

  it('an id outside the release_id pattern is 404 without asking release-controller', async () => {
    const client = fakeClient();
    for (const id of ['r%2F1', '..%2Fcurrent-prod', '.hidden', `a${'b'.repeat(128)}`]) {
      const res = await request(appAs(operator, client)).get(`/api/v1/releases/${id}`);
      expect([id, res.status, res.body.code]).toEqual([id, 404, 'not_found']);
    }
    expect(client.getRelease).not.toHaveBeenCalled();
  });

  it('404 and 503 map through', async () => {
    const missing = fakeClient({ getRelease: vi.fn(async () => { throw new HttpError(404, 'nf'); }) });
    expect((await request(appAs(operator, missing)).get('/api/v1/releases/x')).body.code).toBe('not_found');
    const down = fakeClient({ getRelease: vi.fn(async () => { throw new TypeError('fetch failed'); }) });
    expect((await request(appAs(operator, down)).get('/api/v1/releases/x')).status).toBe(503);
  });

  it('projectRelease: non-terminal status and no publicUrl gives ui_url null', () => {
    expect(projectRelease({ ...rel, status: 'validating' })).toMatchObject({ terminal: false, ui_url: null });
  });

  it('projectRelease: a field of the wrong type becomes its empty value, never a different shape', () => {
    const odd = { release_id: 7, status: null, changed_service: ['core'], bootstrap: 'true', repo: 1, commit_sha: {}, reject_reason: false, reject_detail: { msg: 'x' } };
    expect(projectRelease(odd)).toEqual({
      release_id: '', service: '', status: '', terminal: false, bootstrap: false,
      repo: '', commit_sha: '', reject_reason: '', reject_detail: '', ui_url: null,
    });
  });

  it('503 when release-controller answers a body that is not a JSON object', async () => {
    const client = fakeClient({ getRelease: vi.fn(async () => null) });
    const res = await request(appAs(operator, client)).get('/api/v1/releases/x');
    expect([res.status, res.body.code]).toEqual([503, 'upstream_unavailable']);
  });
});

describe('GET /api/v1/current-prod', () => {
  const cp = () => fakeClient({ getCurrentProd: vi.fn(async () => ({ current_prod_release_id: 'r', node_count: 3, updated_at: 't', extra: 1 })) });

  it('a bound CI identity and a viewer read exactly the three fields', async () => {
    for (const p of [ci(), viewer]) {
      const res = await request(appAs(p, cp())).get('/api/v1/current-prod');
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ current_prod_release_id: 'r', node_count: 3, updated_at: 't' });
    }
  });

  it('a field of the wrong type becomes its empty value', () => {
    expect(projectCurrentProd({ current_prod_release_id: null, node_count: '3', updated_at: 17 })).toEqual({ current_prod_release_id: '', node_count: 0, updated_at: '' });
  });

  it('503 when release-controller answers a body that is not a JSON object', async () => {
    const client = fakeClient({ getCurrentProd: vi.fn(async () => 'oops') });
    const res = await request(appAs(viewer, client)).get('/api/v1/current-prod');
    expect([res.status, res.body.code]).toEqual([503, 'upstream_unavailable']);
  });

  it('a CI identity bound to no service is forbidden', async () => {
    const client = cp();
    const res = await request(appAs(ci({}), client)).get('/api/v1/current-prod');
    expect(res.status).toBe(403);
    expect(res.body.code).toBe('forbidden');
    expect(client.getCurrentProd).not.toHaveBeenCalled();
  });
});

describe('read rate limit', () => {
  it('audits a rate-limited read with the principal, path and outcome 429', async () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      const c = fakeClient({ getCurrentProd: vi.fn(async () => ({ current_prod_release_id: 'r', node_count: 1, updated_at: 't' })) });
      await withServer(appAs(ci(), c), async (server) => {
        for (let i = 0; i < READ_RATE_LIMIT_PER_MINUTE; i++) await request(server).get('/api/v1/current-prod');
        expect((await request(server).get('/api/v1/current-prod')).status).toBe(429);
      });
      const lines = spy.mock.calls
        .map((call) => { try { return JSON.parse(String(call[0])); } catch { return null; } })
        .filter((l) => l !== null && l.audit === true && l.event === 'release_read_limited');
      expect(lines).toHaveLength(1);
      expect(lines[0]).toMatchObject({
        principal: 'ci', repository: 'carolsimone/continuo-demo', method: 'GET', path: '/api/v1/current-prod', outcome: 429,
      });
    } finally { spy.mockRestore(); }
  });


  it('caps release and current-prod reads per principal, on a counter separate from submissions', async () => {
    const c = fakeClient({
      getRelease: vi.fn(async () => ({ release_id: 'r1', status: 'validating', changed_service: 'core' })),
      getCurrentProd: vi.fn(async () => ({ current_prod_release_id: 'r', node_count: 1, updated_at: 't' })),
    });
    const app = express();
    app.use(express.json());
    let who: Principal = ci();
    app.use((req, _res, next) => { req.principal = who; next(); });
    app.use('/api/v1', createV1Router(c));

    await withServer(app, async (server) => {
      for (let i = 0; i < READ_RATE_LIMIT_PER_MINUTE; i++) {
        const path = i % 2 === 0 ? '/api/v1/releases/r1' : '/api/v1/current-prod';
        expect((await request(server).get(path)).status).toBe(200);
      }
      for (const path of ['/api/v1/releases/r1', '/api/v1/current-prod']) {
        const res = await request(server).get(path);
        expect(res.status).toBe(429);
        expect(res.body).toEqual({ error: 'too many reads; retry in a minute', code: 'rate_limited' });
      }
      expect(c.getRelease).toHaveBeenCalledTimes(READ_RATE_LIMIT_PER_MINUTE / 2);
      // Submissions keep their own budget.
      expect((await request(server).post('/api/v1/releases').send(body)).status).toBe(202);
      // Another principal has its own read budget.
      who = operator;
      expect((await request(server).get('/api/v1/current-prod')).status).toBe(200);
    });
  });
});
