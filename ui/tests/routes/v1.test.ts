import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import request from 'supertest';
import { createV1Router, projectRelease, SUBMIT_RATE_LIMIT_PER_MINUTE } from '../../src/server/routes/v1';
import { HttpError, type ReleaseClient } from '../../src/server/release-client';
import type { Principal } from '../../src/server/auth/principal';
import { githubClaimsFrom } from '../../src/server/auth/principal';
import { githubClaims } from '../auth/stub-issuer';

const operator: Principal = { kind: 'human', user: { userId: 'i|o', email: 'o@c.com', name: 'O', role: 'operator' } };
const viewer: Principal = { kind: 'human', user: { userId: 'i|v', email: 'v@c.com', name: 'V', role: 'viewer' } };
const ci = (grants: Record<string, boolean> = { core: false }): Principal => ({
  kind: 'ci', subject: 's', claims: githubClaimsFrom(githubClaims()),
  grants: new Map(Object.entries(grants).map(([s, b]) => [s, { allowBootstrap: b }])),
});

function fakeClient(over: Partial<ReleaseClient> = {}): ReleaseClient {
  return {
    listReleases: vi.fn(), getRelease: vi.fn(), getCurrentProd: vi.fn(), retryRemediation: vi.fn(),
    getVerificationRun: vi.fn(), listVerificationRuns: vi.fn(), getPipeline: vi.fn(),
    submitRelease: vi.fn(async (b) => ({ status: 202, text: JSON.stringify({ release_id: b.release_id, status: 'received' }) })),
    ...over,
  } as ReleaseClient;
}

function appAs(p: Principal, client: ReleaseClient, publicUrl = 'https://continuo.example.com') {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => { req.principal = p; next(); });
  app.use('/api/v1', createV1Router(client, publicUrl));
  return app;
}

const body = { release_id: 'rel-1', service: 'core', image_tag: 'abc' };

describe('POST /api/v1/releases', () => {
  it('a bound CI identity submits; repo and commit_sha are filled from the token', async () => {
    const client = fakeClient();
    const res = await request(appAs(ci(), client)).post('/api/v1/releases').send(body);
    expect(res.status).toBe(202);
    expect(res.body).toEqual({ release_id: 'rel-1', status: 'received' });
    expect(client.submitRelease).toHaveBeenCalledWith({ ...body, repo: 'carolsimone/continuo-demo', commit_sha: 'abc1234def' });
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

  it('rate-limits submissions per principal', async () => {
    const app = appAs(ci(), fakeClient());
    for (let i = 0; i < SUBMIT_RATE_LIMIT_PER_MINUTE; i++) {
      expect((await request(app).post('/api/v1/releases').send({ ...body, release_id: `r${i}` })).status).toBe(202);
    }
    const res = await request(app).post('/api/v1/releases').send(body);
    expect(res.status).toBe(429);
    expect(res.body.code).toBe('rate_limited');
  });
});

describe('GET /api/v1/releases/:id', () => {
  const rel = { release_id: 'r/1', status: 'rejected', changed_service: 'core', bootstrap: false, repo: 'carolsimone/continuo-demo', commit_sha: 'abc', reject_reason: 'validation_failed', reject_detail: 'x', per_node_results: [{}], failing_nodes: ['a'] };

  it('returns the v1 projection only, with terminal and ui_url', async () => {
    const client = fakeClient({ getRelease: vi.fn(async () => rel) });
    const res = await request(appAs(ci(), client)).get('/api/v1/releases/r%2F1');
    expect(res.status).toBe(200);
    expect(client.getRelease).toHaveBeenCalledWith('r/1');
    expect(res.body).toEqual({
      release_id: 'r/1', service: 'core', status: 'rejected', terminal: true, bootstrap: false,
      repo: 'carolsimone/continuo-demo', commit_sha: 'abc', reject_reason: 'validation_failed', reject_detail: 'x',
      ui_url: 'https://continuo.example.com/releases/r%2F1',
    });
  });

  it('CI cannot read a release of an unbound service; 404 and 503 map through', async () => {
    const other = fakeClient({ getRelease: vi.fn(async () => ({ ...rel, changed_service: 'marketing' })) });
    expect((await request(appAs(ci(), other)).get('/api/v1/releases/x')).status).toBe(403);
    const missing = fakeClient({ getRelease: vi.fn(async () => { throw new HttpError(404, 'nf'); }) });
    expect((await request(appAs(operator, missing)).get('/api/v1/releases/x')).body.code).toBe('not_found');
    const down = fakeClient({ getRelease: vi.fn(async () => { throw new TypeError('fetch failed'); }) });
    expect((await request(appAs(operator, down)).get('/api/v1/releases/x')).status).toBe(503);
  });

  it('projectRelease: non-terminal status and no publicUrl gives ui_url null', () => {
    expect(projectRelease({ ...rel, status: 'validating' })).toMatchObject({ terminal: false, ui_url: null });
  });
});

describe('GET /api/v1/current-prod', () => {
  it('any principal reads exactly the three fields', async () => {
    const client = fakeClient({ getCurrentProd: vi.fn(async () => ({ current_prod_release_id: 'r', node_count: 3, updated_at: 't', extra: 1 })) });
    const res = await request(appAs(ci({}), client)).get('/api/v1/current-prod');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ current_prod_release_id: 'r', node_count: 3, updated_at: 't' });
  });
});
