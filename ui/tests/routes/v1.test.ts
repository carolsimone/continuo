import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import request from 'supertest';
import type { Server } from 'node:http';
import { once } from 'node:events';
import { createV1Router, projectLiveRelease, projectRelease, READ_RATE_LIMIT_PER_MINUTE, SUBMIT_RATE_LIMIT_PER_MINUTE } from '../../src/server/routes/v1';
import { MAINTENANCE_MESSAGE } from '../../src/server/maintenance';
import { HttpError, type ReleaseClient } from '../../src/server/clients/release-client';
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
    listReleases: vi.fn(), getLiveRelease: vi.fn(), retryRemediation: vi.fn(),
    getReleaseCheck: vi.fn(), listReleaseChecks: vi.fn(), getReleaseQueue: vi.fn(),
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

const body = { release_id: 'rel-1', project: 'core', image_tag: 'abc' };

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
    expect(Object.keys((client.submitRelease as ReturnType<typeof vi.fn>).mock.calls[0][0]).sort()).toEqual(['image_tag', 'project', 'release_id']);
  });

  it('refuses a body that names the project as service, naming project, and forwards the same body with project', async () => {
    const client = fakeClient();
    const app = appAs(operator, client);
    const named = { release_id: 'rel-1', service: 'core', image_tag: 'abc' };
    const refused = await request(app).post('/api/v1/releases').send(named);
    expect(refused.status).toBe(400);
    expect(refused.body.error).toMatch(/\bproject\b/);
    expect(client.submitRelease).not.toHaveBeenCalled();

    const { service, ...rest } = named;
    const accepted = await request(app).post('/api/v1/releases').send({ ...rest, project: service });
    expect(accepted.status).toBe(202);
    expect(client.submitRelease).toHaveBeenCalledWith({ release_id: 'rel-1', project: 'core', image_tag: 'abc' });
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

  it('unbound project is forbidden; bootstrap without allowBootstrap is bootstrap_not_allowed', async () => {
    const client = fakeClient();
    const app = appAs(ci({ core: false }), client);
    expect((await request(app).post('/api/v1/releases').send({ ...body, project: 'marketing' })).body.code).toBe('forbidden');
    // The kind ontology of an unbound project is refused the same way: the
    // kind-change 409 of release-controller reaches a caller only after the
    // route has checked the identity's bindings.
    expect((await request(app).post('/api/v1/releases').send({ ...body, project: 'marketing', kind: 'ontology' })).body.code).toBe('forbidden');
    expect((await request(app).post('/api/v1/releases').send({ ...body, bootstrap: true })).body.code).toBe('bootstrap_not_allowed');
    // A repository that matches no binding at all.
    const noneClient = fakeClient();
    const none = await request(appAs(ci({}), noneClient)).post('/api/v1/releases').send(body);
    expect([none.status, none.body.code]).toEqual([403, 'forbidden']);
    expect(client.submitRelease).not.toHaveBeenCalled();
    expect(noneClient.submitRelease).not.toHaveBeenCalled();
  });

  it('forwards rebaseline with bootstrap', async () => {
    const client = fakeClient();
    const res = await request(appAs(ci({ core: true }), client)).post('/api/v1/releases').send({ ...body, bootstrap: true, rebaseline: true });
    expect(res.status).toBe(202);
    expect(client.submitRelease).toHaveBeenCalledWith(expect.objectContaining({ bootstrap: true, rebaseline: true }));
  });

  it('a string rebaseline is a 400', async () => {
    const res = await request(appAs(operator, fakeClient())).post('/api/v1/releases').send({ ...body, rebaseline: 'true' });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/rebaseline must be a boolean/);
  });

  it('rebaseline with bootstrap from a binding without allowBootstrap is bootstrap_not_allowed', async () => {
    const res = await request(appAs(ci({ core: false }), fakeClient())).post('/api/v1/releases').send({ ...body, bootstrap: true, rebaseline: true });
    expect(res.body.code).toBe('bootstrap_not_allowed');
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
    ['missing project', { release_id: 'r', image_tag: 't' }, /project/],
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

  it('maps an upstream maintenance refusal to 503 maintenance, and a plain 503 to upstream_unavailable', async () => {
    const refused = fakeClient({ submitRelease: vi.fn(async () => ({ status: 503, text: JSON.stringify({ error: MAINTENANCE_MESSAGE, code: 'maintenance' }) })) });
    const r = await request(appAs(operator, refused)).post('/api/v1/releases').send(body);
    expect([r.status, r.body.code, r.body.error]).toEqual([503, 'maintenance', MAINTENANCE_MESSAGE]);
    expect(r.headers['retry-after']).toBe('300');
    const plain = fakeClient({ submitRelease: vi.fn(async () => ({ status: 503, text: 'overloaded' })) });
    const p = await request(appAs(operator, plain)).post('/api/v1/releases').send(body);
    expect([p.status, p.body.code]).toEqual([503, 'upstream_unavailable']);
  });

  it("passes release-controller's 409 through with its message to a person, and asks it nothing else", async () => {
    const msg = 'release id already names a different candidate: "rel-1" has a different project, image tag, kind, bootstrap or rebaseline flag, or source change\n';
    const client = fakeClient({ submitRelease: vi.fn(async () => ({ status: 409, text: msg })) });
    const res = await request(appAs(operator, client)).post('/api/v1/releases').send(body);
    expect(res.status).toBe(409);
    expect(res.body).toEqual({ error: msg.trim(), code: 'release_kind_conflict' });
    expect(client.getRelease).not.toHaveBeenCalled();
  });

  it('answers a CI identity 409 with a generic message that names no other release or release check kind', async () => {
    for (const msg of ['check id already names a release check of another kind: rel-1 is a fix\n', 'release id already names a different candidate: "rel-1" has a different project\n']) {
      const client = fakeClient({ submitRelease: vi.fn(async () => ({ status: 409, text: msg })) });
      const res = await request(appAs(ci(), client)).post('/api/v1/releases').send(body);
      expect(res.status).toBe(409);
      expect(res.body).toEqual({ error: 'release id already exists with different content', code: 'release_kind_conflict' });
    }
  });

  describe('the ontology 409s and the ontology 400', () => {
    const post = (p: Principal, text: string, status: number, payload: Record<string, unknown> = body) => {
      const client = fakeClient({ submitRelease: vi.fn(async () => ({ status, text })) });
      return request(appAs(p, client)).post('/api/v1/releases').send(payload);
    };
    const GENERIC = 'release id already exists with different content';

    // A kind change names the caller's own project, which the route has
    // already checked against the identity's bindings, so it reaches both.
    it.each([
      ['a dbt project released as an ontology', 'project "core" is released as kind "dbt": a project cannot change between kind "ontology" and a node kind\n'],
      ['the ontology released as a node kind', 'project "company-ontology" is released as kind "ontology": a project cannot change between kind "ontology" and a node kind\n'],
      ['a python project', 'project "core" is released as kind "python": a project cannot change between kind "ontology" and a node kind\n'],
      ['a name Go has to escape', 'project "co\\"re\\\\x" is released as kind "dbt": a project cannot change between kind "ontology" and a node kind\n'],
    ])('passes the kind-change 409 of %s to a person and to a CI identity as it is', async (_n, text) => {
      for (const p of [operator, ci()]) {
        const res = await post(p, text, 409);
        expect(res.status).toBe(409);
        expect(res.body).toEqual({ error: text.trim(), code: 'release_kind_conflict' });
      }
    });

    // The second ontology project is another project's name: a person reads it,
    // a CI identity learns only the rule.
    it('gives a person the second-ontology 409 as it is, and a CI identity the rule without the other project name', async () => {
      const text = 'project "company-ontology" is the install\'s ontology: an install has one ontology project\n';
      const person = await post(operator, text, 409);
      expect(person.status).toBe(409);
      expect(person.body).toEqual({ error: text.trim(), code: 'release_kind_conflict' });
      const pipeline = await post(ci(), text, 409, { ...body, kind: 'ontology' });
      expect(pipeline.status).toBe(409);
      expect(pipeline.body).toEqual({ error: 'an install has one ontology project', code: 'release_kind_conflict' });
      expect(JSON.stringify(pipeline.body)).not.toContain('company-ontology');
    });

    it('a name Go has to escape does not stop the second-ontology text from being recognised', async () => {
      const res = await post(ci(), 'project "o\\"x\\\\" is the install\'s ontology: an install has one ontology project\n', 409);
      expect(res.body.error).toBe('an install has one ontology project');
    });

    it('keeps every other 409 as it was: the generic text for a CI identity and the upstream text for a person', async () => {
      for (const text of [
        'release id already names a different candidate: "rel-1" has a different project, image tag, kind, bootstrap or rebaseline flag, or source change\n',
        'check id already names a release check of another kind: rel-1 is a fix\n',
        'conflict',
      ]) {
        const ciRes = await post(ci(), text, 409);
        expect(ciRes.body).toEqual({ error: GENERIC, code: 'release_kind_conflict' });
        const person = await post(operator, text, 409);
        expect(person.body).toEqual({ error: text.trim(), code: 'release_kind_conflict' });
      }
    });

    // A text is one of the two only when it is that text whole: a line before or
    // after it, another wording, or a name that does not read as one Go-quoted
    // string leaves it the generic conflict for a CI identity.
    it.each([
      ['a line before the second-ontology text', 'release id already names a different candidate\nproject "o" is the install\'s ontology: an install has one ontology project'],
      ['a line after the second-ontology text', 'project "o" is the install\'s ontology: an install has one ontology project\nrelease id already names a different candidate'],
      ['text after the second-ontology text', 'project "o" is the install\'s ontology: an install has one ontology project, and "rel-2" is queued'],
      ['text before the second-ontology text', 'conflict: project "o" is the install\'s ontology: an install has one ontology project'],
      ['an unquoted project name', 'project o is the install\'s ontology: an install has one ontology project'],
      ['a quote inside the name', 'project "o"x" is the install\'s ontology: an install has one ontology project'],
      ['another wording of the second-ontology text', 'project "o" is the ontology: an install has one ontology project'],
      ['a line before the kind-change text', 'x\nproject "core" is released as kind "dbt": a project cannot change between kind "ontology" and a node kind'],
      ['text after the kind-change text', 'project "core" is released as kind "dbt": a project cannot change between kind "ontology" and a node kind (rel-9)'],
      ['an unquoted kind', 'project "core" is released as kind dbt: a project cannot change between kind "ontology" and a node kind'],
      ['a kind that names another project', 'project "core" is released as kind "dbt" by "other": a project cannot change between kind "ontology" and a node kind'],
      ['a kind-change text of another rule', 'project "core" is released as kind "dbt": a project cannot change between kind "python" and a node kind'],
    ])('answers a CI identity the generic 409 for %s, and a person the text', async (_n, text) => {
      const ciRes = await post(ci(), text, 409);
      expect(ciRes.body).toEqual({ error: GENERIC, code: 'release_kind_conflict' });
      const person = await post(operator, text, 409);
      expect(person.body).toEqual({ error: text.trim(), code: 'release_kind_conflict' });
    });

    // release-controller refuses the kind ontology with a 400 while the install's
    // ontology switch is off; it reaches the caller as any 400 does.
    it('passes the 400 for kind ontology with the switch off through as bad_request, to a person and to a CI identity', async () => {
      const text = 'kind "ontology" is accepted only when the install\'s ontology switch is on (contracts.ontology.enabled)\n';
      for (const p of [operator, ci()]) {
        const client = fakeClient({ submitRelease: vi.fn(async () => ({ status: 400, text })) });
        const res = await request(appAs(p, client)).post('/api/v1/releases').send({ ...body, kind: 'ontology' });
        expect(res.status).toBe(400);
        expect(res.body).toEqual({ error: text.trim(), code: 'bad_request' });
        expect(client.submitRelease).toHaveBeenCalledWith(expect.objectContaining({ kind: 'ontology' }));
      }
    });
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
  it('audits a 202 CI submit with the repository identity, project and bootstrap', async () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      await request(appAs(ci(), fakeClient())).post('/api/v1/releases').send(body);
      const lines = auditLines(spy);
      expect(lines).toHaveLength(1);
      expect(lines[0]).toMatchObject({
        principal: 'ci', repository: 'carolsimone/continuo-demo', repository_id: expect.any(String),
        project: 'core', release_id: 'rel-1', bootstrap: false, rebaseline: false, outcome: 202,
      });
    } finally { spy.mockRestore(); }
  });

  it('audits a 400 submit, recording only string project and release_id', async () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      await request(appAs(operator, fakeClient())).post('/api/v1/releases').send({ ...body, project: 'core', release_id: 7, bootstrap: 'true' });
      const lines = auditLines(spy);
      expect(lines).toHaveLength(1);
      expect(lines[0]).toMatchObject({ principal: 'human', user_id: 'i|o', project: 'core', bootstrap: false, outcome: 400 });
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
      expect(lines[lines.length - 1]).toMatchObject({ principal: 'ci', repository: 'carolsimone/continuo-demo', project: 'core', outcome: 429 });
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
  const rel = { release_id: 'r.1', status: 'rejected', changed_project: 'core', bootstrap: false, repo: 'carolsimone/continuo-demo', commit_sha: 'abc', reject_reason: 'validation_failed', reject_detail: 'x', per_node_results: [{}], failing_nodes: ['a'] };

  it('returns the v1 projection only, with terminal and ui_url', async () => {
    const client = fakeClient({ getRelease: vi.fn(async () => rel) });
    const res = await request(appAs(ci(), client)).get('/api/v1/releases/r.1');
    expect(res.status).toBe(200);
    expect(client.getRelease).toHaveBeenCalledWith('r.1');
    expect(res.body).toEqual({
      release_id: 'r.1', project: 'core', status: 'rejected', terminal: true, bootstrap: false, rebaseline: false,
      repo: 'carolsimone/continuo-demo', commit_sha: 'abc', reject_reason: 'validation_failed', reject_detail: 'x',
      ui_url: 'https://continuo.example.com/releases/r.1', notices: [], continuo_contracts: '',
    });
  });

  it('returns continuo_contracts for a rejected release', async () => {
    const rejected = {
      release_id: 'rel-4e5f6a7-213', changed_project: 'service-py', status: 'rejected', bootstrap: false,
      repo: 'carolsimone/continuo-demo', commit_sha: '4e5f6a7', reject_reason: 'invalid_contract',
      reject_detail: '1 node failed to parse: analytics.py_daily_kpis', continuo_contracts: '0.2.0',
    };
    const client = fakeClient({ getRelease: vi.fn(async () => rejected) });
    const res = await request(appAs(ci({ 'service-py': false }), client)).get('/api/v1/releases/rel-4e5f6a7-213');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      release_id: 'rel-4e5f6a7-213', project: 'service-py', status: 'rejected', terminal: true, bootstrap: false, rebaseline: false,
      repo: 'carolsimone/continuo-demo', commit_sha: '4e5f6a7', reject_reason: 'invalid_contract',
      reject_detail: '1 node failed to parse: analytics.py_daily_kpis',
      ui_url: 'https://continuo.example.com/releases/rel-4e5f6a7-213', notices: [], continuo_contracts: '0.2.0',
    });
  });

  it('returns an empty continuo_contracts before the parse ends', async () => {
    const parsing = { ...rel, status: 'validating', reject_reason: '', reject_detail: '', continuo_contracts: '' };
    const client = fakeClient({ getRelease: vi.fn(async () => parsing) });
    const res = await request(appAs(ci(), client)).get('/api/v1/releases/r.1');
    expect(res.status).toBe(200);
    expect(res.body.continuo_contracts).toBe('');
    // A release-controller answer without the field reads the same.
    expect(projectRelease({ ...rel, status: 'validating' }).continuo_contracts).toBe('');
  });

  it('does not return the contract bundle uri or its hash', async () => {
    const bundled = {
      ...rel, status: 'validating', continuo_contracts: '0.2.0',
      contract_bundle_uri: 's3://continuo-dev/tenants/default/topologies/r.1/contracts.json.gz',
      contract_bundle_sha256: '9b0c4f',
    };
    const client = fakeClient({ getRelease: vi.fn(async () => bundled) });
    const res = await request(appAs(ci(), client)).get('/api/v1/releases/r.1');
    expect(res.status).toBe(200);
    expect(Object.keys(res.body).filter((k) => k.startsWith('contract_bundle'))).toEqual([]);
    expect(JSON.stringify(res.body)).not.toContain('contracts.json.gz');
    expect(JSON.stringify(res.body)).not.toContain('9b0c4f');
    expect(res.body.continuo_contracts).toBe('0.2.0');
  });

  it('does not return the parse timings release-controller keeps for its own readers', async () => {
    const parsed = {
      ...rel, status: 'validating', continuo_contracts: '0.2.0',
      parse_ms: 4180, contracts_ms: 1260, contracts_by_family: { dbt: 3, python: 2 },
    };
    const client = fakeClient({ getRelease: vi.fn(async () => parsed) });
    const res = await request(appAs(ci(), client)).get('/api/v1/releases/r.1');
    expect(res.status).toBe(200);
    expect(Object.keys(res.body).filter((k) => ['parse_ms', 'contracts_ms', 'contracts_by_family'].includes(k))).toEqual([]);
    expect(JSON.stringify(res.body)).not.toContain('4180');
  });

  it('projects the seed reload notices of a release, field by field', async () => {
    const notice = {
      kind: 'seed_reload', node_id: 'analytics.seed_users', project: 'analytics', engine_family: 'schema_bound',
      change: 'columns_changed', columns: { added: ['plan'], removed: [], retyped: ['age'] },
      load: { operation: 'full_refresh', drops_table: true, reason: 'columns changed' },
      rebuilds: [{ node_id: 'analytics.v_users', materialized: 'view', operation: 'run', schedule: 'daily' }],
      refresh_on_schedule: [{ node_id: 'analytics.m_users', materialized: 'table', schedule: 'daily' }],
      warehouse: { checked: true, completeness: 'complete', outside_continuo: [{ object: 'e2e_bi.seed_reader', kind: 'view', owner: 'bi' }] },
      prediction: 'stops', stop_reason: 'unowned_dependents', unvalidated: false, extra_key: 'x',
    };
    const client = fakeClient({ getRelease: vi.fn(async () => ({ ...rel, notices: [notice] })) });
    const res = await request(appAs(ci(), client)).get('/api/v1/releases/r.1');
    expect(res.body.notices).toEqual([{
      kind: 'seed_reload', node_id: 'analytics.seed_users', project: 'analytics', change: 'columns_changed',
      load: 'full_refresh', columns: { added: ['plan'], removed: [], retyped: ['age'] },
      rebuilds: [{ node_id: 'analytics.v_users', operation: 'run' }],
      stops_on: ['e2e_bi.seed_reader'],
    }]);
  });

  it('a notice that reloads has no stops_on, whatever the warehouse holds outside continuo', () => {
    const notice = {
      kind: 'seed_reload', node_id: 'a.s', project: 'a', change: 'rows_changed',
      load: { operation: 'run' }, warehouse: { outside_continuo: [{ object: 'x.y' }] }, prediction: 'reloads',
    };
    expect(projectRelease({ ...rel, notices: [notice] }).notices).toEqual([{
      kind: 'seed_reload', node_id: 'a.s', project: 'a', change: 'rows_changed', load: 'run',
      columns: { added: [], removed: [], retyped: [] }, rebuilds: [], stops_on: [],
    }]);
  });

  it.each([
    ['absent', undefined],
    ['a string', 'oops'],
    ['an object', { kind: 'seed_reload' }],
    ['null', null],
  ])('notices is [] when release-controller sends %s', (_label, notices) => {
    expect(projectRelease({ ...rel, notices }).notices).toEqual([]);
  });

  it('a notice of the wrong shape is dropped, a field of the wrong type is its empty value', () => {
    const odd = [1, null, 'x', { kind: 7, node_id: [], load: { operation: 3 }, columns: { added: 'a', removed: [1, 'b'] }, rebuilds: [{ node_id: 2 }, 'z'] }];
    expect(projectRelease({ ...rel, notices: odd }).notices).toEqual([{
      kind: '', node_id: '', project: '', change: '', load: '',
      columns: { added: [], removed: ['b'], retyped: [] }, rebuilds: [{ node_id: '', operation: '' }], stops_on: [],
    }]);
  });

  it('a release of a project the CI identity is not bound to reads as not found, without naming the project', async () => {
    const other = fakeClient({ getRelease: vi.fn(async () => ({ ...rel, changed_project: 'marketing' })) });
    const res = await request(appAs(ci(), other)).get('/api/v1/releases/x');
    expect(res.status).toBe(404);
    expect(res.body).toEqual({ error: 'release not found', code: 'not_found' });
    // A person reads every release.
    expect((await request(appAs(viewer, other)).get('/api/v1/releases/x')).status).toBe(200);
  });

  it('an id outside the release_id pattern is 404 without asking release-controller', async () => {
    const client = fakeClient();
    for (const id of ['r%2F1', '..%2Flive-release', '.hidden', `a${'b'.repeat(128)}`]) {
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

  it('projectRelease carries rebaseline', () => {
    expect(projectRelease({ release_id: 'r', status: 'promoted', changed_project: 'core', bootstrap: true, rebaseline: true }).rebaseline).toBe(true);
    expect(projectRelease({ release_id: 'r', status: 'promoted', changed_project: 'core' }).rebaseline).toBe(false);
  });

  it('projectRelease: non-terminal status and no publicUrl gives ui_url null', () => {
    expect(projectRelease({ ...rel, status: 'validating' })).toMatchObject({ terminal: false, ui_url: null });
  });

  it('projectRelease: a field of the wrong type becomes its empty value, never a different shape', () => {
    const odd = { release_id: 7, status: null, changed_project: ['core'], bootstrap: 'true', repo: 1, commit_sha: {}, reject_reason: false, reject_detail: { msg: 'x' } };
    expect(projectRelease(odd)).toEqual({
      release_id: '', project: '', status: '', terminal: false, bootstrap: false, rebaseline: false,
      repo: '', commit_sha: '', reject_reason: '', reject_detail: '', ui_url: null, notices: [], continuo_contracts: '',
    });
  });

  it('projectRelease: a continuo_contracts of the wrong type is the empty string', () => {
    for (const odd of [0.2, null, ['0.2.0'], { v: '0.2.0' }, true]) {
      expect(projectRelease({ ...rel, continuo_contracts: odd }).continuo_contracts).toBe('');
    }
  });

  it('503 when release-controller answers a body that is not a JSON object', async () => {
    const client = fakeClient({ getRelease: vi.fn(async () => null) });
    const res = await request(appAs(operator, client)).get('/api/v1/releases/x');
    expect([res.status, res.body.code]).toEqual([503, 'upstream_unavailable']);
  });
});

describe('GET /api/v1/live-release', () => {
  const cp = () => fakeClient({ getLiveRelease: vi.fn(async () => ({ live_release_id: 'r', node_count: 3, updated_at: 't', extra: 1 })) });

  it('a bound CI identity and a viewer read exactly the three fields', async () => {
    for (const p of [ci(), viewer]) {
      const res = await request(appAs(p, cp())).get('/api/v1/live-release');
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ live_release_id: 'r', node_count: 3, updated_at: 't' });
    }
  });

  it('a field of the wrong type becomes its empty value', () => {
    expect(projectLiveRelease({ live_release_id: null, node_count: '3', updated_at: 17 })).toEqual({ live_release_id: '', node_count: 0, updated_at: '' });
  });

  it('503 when release-controller answers a body that is not a JSON object', async () => {
    const client = fakeClient({ getLiveRelease: vi.fn(async () => 'oops') });
    const res = await request(appAs(viewer, client)).get('/api/v1/live-release');
    expect([res.status, res.body.code]).toEqual([503, 'upstream_unavailable']);
  });

  it('a CI identity bound to no project is forbidden', async () => {
    const client = cp();
    const res = await request(appAs(ci({}), client)).get('/api/v1/live-release');
    expect(res.status).toBe(403);
    expect(res.body.code).toBe('forbidden');
    expect(client.getLiveRelease).not.toHaveBeenCalled();
  });
});

describe('read rate limit', () => {
  it('audits a rate-limited read with the principal, path and outcome 429', async () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      const c = fakeClient({ getLiveRelease: vi.fn(async () => ({ live_release_id: 'r', node_count: 1, updated_at: 't' })) });
      await withServer(appAs(ci(), c), async (server) => {
        for (let i = 0; i < READ_RATE_LIMIT_PER_MINUTE; i++) await request(server).get('/api/v1/live-release');
        expect((await request(server).get('/api/v1/live-release')).status).toBe(429);
      });
      const lines = spy.mock.calls
        .map((call) => { try { return JSON.parse(String(call[0])); } catch { return null; } })
        .filter((l) => l !== null && l.audit === true && l.event === 'release_read_limited');
      expect(lines).toHaveLength(1);
      expect(lines[0]).toMatchObject({
        principal: 'ci', repository: 'carolsimone/continuo-demo', method: 'GET', path: '/api/v1/live-release', outcome: 429,
      });
    } finally { spy.mockRestore(); }
  });


  it('caps release and live-release reads per principal, on a counter separate from submissions', async () => {
    const c = fakeClient({
      getRelease: vi.fn(async () => ({ release_id: 'r1', status: 'validating', changed_project: 'core' })),
      getLiveRelease: vi.fn(async () => ({ live_release_id: 'r', node_count: 1, updated_at: 't' })),
    });
    const app = express();
    app.use(express.json());
    let who: Principal = ci();
    app.use((req, _res, next) => { req.principal = who; next(); });
    app.use('/api/v1', createV1Router(c));

    await withServer(app, async (server) => {
      for (let i = 0; i < READ_RATE_LIMIT_PER_MINUTE; i++) {
        const path = i % 2 === 0 ? '/api/v1/releases/r1' : '/api/v1/live-release';
        expect((await request(server).get(path)).status).toBe(200);
      }
      for (const path of ['/api/v1/releases/r1', '/api/v1/live-release']) {
        const res = await request(server).get(path);
        expect(res.status).toBe(429);
        expect(res.body).toEqual({ error: 'too many reads; retry in a minute', code: 'rate_limited' });
      }
      expect(c.getRelease).toHaveBeenCalledTimes(READ_RATE_LIMIT_PER_MINUTE / 2);
      // Submissions keep their own budget.
      expect((await request(server).post('/api/v1/releases').send(body)).status).toBe(202);
      // Another principal has its own read budget.
      who = operator;
      expect((await request(server).get('/api/v1/live-release')).status).toBe(200);
    });
  });
});
