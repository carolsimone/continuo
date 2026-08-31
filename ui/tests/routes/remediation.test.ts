import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import request from 'supertest';
import { createRemediationRouter } from '../../src/server/routes/remediation';
import type { RemediationClient } from '../../src/server/remediation-client';
import type { PullRequestCreator } from '../../src/server/github/pull-request-creator';

// Constructs a gRPC-style error with the given code and message, mirroring what
// @grpc/grpc-js rejects with.
function grpcError(code: number, message: string): Error {
  const err: any = new Error(message);
  err.code = code;
  err.details = message;
  return err;
}

// gRPC status codes (subset used in these tests).
const GRPC_NOT_FOUND = 5;
const GRPC_FAILED_PRECONDITION = 9;

function grpcNotFound(msg: string): Error {
  return grpcError(GRPC_NOT_FOUND, msg);
}

function grpcFailedPrecondition(msg: string): Error {
  return grpcError(GRPC_FAILED_PRECONDITION, msg);
}

function makeRemediation(overrides: Partial<RemediationClient> = {}): RemediationClient {
  return {
    listProposals: vi.fn().mockResolvedValue({ proposals: [] }),
    // The default proposal carries no pr_services, so the route falls back
    // to the single legacy group (service '').
    getProposal: vi.fn().mockResolvedValue({}),
    beginPullRequest: vi.fn().mockResolvedValue({
      proposed_sql_uri: 's3://continuo/proposals/p1/fix.sql',
      diff_uri: 's3://continuo/proposals/p1/fix.diff',
      branch: 'remediation/p1',
      file_path: 'models/mymodel.sql',
      repo: 'owner/repo',
      commit_sha: 'abc123',
      claimed_at: '2026-06-24T00:00:00Z',
      service: '',
      edits: [
        {
          path: 'models/mymodel.sql',
          content_uri: 's3://continuo/proposals/p1/fix.sql',
          diff_uri: 's3://continuo/proposals/p1/fix.diff',
        },
      ],
    }),
    recordPullRequest: vi.fn().mockResolvedValue({}),
    failPullRequest: vi.fn().mockResolvedValue({}),
    ...overrides,
  };
}

function makePrCreator(overrides: Partial<PullRequestCreator> = {}): PullRequestCreator {
  return {
    create: vi.fn().mockResolvedValue({ url: 'https://github.com/owner/repo/pull/42', number: 42 }),
    ...overrides,
  };
}

function makeGetObject(content = 'SELECT 1') {
  return vi.fn().mockResolvedValue(content);
}

function appWith(
  deps: {
    remediation: RemediationClient;
    prCreator?: PullRequestCreator;
    getObject: (key: string) => Promise<string>;
  },
  user?: { userId: string; role: string; email: string; name: string },
) {
  const app = express();
  app.use(express.json());
  // Inject a synthetic req.user when the caller supplies one, mimicking the
  // app-level auth middleware that normally does this.
  if (user) {
    app.use((req: any, _res: any, next: any) => {
      req.user = user;
      next();
    });
  }
  // Mount without apiGuards — operator gating is tested separately via the
  // app-level auth middleware; here we verify route behaviour in isolation.
  app.use('/api/remediation', createRemediationRouter(deps.remediation, deps.prCreator, deps.getObject));
  return app;
}

describe('remediation router', () => {
  // ── GET /proposals ────────────────────────────────────────────────────────

  it('proxies list proposals to the client with query params', async () => {
    const remediation = makeRemediation({
      listProposals: vi.fn().mockResolvedValue({ proposals: [{ id: 'p1' }] }),
    });
    const app = appWith({ remediation, getObject: makeGetObject() });

    const res = await request(app).get('/api/remediation/proposals?status=pending&pr_state=open');
    expect(res.status).toBe(200);
    expect(res.body.proposals[0].id).toBe('p1');
    expect(remediation.listProposals).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'pending', pr_state: 'open' }),
    );
  });

  it('returns empty proposals list when none are present', async () => {
    const remediation = makeRemediation({
      listProposals: vi.fn().mockResolvedValue({ proposals: [] }),
    });
    const app = appWith({ remediation, getObject: makeGetObject() });

    const res = await request(app).get('/api/remediation/proposals');
    expect(res.status).toBe(200);
    expect(res.body.proposals).toEqual([]);
  });

  // ── GET /proposals/:id ────────────────────────────────────────────────────

  it('returns a proposal by id', async () => {
    const remediation = makeRemediation({
      getProposal: vi.fn().mockResolvedValue({ id: 'p1', status: 'pending' }),
    });
    const app = appWith({ remediation, getObject: makeGetObject() });

    const res = await request(app).get('/api/remediation/proposals/p1');
    expect(res.status).toBe(200);
    expect(res.body.id).toBe('p1');
    expect(remediation.getProposal).toHaveBeenCalledWith({ id: 'p1' });
  });

  it('returns 404 when the proposal does not exist', async () => {
    const remediation = makeRemediation({
      getProposal: vi.fn().mockRejectedValue(grpcNotFound('proposal not found')),
    });
    const app = appWith({ remediation, getObject: makeGetObject() });

    const res = await request(app).get('/api/remediation/proposals/missing');
    expect(res.status).toBe(404);
  });

  // ── POST /proposals/:id/pull-request — 503 when prCreator not configured ──

  it('returns 503 when prCreator is undefined, without calling getProposal or begin', async () => {
    const remediation = makeRemediation();
    const app = appWith({ remediation, prCreator: undefined, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(503);
    expect(res.body.error).toMatch(/not configured/i);
    expect(remediation.getProposal).not.toHaveBeenCalled();
    expect(remediation.beginPullRequest).not.toHaveBeenCalled();
  });

  // ── POST — NotFound during getProposal → 404 ──────────────────────────────

  it('returns 404 when the proposal is not found', async () => {
    const remediation = makeRemediation({
      getProposal: vi.fn().mockRejectedValue(grpcNotFound('proposal not found')),
    });
    const prCreator = makePrCreator();
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/missing/pull-request');
    expect(res.status).toBe(404);
    expect(remediation.beginPullRequest).not.toHaveBeenCalled();
    expect(prCreator.create).not.toHaveBeenCalled();
  });

  // ── POST — a service that already has an open PR is skipped, not errored ─

  it('skips a service whose PR is already open (FAILED_PRECONDITION) and lists its existing URL', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockRejectedValue(
        grpcFailedPrecondition('https://github.com/owner/repo/pull/7'),
      ),
    });
    const prCreator = makePrCreator();
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(200);
    expect(res.body.pull_requests).toEqual([
      { service: '', pr_url: 'https://github.com/owner/repo/pull/7', pr_number: 7 },
    ]);
    expect(res.body.errors).toEqual([]);
    expect(prCreator.create).not.toHaveBeenCalled();
  });

  // ── POST — a FAILED_PRECONDITION with no embedded URL is a genuine failure,
  //    not an already-open PR (ErrNotSourceResolved / ErrNotProposed) ────────

  it('reports a URL-less FAILED_PRECONDITION as a per-service failure, not a fabricated success', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockRejectedValue(
        grpcFailedPrecondition('proposal is not in status proposed'),
      ),
    });
    const prCreator = makePrCreator();
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    expect(res.body.pull_requests).toEqual([]);
    expect(res.body.errors).toEqual([
      { service: '', error: 'proposal is not in status proposed' },
    ]);
    expect(prCreator.create).not.toHaveBeenCalled();
    // No dead link should ever be assembled for this service.
    expect(res.body.pull_requests.some((pr: any) => pr.pr_url === '')).toBe(false);
  });

  // ── POST — happy path ─────────────────────────────────────────────────────

  it('happy path: begin → getObject → create → record, returns 200 with one pull_requests entry', async () => {
    const remediation = makeRemediation();
    const prCreator = makePrCreator();
    const getObject = makeGetObject('SELECT 1 -- fixed');
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(200);
    expect(res.body.pull_requests).toEqual([
      { service: '', pr_url: 'https://github.com/owner/repo/pull/42', pr_number: 42 },
    ]);
    expect(res.body.errors).toEqual([]);

    expect(remediation.getProposal).toHaveBeenCalledWith({ id: 'p1' });
    expect(remediation.beginPullRequest).toHaveBeenCalledWith({ id: 'p1', service: '' });
    // getObject called at least once for the proposed SQL
    expect(getObject).toHaveBeenCalled();
    expect(prCreator.create).toHaveBeenCalledWith(
      expect.objectContaining({
        repo: 'owner/repo',
        headBranch: 'remediation/p1',
        baseBranch: 'main',
        baseSha: 'abc123',
        files: [{ path: 'models/mymodel.sql', content: 'SELECT 1 -- fixed' }],
      }),
    );
    expect(remediation.recordPullRequest).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'p1',
        service: '',
        pr_url: 'https://github.com/owner/repo/pull/42',
        pr_number: 42,
      }),
    );
  });

  it('titles a single-node proposal by its one node_id, as before batching existed', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockResolvedValue({
        proposed_sql_uri: 's3://continuo/proposals/p1/fix.sql',
        diff_uri: 's3://continuo/proposals/p1/fix.diff',
        branch: 'remediation/p1',
        file_path: 'models/mymodel.sql',
        repo: 'owner/repo',
        commit_sha: 'abc123',
        claimed_at: '2026-06-24T00:00:00Z',
        node_id: 'svc.schema.mymodel',
        release_id: 'rel-1',
        // No resolved_node_ids — a legacy row, or a proposal predating batching.
        edits: [
          {
            path: 'models/mymodel.sql',
            content_uri: 's3://continuo/proposals/p1/fix.sql',
            diff_uri: 's3://continuo/proposals/p1/fix.diff',
          },
        ],
      }),
    });
    const prCreator = makePrCreator();
    const getObject = makeGetObject('SELECT 1 -- fixed');
    const app = appWith({ remediation, prCreator, getObject });

    await request(app).post('/api/remediation/proposals/p1/pull-request');

    const call = (prCreator.create as any).mock.calls[0][0];
    expect(call.title).toBe('[remediation] fix svc.schema.mymodel (release rel-1)');
    expect(call.body).toContain('**Nodes:** `svc.schema.mymodel`');
  });

  it('strips s3:// prefix when fetching the proposed SQL content', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockResolvedValue({
        proposed_sql_uri: 's3://continuo/proposals/p1/fix.sql',
        diff_uri: '',
        branch: 'remediation/p1',
        file_path: 'models/mymodel.sql',
        repo: 'owner/repo',
        edits: [
          {
            path: 'models/mymodel.sql',
            content_uri: 's3://continuo/proposals/p1/fix.sql',
            diff_uri: '',
          },
        ],
      }),
    });
    const prCreator = makePrCreator();
    const getObject = makeGetObject('SELECT 1');
    const app = appWith({ remediation, prCreator, getObject });

    await request(app).post('/api/remediation/proposals/p1/pull-request');
    // Must strip the s3://<bucket>/ prefix before calling getObject
    expect(getObject).toHaveBeenCalledWith(expect.not.stringContaining('s3://'));
  });

  // ── POST — multi-file claims (a single service group with several edits) ─

  function multiEditRemediation() {
    return makeRemediation({
      beginPullRequest: vi.fn().mockResolvedValue({
        proposed_sql_uri: 's3://continuo/proposals/p1/contract.yml',
        diff_uri: 's3://continuo/proposals/p1/contract.diff',
        branch: 'remediation/p1',
        file_path: 'contracts/a.yml',
        repo: 'owner/repo',
        commit_sha: 'abc123',
        claimed_at: '2026-06-24T00:00:00Z',
        node_id: 'svc.schema.a',
        release_id: 'rel-1',
        resolved_node_ids: ['s.a', 's.b'],
        edits: [
          {
            path: 'contracts/a.yml',
            content_uri: 's3://continuo/proposals/p1/contract.yml',
            diff_uri: 's3://continuo/proposals/p1/contract.diff',
            target_node_id: 's.a',
          },
          {
            path: 'scripts/a.py',
            content_uri: 's3://continuo/proposals/p1/script.py',
            diff_uri: 's3://continuo/proposals/p1/script.diff',
            target_node_id: 's.b',
          },
        ],
      }),
    });
  }

  it('fetches every edit content and passes one file per edit to the PR creator', async () => {
    const remediation = multiEditRemediation();
    const prCreator = makePrCreator();
    const getObject = vi.fn().mockImplementation(async (key: string) => `content of ${key}`);
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(200);

    expect(getObject).toHaveBeenCalledWith('proposals/p1/contract.yml');
    expect(getObject).toHaveBeenCalledWith('proposals/p1/script.py');
    expect(prCreator.create).toHaveBeenCalledWith(
      expect.objectContaining({
        files: [
          { path: 'contracts/a.yml', content: 'content of proposals/p1/contract.yml', target_node_id: 's.a' },
          { path: 'scripts/a.py', content: 'content of proposals/p1/script.py', target_node_id: 's.b' },
        ],
      }),
    );
  });

  it('lists every edited path in the PR body and inlines only the first edit diff', async () => {
    const remediation = multiEditRemediation();
    const prCreator = makePrCreator();
    const getObject = vi.fn().mockImplementation(async (key: string) => `content of ${key}`);
    const app = appWith({ remediation, prCreator, getObject });

    await request(app).post('/api/remediation/proposals/p1/pull-request');

    const body: string = (prCreator.create as any).mock.calls[0][0].body;
    expect(body).toContain('contracts/a.yml');
    expect(body).toContain('scripts/a.py');
    // The inline preview comes from the first edit's diff, not from a second one.
    expect(getObject).toHaveBeenCalledWith('proposals/p1/contract.diff');
    expect(getObject).not.toHaveBeenCalledWith('proposals/p1/script.diff');
    expect(body).toContain('content of proposals/p1/contract.diff');
  });

  it('titles a batched proposal by node count and lists every resolved node and its target file in the body', async () => {
    const remediation = multiEditRemediation();
    const prCreator = makePrCreator();
    const getObject = vi.fn().mockImplementation(async (key: string) => `content of ${key}`);
    const app = appWith({ remediation, prCreator, getObject });

    await request(app).post('/api/remediation/proposals/p1/pull-request');

    const call = (prCreator.create as any).mock.calls[0][0];
    expect(call.title).toBe('[remediation] fix 2 nodes (release rel-1)');
    expect(call.body).toContain('**Nodes:**');
    expect(call.body).toContain('`s.a`');
    expect(call.body).toContain('`s.b`');
    expect(call.body).toContain('`contracts/a.yml` (fixes `s.a`)');
    expect(call.body).toContain('`scripts/a.py` (fixes `s.b`)');
  });

  it('fails the claim and reports a per-service error when a single edit content fetch rejects', async () => {
    const remediation = multiEditRemediation();
    const prCreator = makePrCreator();
    const getObject = vi.fn().mockImplementation(async (key: string) => {
      if (key === 'proposals/p1/script.py') throw new Error('S3 NoSuchKey');
      return 'ok';
    });
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    expect(res.body.pull_requests).toEqual([]);
    expect(res.body.errors).toEqual([{ service: '', error: 'failed to fetch proposed file content from S3' }]);
    expect(prCreator.create).not.toHaveBeenCalled();
    expect(remediation.failPullRequest).toHaveBeenCalledWith({
      id: 'p1',
      claimed_at: '2026-06-24T00:00:00Z',
      service: '',
    });
  });

  // ── POST — an empty edits list ────────────────────────────────────────────

  it('falls back to the single-file claim fields when the edits list is empty', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockResolvedValue({
        proposed_sql_uri: 's3://continuo/proposals/p1/fix.sql',
        diff_uri: 's3://continuo/proposals/p1/fix.diff',
        branch: 'remediation/p1',
        file_path: 'models/mymodel.sql',
        repo: 'owner/repo',
        commit_sha: 'abc123',
        claimed_at: '2026-06-24T00:00:00Z',
        // Under this project's proto-loader options an empty repeated field
        // arrives as [], which is what a peer that predates the edits list
        // sends.
        edits: [],
      }),
    });
    const prCreator = makePrCreator();
    const getObject = makeGetObject('SELECT 1 -- fixed');
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(200);
    expect(res.body.pull_requests).toEqual([
      { service: '', pr_url: 'https://github.com/owner/repo/pull/42', pr_number: 42 },
    ]);
    expect(prCreator.create).toHaveBeenCalledWith(
      expect.objectContaining({
        files: [{ path: 'models/mymodel.sql', content: 'SELECT 1 -- fixed' }],
      }),
    );
    expect(remediation.failPullRequest).not.toHaveBeenCalled();
    expect(remediation.recordPullRequest).toHaveBeenCalled();
  });

  it('returns 502 and fails the claim when there is neither an edits list nor a file path', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockResolvedValue({
        proposed_sql_uri: '',
        diff_uri: '',
        branch: 'remediation/p1',
        file_path: '',
        repo: 'owner/repo',
        commit_sha: 'abc123',
        claimed_at: '2026-06-24T00:00:00Z',
        edits: [],
      }),
    });
    const prCreator = makePrCreator();
    const getObject = makeGetObject();
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    expect(res.body.pull_requests).toEqual([]);
    expect(res.body.errors).toEqual([{ service: '', error: 'proposal carries no file edits' }]);
    // Nothing to commit, so no empty-tree PR and no S3 read.
    expect(getObject).not.toHaveBeenCalled();
    expect(prCreator.create).not.toHaveBeenCalled();
    expect(remediation.recordPullRequest).not.toHaveBeenCalled();
    expect(remediation.failPullRequest).toHaveBeenCalledWith({
      id: 'p1',
      claimed_at: '2026-06-24T00:00:00Z',
      service: '',
    });

    const logged = consoleError.mock.calls.map((call) => call.join(' ')).join('\n');
    expect(logged).toContain('p1');

    consoleError.mockRestore();
  });

  // ── POST — GitHub failure → failPullRequest + per-service error ──────────

  it('fails the claim and reports a per-service error when prCreator.create throws', async () => {
    const remediation = makeRemediation();
    const prCreator = makePrCreator({
      create: vi.fn().mockRejectedValue(new Error('GitHub API error')),
    });
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    expect(res.body.pull_requests).toEqual([]);
    expect(res.body.errors).toEqual([{ service: '', error: 'failed to open pull request' }]);
    expect(remediation.failPullRequest).toHaveBeenCalledWith({ id: 'p1', claimed_at: '2026-06-24T00:00:00Z', service: '' });
    expect(remediation.recordPullRequest).not.toHaveBeenCalled();
  });

  it('logs the proposal id and the Octokit error status/message when prCreator.create throws, without leaking it to the client', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const remediation = makeRemediation();
    const githubError = Object.assign(new Error('Bad credentials'), { status: 401 });
    const prCreator = makePrCreator({
      create: vi.fn().mockRejectedValue(githubError),
    });
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    // The response body must not leak the underlying cause to the browser.
    expect(res.body.errors[0].error).not.toMatch(/Bad credentials/);

    const logged = consoleError.mock.calls.map((call) => call.join(' ')).join('\n');
    expect(logged).toContain('p1');
    expect(logged).toContain('401');
    expect(logged).toContain('Bad credentials');
    // Never log the raw error object — an Octokit error can carry the
    // Authorization header used to authenticate as the GitHub App.
    for (const call of consoleError.mock.calls) {
      expect(call).not.toContain(githubError);
    }

    consoleError.mockRestore();
  });

  it('still calls failPullRequest when the GitHub error carries no status (network failure)', async () => {
    const remediation = makeRemediation();
    const prCreator = makePrCreator({
      create: vi.fn().mockRejectedValue(new Error('socket hang up')),
    });
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    expect(remediation.failPullRequest).toHaveBeenCalledWith({
      id: 'p1',
      claimed_at: '2026-06-24T00:00:00Z',
      service: '',
    });
  });

  // ── POST — opened_by forwarded from authenticated user ────────────────────

  it('forwards the authenticated user id as opened_by in recordPullRequest', async () => {
    const remediation = makeRemediation();
    const prCreator = makePrCreator();
    const getObject = makeGetObject('SELECT 1');
    const app = appWith(
      { remediation, prCreator, getObject },
      { userId: 'u42', role: 'operator', email: 'x@example.com', name: 'Test User' },
    );

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(200);
    expect(remediation.recordPullRequest).toHaveBeenCalledWith(
      expect.objectContaining({ opened_by: 'u42' }),
    );
  });

  // ── POST — recordPullRequest failure → still succeeds (PR already open) ──

  it('still reports the pull request even when recordPullRequest rejects', async () => {
    // The GitHub PR has already been created when recordPullRequest is called.
    // A failure there must not hang the request or return an error — the operator
    // must receive their PR link regardless.
    const remediation = makeRemediation({
      recordPullRequest: vi.fn().mockRejectedValue(new Error('gRPC unavailable')),
    });
    const prCreator = makePrCreator();
    const getObject = makeGetObject('SELECT 1');
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(200);
    expect(res.body.pull_requests).toEqual([
      { service: '', pr_url: 'https://github.com/owner/repo/pull/42', pr_number: 42 },
    ]);
    // The PR was opened — failPullRequest must NOT be called.
    expect(remediation.failPullRequest).not.toHaveBeenCalled();
  });

  // ── POST — S3 fetch failure → failPullRequest + per-service error ────────

  it('fails the claim and reports a per-service error when the proposed SQL fetch from S3 rejects', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const remediation = makeRemediation();
    const prCreator = makePrCreator();
    // First call (proposed SQL) rejects; second call (diff) should never happen.
    const getObject = vi.fn().mockRejectedValue(new Error('S3 NoSuchKey'));
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    expect(res.body.errors).toEqual([{ service: '', error: 'failed to fetch proposed file content from S3' }]);
    expect(remediation.failPullRequest).toHaveBeenCalledWith({ id: 'p1', claimed_at: '2026-06-24T00:00:00Z', service: '' });
    expect(prCreator.create).not.toHaveBeenCalled();
    expect(remediation.recordPullRequest).not.toHaveBeenCalled();
    // The cause must be logged, not just swallowed into a generic response.
    const logged = consoleError.mock.calls.map((call) => call.join(' ')).join('\n');
    expect(logged).toContain('p1');
    expect(logged).toContain('S3 NoSuchKey');

    consoleError.mockRestore();
  });

  // ── POST — failPullRequest echoes the exact claimed_at BeginPullRequest
  //    returned, so the repository CAS releases only this claim ───────────

  // ── POST — version skew: an empty/absent claimed_at must never crash the
  //    process or hang the request ─────────────────────────────────────────

  it('skips failPullRequest and still returns 502 when claimed_at is empty (version skew)', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockResolvedValue({
        proposed_sql_uri: 's3://continuo/proposals/p1/fix.sql',
        diff_uri: '',
        branch: 'remediation/p1',
        file_path: 'models/mymodel.sql',
        repo: 'owner/repo',
        commit_sha: 'abc123',
        claimed_at: '', // proto3 default when the peer predates the field
        edits: [
          {
            path: 'models/mymodel.sql',
            content_uri: 's3://continuo/proposals/p1/fix.sql',
            diff_uri: '',
          },
        ],
      }),
    });
    const prCreator = makePrCreator({
      create: vi.fn().mockRejectedValue(new Error('GitHub API error')),
    });
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    expect(remediation.failPullRequest).not.toHaveBeenCalled();
  });

  it('skips failPullRequest on the S3-fetch failure path too when claimed_at is empty', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockResolvedValue({
        proposed_sql_uri: 's3://continuo/proposals/p1/fix.sql',
        diff_uri: '',
        branch: 'remediation/p1',
        file_path: 'models/mymodel.sql',
        repo: 'owner/repo',
        commit_sha: 'abc123',
        claimed_at: '',
        edits: [
          {
            path: 'models/mymodel.sql',
            content_uri: 's3://continuo/proposals/p1/fix.sql',
            diff_uri: '',
          },
        ],
      }),
    });
    const prCreator = makePrCreator();
    const getObject = vi.fn().mockRejectedValue(new Error('S3 NoSuchKey'));
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    expect(remediation.failPullRequest).not.toHaveBeenCalled();
  });

  it('does not produce an unhandled rejection and still returns 502 when failPullRequest itself rejects', async () => {
    const unhandled = vi.fn();
    process.on('unhandledRejection', unhandled);
    try {
      const remediation = makeRemediation({
        failPullRequest: vi.fn().mockRejectedValue(grpcError(3, 'claimed_at is required')),
      });
      const prCreator = makePrCreator({
        create: vi.fn().mockRejectedValue(new Error('GitHub API error')),
      });
      const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

      const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
      expect(res.status).toBe(502);
      // Give any stray microtask a chance to surface as an unhandled rejection
      // before asserting none did.
      await new Promise((resolve) => setImmediate(resolve));
      expect(unhandled).not.toHaveBeenCalled();
    } finally {
      process.off('unhandledRejection', unhandled);
    }
  });

  it('echoes the claim-specific claimed_at from beginPullRequest into failPullRequest', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockResolvedValue({
        proposed_sql_uri: 's3://continuo/proposals/p1/fix.sql',
        diff_uri: '',
        branch: 'remediation/p1',
        file_path: 'models/mymodel.sql',
        repo: 'owner/repo',
        commit_sha: 'abc123',
        claimed_at: '2026-07-01T12:34:56Z',
        edits: [
          {
            path: 'models/mymodel.sql',
            content_uri: 's3://continuo/proposals/p1/fix.sql',
            diff_uri: '',
          },
        ],
      }),
    });
    const prCreator = makePrCreator({
      create: vi.fn().mockRejectedValue(new Error('GitHub API error')),
    });
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(remediation.failPullRequest).toHaveBeenCalledWith({
      id: 'p1',
      claimed_at: '2026-07-01T12:34:56Z',
      service: '',
    });
  });

  // ── POST — legacy proposal (pr_services: ['']) ────────────────────────────

  it('legacy proposal (pr_services=[""]) opens exactly one PR with no service suffix', async () => {
    const remediation = makeRemediation({
      getProposal: vi.fn().mockResolvedValue({ pr_services: [''] }),
    });
    const prCreator = makePrCreator();
    const getObject = makeGetObject('SELECT 1 -- fixed');
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(200);
    expect(res.body.pull_requests).toEqual([
      { service: '', pr_url: 'https://github.com/owner/repo/pull/42', pr_number: 42 },
    ]);
    expect(res.body.errors).toEqual([]);
    expect(remediation.beginPullRequest).toHaveBeenCalledTimes(1);
    expect(remediation.beginPullRequest).toHaveBeenCalledWith({ id: 'p1', service: '' });

    const call = (prCreator.create as any).mock.calls[0][0];
    expect(call.headBranch).toBe('remediation/p1');
    // No " (service)" suffix at all for the legacy group — exactly today's title shape.
    expect(call.title).toBe('[remediation] fix p1 (release )');
  });

  // ── POST — per-service split across two owning services ──────────────────

  describe('a proposal split across several owning services', () => {
    function multiServiceRemediation() {
      const claimsByService: Record<string, any> = {
        core: {
          repo: 'org/core-repo',
          commit_sha: 'core-sha',
          branch: 'remediation/rel-1/attempt1/core',
          claimed_at: '2026-06-24T00:00:00Z',
          release_id: 'rel-1',
          resolved_node_ids: ['core.schema.a'],
          service: 'core',
          edits: [
            { path: 'models/a.sql', content_uri: 's3://bucket/core/a.sql', diff_uri: 's3://bucket/core/a.diff' },
          ],
        },
        finance: {
          repo: 'org/finance-repo',
          commit_sha: 'finance-sha',
          branch: 'remediation/rel-1/attempt1/finance',
          claimed_at: '2026-06-24T00:00:01Z',
          release_id: 'rel-1',
          resolved_node_ids: ['finance.schema.b'],
          service: 'finance',
          edits: [
            { path: 'models/b.sql', content_uri: 's3://bucket/finance/b.sql', diff_uri: 's3://bucket/finance/b.diff' },
          ],
        },
      };
      return {
        claimsByService,
        remediation: makeRemediation({
          getProposal: vi.fn().mockResolvedValue({ pr_services: ['finance', 'core'] }),
          beginPullRequest: vi.fn().mockImplementation(async ({ service }: { service: string }) => claimsByService[service]),
        }),
      };
    }

    it('claims and opens one PR per service in sorted order, with per-service branch/repo/title', async () => {
      const { remediation } = multiServiceRemediation();
      const prCreator = makePrCreator({
        create: vi.fn()
          .mockResolvedValueOnce({ url: 'https://github.com/org/core-repo/pull/10', number: 10 })
          .mockResolvedValueOnce({ url: 'https://github.com/org/finance-repo/pull/11', number: 11 }),
      });
      const getObject = vi.fn().mockImplementation(async (key: string) => `content of ${key}`);
      const app = appWith({ remediation, prCreator, getObject });

      const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
      expect(res.status).toBe(200);
      expect(res.body.pull_requests).toEqual([
        { service: 'core', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10 },
        { service: 'finance', pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11 },
      ]);
      expect(res.body.errors).toEqual([]);

      // Sorted order: core before finance, even though pr_services arrived as ['finance', 'core'].
      const beginCalls = (remediation.beginPullRequest as any).mock.calls;
      expect(beginCalls[0][0]).toEqual({ id: 'p1', service: 'core' });
      expect(beginCalls[1][0]).toEqual({ id: 'p1', service: 'finance' });

      const createCalls = (prCreator.create as any).mock.calls;
      expect(createCalls[0][0]).toEqual(expect.objectContaining({
        repo: 'org/core-repo',
        headBranch: 'remediation/rel-1/attempt1/core',
        baseSha: 'core-sha',
        files: [{ path: 'models/a.sql', content: 'content of core/a.sql' }],
      }));
      expect(createCalls[0][0].title).toBe('[remediation] fix core.schema.a (release rel-1) (core)');
      expect(createCalls[1][0]).toEqual(expect.objectContaining({
        repo: 'org/finance-repo',
        headBranch: 'remediation/rel-1/attempt1/finance',
        baseSha: 'finance-sha',
      }));
      expect(createCalls[1][0].title).toBe('[remediation] fix finance.schema.b (release rel-1) (finance)');

      expect(remediation.recordPullRequest).toHaveBeenCalledWith(
        expect.objectContaining({ id: 'p1', service: 'core', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10 }),
      );
      expect(remediation.recordPullRequest).toHaveBeenCalledWith(
        expect.objectContaining({ id: 'p1', service: 'finance', pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11 }),
      );
    });

    it('skips a service already open and still opens the other, listing both', async () => {
      const { remediation, claimsByService } = multiServiceRemediation();
      (remediation.beginPullRequest as any).mockImplementation(async ({ service }: { service: string }) => {
        if (service === 'core') {
          throw grpcFailedPrecondition('https://github.com/owner/repo/pull/7');
        }
        return claimsByService.finance;
      });
      const prCreator = makePrCreator({
        create: vi.fn().mockResolvedValue({ url: 'https://github.com/org/finance-repo/pull/11', number: 11 }),
      });
      const getObject = vi.fn().mockImplementation(async (key: string) => `content of ${key}`);
      const app = appWith({ remediation, prCreator, getObject });

      const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
      expect(res.status).toBe(200);
      expect(res.body.pull_requests).toEqual([
        { service: 'core', pr_url: 'https://github.com/owner/repo/pull/7', pr_number: 7 },
        { service: 'finance', pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11 },
      ]);
      expect(res.body.errors).toEqual([]);
      // Only finance actually went through GitHub — core was skipped.
      expect(prCreator.create).toHaveBeenCalledTimes(1);
      expect((prCreator.create as any).mock.calls[0][0].repo).toBe('org/finance-repo');
    });

    it('keeps the successfully created PR and reports 207 when a later service fails mid-loop', async () => {
      const { remediation } = multiServiceRemediation();
      const prCreator = makePrCreator({
        create: vi.fn().mockResolvedValueOnce({ url: 'https://github.com/org/core-repo/pull/10', number: 10 }),
      });
      const getObject = vi.fn().mockImplementation(async (key: string) => {
        if (key.includes('finance')) throw new Error('S3 NoSuchKey');
        return `content of ${key}`;
      });
      const app = appWith({ remediation, prCreator, getObject });

      const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
      expect(res.status).toBe(207);
      expect(res.body.pull_requests).toEqual([
        { service: 'core', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10 },
      ]);
      expect(res.body.errors).toEqual([
        { service: 'finance', error: 'failed to fetch proposed file content from S3' },
      ]);
      // The core PR must not be lost because finance failed afterwards.
      expect(remediation.recordPullRequest).toHaveBeenCalledWith(
        expect.objectContaining({ id: 'p1', service: 'core', pr_url: 'https://github.com/org/core-repo/pull/10' }),
      );
      expect(remediation.failPullRequest).toHaveBeenCalledWith({
        id: 'p1',
        claimed_at: '2026-06-24T00:00:01Z',
        service: 'finance',
      });
    });

    it('returns 502 with no successes when every service fails', async () => {
      const { remediation } = multiServiceRemediation();
      const prCreator = makePrCreator({
        create: vi.fn().mockRejectedValue(new Error('GitHub API error')),
      });
      const getObject = vi.fn().mockImplementation(async (key: string) => `content of ${key}`);
      const app = appWith({ remediation, prCreator, getObject });

      const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
      expect(res.status).toBe(502);
      expect(res.body.pull_requests).toEqual([]);
      expect(res.body.errors).toEqual([
        { service: 'core', error: 'failed to open pull request' },
        { service: 'finance', error: 'failed to open pull request' },
      ]);
    });

    it('reports 207 when one service is refused with a URL-less FAILED_PRECONDITION and the other succeeds', async () => {
      const { remediation, claimsByService } = multiServiceRemediation();
      (remediation.beginPullRequest as any).mockImplementation(async ({ service }: { service: string }) => {
        if (service === 'core') {
          // No URL embedded: a genuine refusal (e.g. ErrNotSourceResolved /
          // ErrNotProposed), not an already-open PR.
          throw grpcFailedPrecondition('proposal is not in status proposed');
        }
        return claimsByService.finance;
      });
      const prCreator = makePrCreator({
        create: vi.fn().mockResolvedValue({ url: 'https://github.com/org/finance-repo/pull/11', number: 11 }),
      });
      const getObject = vi.fn().mockImplementation(async (key: string) => `content of ${key}`);
      const app = appWith({ remediation, prCreator, getObject });

      const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
      expect(res.status).toBe(207);
      // Only the succeeding service lands in pull_requests — no fabricated
      // empty-url entry for the refused one.
      expect(res.body.pull_requests).toEqual([
        { service: 'finance', pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11 },
      ]);
      expect(res.body.errors).toEqual([
        { service: 'core', error: 'proposal is not in status proposed' },
      ]);
      // core never reached GitHub at all.
      expect(prCreator.create).toHaveBeenCalledTimes(1);
      expect((prCreator.create as any).mock.calls[0][0].repo).toBe('org/finance-repo');
    });
  });
});
