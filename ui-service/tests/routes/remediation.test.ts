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
    getProposal: vi.fn().mockResolvedValue({}),
    beginPullRequest: vi.fn().mockResolvedValue({
      proposed_sql_uri: 's3://continuo/proposals/p1/fix.sql',
      diff_uri: 's3://continuo/proposals/p1/fix.diff',
      branch: 'remediation/p1',
      file_path: 'models/mymodel.sql',
      repo: 'owner/repo',
      commit_sha: 'abc123',
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

  it('returns 503 when prCreator is undefined, without calling begin', async () => {
    const remediation = makeRemediation();
    const app = appWith({ remediation, prCreator: undefined, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(503);
    expect(res.body.error).toMatch(/not configured/i);
    expect(remediation.beginPullRequest).not.toHaveBeenCalled();
  });

  // ── POST — FAILED_PRECONDITION (already has a PR) → 409 ──────────────────

  it('does not call GitHub when proposal already has a PR', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockRejectedValue(
        grpcFailedPrecondition('https://github.com/owner/repo/pull/7'),
      ),
    });
    const prCreator = makePrCreator();
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(409);
    expect(res.body.pr_url).toBe('https://github.com/owner/repo/pull/7');
    expect(prCreator.create).not.toHaveBeenCalled();
  });

  // ── POST — NotFound → 404 ─────────────────────────────────────────────────

  it('returns 404 when the proposal is not found during begin', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockRejectedValue(grpcNotFound('proposal not found')),
    });
    const prCreator = makePrCreator();
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/missing/pull-request');
    expect(res.status).toBe(404);
    expect(prCreator.create).not.toHaveBeenCalled();
  });

  // ── POST — happy path ─────────────────────────────────────────────────────

  it('happy path: begin → getObject → create → record, returns 200 with pr_url and pr_number', async () => {
    const remediation = makeRemediation();
    const prCreator = makePrCreator();
    const getObject = makeGetObject('SELECT 1 -- fixed');
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(200);
    expect(res.body.pr_url).toBe('https://github.com/owner/repo/pull/42');
    expect(res.body.pr_number).toBe(42);

    expect(remediation.beginPullRequest).toHaveBeenCalledWith({ id: 'p1' });
    // getObject called at least once for the proposed SQL
    expect(getObject).toHaveBeenCalled();
    expect(prCreator.create).toHaveBeenCalledWith(
      expect.objectContaining({
        repo: 'owner/repo',
        headBranch: 'remediation/p1',
        baseBranch: 'main',
        baseSha: 'abc123',
        filePath: 'models/mymodel.sql',
      }),
    );
    expect(remediation.recordPullRequest).toHaveBeenCalledWith(
      expect.objectContaining({
        id: 'p1',
        pr_url: 'https://github.com/owner/repo/pull/42',
        pr_number: 42,
      }),
    );
  });

  it('strips s3:// prefix when fetching the proposed SQL content', async () => {
    const remediation = makeRemediation({
      beginPullRequest: vi.fn().mockResolvedValue({
        proposed_sql_uri: 's3://continuo/proposals/p1/fix.sql',
        diff_uri: '',
        branch: 'remediation/p1',
        file_path: 'models/mymodel.sql',
        repo: 'owner/repo',
      }),
    });
    const prCreator = makePrCreator();
    const getObject = makeGetObject('SELECT 1');
    const app = appWith({ remediation, prCreator, getObject });

    await request(app).post('/api/remediation/proposals/p1/pull-request');
    // Must strip the s3://<bucket>/ prefix before calling getObject
    expect(getObject).toHaveBeenCalledWith(expect.not.stringContaining('s3://'));
  });

  // ── POST — GitHub failure → failPullRequest + 502 ─────────────────────────

  it('calls failPullRequest and returns 502 when prCreator.create throws', async () => {
    const remediation = makeRemediation();
    const prCreator = makePrCreator({
      create: vi.fn().mockRejectedValue(new Error('GitHub API error')),
    });
    const app = appWith({ remediation, prCreator, getObject: makeGetObject() });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    expect(res.body.error).toMatch(/pull request/i);
    expect(remediation.failPullRequest).toHaveBeenCalledWith({ id: 'p1' });
    expect(remediation.recordPullRequest).not.toHaveBeenCalled();
  });

  it('logs the proposal id and the Octokit error status/message when prCreator.create throws', async () => {
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
    expect(res.body.error).not.toMatch(/Bad credentials/);

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
    expect(remediation.failPullRequest).toHaveBeenCalledWith({ id: 'p1' });
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

  // ── POST — recordPullRequest failure → still 200 (PR already open) ──────

  it('returns 200 with pr_url/pr_number even when recordPullRequest rejects', async () => {
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
    expect(res.body.pr_url).toBe('https://github.com/owner/repo/pull/42');
    expect(res.body.pr_number).toBe(42);
    // The PR was opened — failPullRequest must NOT be called.
    expect(remediation.failPullRequest).not.toHaveBeenCalled();
  });

  // ── POST — S3 fetch failure → failPullRequest + 502, no GitHub call ──────

  it('calls failPullRequest and returns 502 when proposed SQL fetch from S3 rejects', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const remediation = makeRemediation();
    const prCreator = makePrCreator();
    // First call (proposed SQL) rejects; second call (diff) should never happen.
    const getObject = vi.fn().mockRejectedValue(new Error('S3 NoSuchKey'));
    const app = appWith({ remediation, prCreator, getObject });

    const res = await request(app).post('/api/remediation/proposals/p1/pull-request');
    expect(res.status).toBe(502);
    expect(res.body.error).toMatch(/S3/i);
    expect(remediation.failPullRequest).toHaveBeenCalledWith({ id: 'p1' });
    expect(prCreator.create).not.toHaveBeenCalled();
    expect(remediation.recordPullRequest).not.toHaveBeenCalled();
    // The cause must be logged, not just swallowed into a generic response.
    const logged = consoleError.mock.calls.map((call) => call.join(' ')).join('\n');
    expect(logged).toContain('p1');
    expect(logged).toContain('S3 NoSuchKey');

    consoleError.mockRestore();
  });
});
