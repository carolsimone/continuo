import { describe, it, expect, vi, beforeEach } from 'vitest';
import crypto from 'crypto';
import { makePullRequestCreator, resolveGithubAppPullRequestCreator } from '../../src/server/github/pull-request-creator';
import type { CreatePRInput } from '../../src/server/github/pull-request-creator';

// Minimal octokit-like fake covering only the methods used
function buildFakeOctokit(opts: {
  defaultBranchSha?: string;
  fileSha?: string;
  prCreate422?: boolean;
  createRef422?: boolean;
  existingPR?: { html_url: string; number: number };
  getContent404?: boolean;
}) {
  const {
    defaultBranchSha = 'sha-base',
    fileSha = undefined,
    prCreate422 = false,
    createRef422 = false,
    existingPR = { html_url: 'https://github.com/o/r/pull/7', number: 7 },
    getContent404 = false,
  } = opts;

  const getRef = vi.fn().mockResolvedValue({
    data: { object: { sha: defaultBranchSha } },
  });

  const createRef = vi.fn().mockImplementation(async () => {
    if (createRef422) {
      const err = { status: 422, message: 'Reference already exists' };
      throw err;
    }
    return {};
  });

  const getContent = vi.fn().mockImplementation(async () => {
    if (getContent404) {
      const err = { status: 404 };
      throw err;
    }
    return { data: { sha: fileSha } };
  });

  const createOrUpdateFileContents = vi.fn().mockResolvedValue({});

  const prCreate = vi.fn().mockImplementation(async () => {
    if (prCreate422) {
      const err = { status: 422, message: 'PR already exists' };
      throw err;
    }
    return { data: { html_url: existingPR.html_url, number: existingPR.number } };
  });

  const prList = vi.fn().mockResolvedValue({
    data: [existingPR],
  });

  return {
    git: { getRef, createRef },
    repos: { getContent, createOrUpdateFileContents },
    pulls: { create: prCreate, list: prList },
  };
}

const baseInput: CreatePRInput = {
  repo: 'o/continuo-dbt-demo',
  baseBranch: 'main',
  headBranch: 'remediation/r-1/orders_d-attempt1',
  filePath: 'models/orders_d.sql',
  content: 'SELECT 1',
  commitMessage: 'fix: remediation for orders_d',
  title: 'Remediation fix for orders_d',
  body: 'Auto-generated fix proposal.',
};

describe('makePullRequestCreator', () => {
  describe('happy path — creates branch, upserts file with existing sha, opens PR', () => {
    it('calls createRef with refs/heads/<headBranch> and base sha', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'base1', fileSha: 'f1' });
      const creator = makePullRequestCreator(octokit);
      const res = await creator.create(baseInput);

      expect(octokit.git.createRef).toHaveBeenCalledWith(
        expect.objectContaining({
          ref: 'refs/heads/remediation/r-1/orders_d-attempt1',
          sha: 'base1',
        })
      );
      expect(res).toEqual({
        url: 'https://github.com/o/r/pull/7',
        number: 7,
      });
    });

    it('upserts file with base64-encoded content and existing file sha', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'base1', fileSha: 'f1' });
      const creator = makePullRequestCreator(octokit);
      await creator.create(baseInput);

      expect(octokit.repos.createOrUpdateFileContents).toHaveBeenCalledWith(
        expect.objectContaining({
          path: 'models/orders_d.sql',
          content: Buffer.from('SELECT 1').toString('base64'),
          branch: 'remediation/r-1/orders_d-attempt1',
          sha: 'f1',
        })
      );
    });
  });

  describe('baseSha provided — branches from the proposal commit, not base HEAD', () => {
    it('creates the branch from baseSha and does not resolve base branch HEAD', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'base1', fileSha: 'f1' });
      const creator = makePullRequestCreator(octokit);
      await creator.create({ ...baseInput, baseSha: 'commit-abc' });

      // Branch is cut from the proposal's commit, not the (different) base HEAD.
      expect(octokit.git.createRef).toHaveBeenCalledWith(
        expect.objectContaining({
          ref: 'refs/heads/remediation/r-1/orders_d-attempt1',
          sha: 'commit-abc',
        })
      );
      // No need to look up the base branch HEAD when the commit is known.
      expect(octokit.git.getRef).not.toHaveBeenCalled();
    });
  });

  describe('new file — getContent returns 404, no sha passed to createOrUpdateFileContents', () => {
    it('calls createOrUpdateFileContents WITHOUT sha when file does not exist', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'base1', getContent404: true });
      const creator = makePullRequestCreator(octokit);
      await creator.create(baseInput);

      const call = octokit.repos.createOrUpdateFileContents.mock.calls[0][0];
      expect(call).not.toHaveProperty('sha');
      expect(call.content).toBe(Buffer.from('SELECT 1').toString('base64'));
    });
  });

  describe('PR already exists — pulls.create 422 → returns existing PR from pulls.list', () => {
    it('returns the existing PR url and number', async () => {
      const octokit = buildFakeOctokit({
        prCreate422: true,
        existingPR: { html_url: 'https://github.com/o/r/pull/7', number: 7 },
      });
      const creator = makePullRequestCreator(octokit);
      const res = await creator.create(baseInput);

      expect(octokit.pulls.list).toHaveBeenCalledWith(
        expect.objectContaining({
          state: 'open',
          head: 'o:remediation/r-1/orders_d-attempt1',
        })
      );
      expect(res).toEqual({ url: 'https://github.com/o/r/pull/7', number: 7 });
    });
  });

  describe('head branch already exists — createRef 422 → continues without error', () => {
    it('proceeds to upsert file and open PR when branch already exists', async () => {
      const octokit = buildFakeOctokit({ createRef422: true, fileSha: 'f2' });
      const creator = makePullRequestCreator(octokit);
      const res = await creator.create(baseInput);

      expect(octokit.repos.createOrUpdateFileContents).toHaveBeenCalled();
      expect(res).toEqual({
        url: 'https://github.com/o/r/pull/7',
        number: 7,
      });
    });
  });
});

describe('resolveGithubAppPullRequestCreator', () => {
  function validPrivateKey(): string {
    const { privateKey } = crypto.generateKeyPairSync('rsa', {
      modulusLength: 2048,
      privateKeyEncoding: { type: 'pkcs1', format: 'pem' },
      publicKeyEncoding: { type: 'spki', format: 'pem' },
    });
    return privateKey;
  }

  const baseCfg = { appId: '12345', installationId: '67890' };

  it('returns a PullRequestCreator for a valid key and does not log', () => {
    const log = vi.fn();
    const creator = resolveGithubAppPullRequestCreator(
      { ...baseCfg, privateKey: validPrivateKey() },
      log,
    );
    expect(creator).toBeDefined();
    expect(creator?.create).toBeInstanceOf(Function);
    expect(log).not.toHaveBeenCalled();
  });

  it('returns undefined and logs an actionable error for a malformed key (e.g. space-folded newlines)', () => {
    const log = vi.fn();
    const spaceFolded = validPrivateKey().replace(/\n/g, ' ').trim();
    const creator = resolveGithubAppPullRequestCreator(
      { ...baseCfg, privateKey: spaceFolded },
      log,
    );
    expect(creator).toBeUndefined();
    expect(log).toHaveBeenCalledTimes(1);
    expect(log.mock.calls[0][0]).toMatch(/does not parse into a key that can sign/i);
    expect(log.mock.calls[0][0]).toMatch(/PR creation is disabled/i);
  });

  it('returns undefined without logging when credentials are entirely absent', () => {
    const log = vi.fn();
    const creator = resolveGithubAppPullRequestCreator(
      { appId: '', privateKey: '', installationId: '' },
      log,
    );
    expect(creator).toBeUndefined();
    expect(log).not.toHaveBeenCalled();
  });

  it('returns undefined without logging when only the private key is absent (partial config)', () => {
    const log = vi.fn();
    const creator = resolveGithubAppPullRequestCreator(
      { ...baseCfg, privateKey: '' },
      log,
    );
    expect(creator).toBeUndefined();
    expect(log).not.toHaveBeenCalled();
  });
});
