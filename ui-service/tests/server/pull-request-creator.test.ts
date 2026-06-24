import { describe, it, expect, vi, beforeEach } from 'vitest';
import { makePullRequestCreator } from '../../src/server/github/pull-request-creator';
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
