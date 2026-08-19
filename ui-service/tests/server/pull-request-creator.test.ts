import { describe, it, expect, vi, beforeEach } from 'vitest';
import crypto from 'crypto';
import { makePullRequestCreator, resolveGithubAppPullRequestCreator } from '../../src/server/github/pull-request-creator';
import type { CreatePRInput } from '../../src/server/github/pull-request-creator';

// Minimal octokit-like fake covering only the methods used
function buildFakeOctokit(opts: {
  defaultBranchSha?: string;
  prCreate422?: boolean;
  createRefStatus?: number;
  existingPR?: { html_url: string; number: number };
  existingPRs?: Array<{ html_url: string; number: number }>;
}) {
  const {
    defaultBranchSha = 'sha-base',
    prCreate422 = false,
    createRefStatus = undefined,
    existingPR = { html_url: 'https://github.com/o/r/pull/7', number: 7 },
    existingPRs = [existingPR],
  } = opts;

  const getRef = vi.fn().mockResolvedValue({
    data: { object: { sha: defaultBranchSha } },
  });

  const createRef = vi.fn().mockImplementation(async () => {
    if (createRefStatus !== undefined) {
      const err = { status: createRefStatus, message: 'createRef rejected' };
      throw err;
    }
    return {};
  });

  // The tree flow is content-addressed and never upserts a file directly.
  // Kept only so tests can assert it stays untouched.
  const createOrUpdateFileContents = vi.fn().mockResolvedValue({});

  let blobCounter = 0;

  const prCreate = vi.fn().mockImplementation(async () => {
    if (prCreate422) {
      const err = { status: 422, message: 'PR already exists' };
      throw err;
    }
    return { data: { html_url: existingPR.html_url, number: existingPR.number } };
  });

  const prList = vi.fn().mockResolvedValue({
    data: existingPRs,
  });

  const getCommit = vi.fn().mockResolvedValue({ data: { tree: { sha: 'base-tree-sha' } } });
  const createBlob = vi.fn().mockImplementation(async () => ({ data: { sha: `blob-${++blobCounter}` } }));
  const createTree = vi.fn().mockResolvedValue({ data: { sha: 'new-tree-sha' } });
  const createCommit = vi.fn().mockResolvedValue({ data: { sha: 'new-commit-sha' } });
  const updateRef = vi.fn().mockResolvedValue({});

  return {
    git: { getRef, createRef, getCommit, createBlob, createTree, createCommit, updateRef },
    repos: { createOrUpdateFileContents },
    pulls: { create: prCreate, list: prList },
  };
}

const baseInput: CreatePRInput = {
  repo: 'o/continuo-dbt-demo',
  baseBranch: 'main',
  headBranch: 'remediation/r-1/orders_d-attempt1',
  files: [{ path: 'models/orders_d.sql', content: 'SELECT 1' }],
  commitMessage: 'fix: remediation for orders_d',
  title: 'Remediation fix for orders_d',
  body: 'Auto-generated fix proposal.',
};

describe('makePullRequestCreator', () => {
  describe('happy path — creates branch, commits the files, opens PR', () => {
    it('calls createRef with refs/heads/<headBranch> and base sha', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'base1' });
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

    it('commits a single file through the same tree flow, with no prior read of the file', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'base1' });
      const creator = makePullRequestCreator(octokit);
      await creator.create(baseInput);

      expect(octokit.git.createBlob).toHaveBeenCalledWith(
        expect.objectContaining({
          content: Buffer.from('SELECT 1', 'utf8').toString('base64'),
          encoding: 'base64',
        })
      );
      expect(octokit.git.createTree).toHaveBeenCalledWith(
        expect.objectContaining({
          tree: [
            { path: 'models/orders_d.sql', mode: '100644', type: 'blob', sha: 'blob-1' },
          ],
        })
      );
      expect(octokit.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
    });
  });

  describe('baseSha provided — branches from the proposal commit, not base HEAD', () => {
    it('creates the branch from baseSha and does not resolve base branch HEAD', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'base1' });
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

    it('throws when pulls.create returns 422 but pulls.list finds no open PR', async () => {
      const octokit = buildFakeOctokit({ prCreate422: true, existingPRs: [] });
      const creator = makePullRequestCreator(octokit);

      await expect(creator.create(baseInput)).rejects.toThrow(
        /422 but no open PR found for head o:remediation\/r-1\/orders_d-attempt1/
      );
    });
  });

  describe('non-422 failures propagate to the caller', () => {
    it('rethrows a createRef error whose status is not 422', async () => {
      const octokit = buildFakeOctokit({ createRefStatus: 500 });
      const creator = makePullRequestCreator(octokit);

      await expect(creator.create(baseInput)).rejects.toMatchObject({ status: 500 });
      expect(octokit.git.createBlob).not.toHaveBeenCalled();
    });

    it('rethrows a pulls.create error whose status is not 422', async () => {
      const octokit = buildFakeOctokit({});
      octokit.pulls.create.mockRejectedValue({ status: 500, message: 'server error' });
      const creator = makePullRequestCreator(octokit);

      await expect(creator.create(baseInput)).rejects.toMatchObject({ status: 500 });
      expect(octokit.pulls.list).not.toHaveBeenCalled();
    });
  });

  describe('multi-file commit — one tree commit carrying every file', () => {
    const multiFileInput: CreatePRInput = {
      repo: 'o/r',
      baseBranch: 'main',
      baseSha: 'base',
      headBranch: 'b',
      files: [
        { path: 'contracts/a.yml', content: 'x' },
        { path: 'scripts/a.py', content: 'y' },
      ],
      commitMessage: 'm',
      title: 't',
      body: 'b',
    };

    it('commits N files in one tree commit', async () => {
      const octokit = buildFakeOctokit({});
      const create = makePullRequestCreator(octokit);
      await create.create(multiFileInput);

      expect(octokit.git.createBlob).toHaveBeenCalledTimes(2);
      expect(octokit.git.createTree).toHaveBeenCalledTimes(1);
      expect(
        octokit.git.createTree.mock.calls[0][0].tree.map((e: any) => e.path)
      ).toEqual(['contracts/a.yml', 'scripts/a.py']);
      expect(octokit.git.createCommit).toHaveBeenCalledTimes(1);
      expect(octokit.git.updateRef).toHaveBeenCalledWith(
        expect.objectContaining({ ref: 'heads/b', force: true })
      );
      expect(octokit.repos.createOrUpdateFileContents).not.toHaveBeenCalled();
    });

    it('base64-encodes each file into its own blob and wires the blob shas into the tree', async () => {
      const octokit = buildFakeOctokit({});
      const create = makePullRequestCreator(octokit);
      await create.create(multiFileInput);

      expect(octokit.git.createBlob).toHaveBeenNthCalledWith(
        1,
        expect.objectContaining({
          content: Buffer.from('x', 'utf8').toString('base64'),
          encoding: 'base64',
        })
      );
      expect(octokit.git.createBlob).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          content: Buffer.from('y', 'utf8').toString('base64'),
          encoding: 'base64',
        })
      );
      expect(octokit.git.createTree).toHaveBeenCalledWith(
        expect.objectContaining({
          base_tree: 'base-tree-sha',
          tree: [
            { path: 'contracts/a.yml', mode: '100644', type: 'blob', sha: 'blob-1' },
            { path: 'scripts/a.py', mode: '100644', type: 'blob', sha: 'blob-2' },
          ],
        })
      );
      expect(octokit.git.createCommit).toHaveBeenCalledWith(
        expect.objectContaining({ message: 'm', tree: 'new-tree-sha', parents: ['base'] })
      );
      expect(octokit.git.updateRef).toHaveBeenCalledWith(
        expect.objectContaining({ ref: 'heads/b', sha: 'new-commit-sha', force: true })
      );
    });

    it('reads the base tree from, and parents the commit on, the resolved base sha when baseSha is omitted', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'head-of-main' });
      const create = makePullRequestCreator(octokit);
      const { baseSha: _omitted, ...withoutBaseSha } = multiFileInput;
      await create.create(withoutBaseSha);

      expect(octokit.git.getCommit).toHaveBeenCalledWith(
        expect.objectContaining({ commit_sha: 'head-of-main' })
      );
      expect(octokit.git.createCommit).toHaveBeenCalledWith(
        expect.objectContaining({ parents: ['head-of-main'] })
      );
    });
  });

  describe('no files to commit', () => {
    it('rejects naming the empty file list, before touching GitHub', async () => {
      const octokit = buildFakeOctokit({});
      const creator = makePullRequestCreator(octokit);

      await expect(creator.create({ ...baseInput, files: [] })).rejects.toThrow(/no files to commit/);
      expect(octokit.git.createRef).not.toHaveBeenCalled();
      expect(octokit.pulls.create).not.toHaveBeenCalled();
    });
  });

  describe('head branch already exists — createRef 422 → continues without error', () => {
    it('proceeds to commit the files and open the PR when the branch already exists', async () => {
      const octokit = buildFakeOctokit({ createRefStatus: 422 });
      const creator = makePullRequestCreator(octokit);
      const res = await creator.create(baseInput);

      expect(octokit.git.createCommit).toHaveBeenCalled();
      expect(octokit.git.updateRef).toHaveBeenCalled();
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
