import { describe, it, expect, vi, beforeEach } from 'vitest';
import crypto from 'crypto';
import {
  FileChangedOnBaseBranchError,
  makePullRequestCreator,
  resolveGithubAppPullRequestCreator,
} from '../../src/server/github/pull-request-creator';
import type { CreatePRInput } from '../../src/server/github/pull-request-creator';

// Minimal octokit-like fake covering only the methods used
function buildFakeOctokit(opts: {
  defaultBranchSha?: string;
  // The blob sha of each file at each commit: contents[ref][path]. A path a
  // ref does not list answers 404, as GitHub does for a file the commit lacks.
  contents?: Record<string, Record<string, string>>;
  // Thrown by every repos.getContent call instead of answering.
  getContentError?: { status?: number; message?: string };
  prCreate422?: boolean;
  createRefStatus?: number;
  existingPR?: { html_url: string; number: number };
  existingPRs?: Array<{ html_url: string; number: number }>;
  requestReviewersError?: { status?: number; message?: string };
  // Decides each requestReviewers call: throw to refuse it.
  requestReviewersImpl?: (params: { reviewers: string[]; team_reviewers: string[] }) => void;
  updateError?: { status?: number; message?: string };
}) {
  const {
    defaultBranchSha = 'sha-base',
    contents = {},
    getContentError,
    prCreate422 = false,
    createRefStatus = undefined,
    existingPR = { html_url: 'https://github.com/o/r/pull/7', number: 7 },
    existingPRs = [existingPR],
    requestReviewersError,
    requestReviewersImpl,
    updateError,
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

  const getContent = vi.fn().mockImplementation(async (params: { path: string; ref: string }) => {
    if (getContentError) throw getContentError;
    const sha = contents[params.ref]?.[params.path];
    if (sha === undefined) {
      const err = { status: 404, message: 'Not Found' };
      throw err;
    }
    return { data: { type: 'file', path: params.path, sha } };
  });

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

  const requestReviewers = vi.fn().mockImplementation(async (params: { reviewers: string[]; team_reviewers: string[] }) => {
    if (requestReviewersError) throw requestReviewersError;
    requestReviewersImpl?.(params);
    return {};
  });

  const update = vi.fn().mockImplementation(async () => {
    if (updateError) throw updateError;
    return {};
  });

  const getCommit = vi.fn().mockImplementation(async (params: { commit_sha: string }) => ({
    data: { tree: { sha: `tree-of-${params.commit_sha}` } },
  }));
  const createBlob = vi.fn().mockImplementation(async () => ({ data: { sha: `blob-${++blobCounter}` } }));
  const createTree = vi.fn().mockResolvedValue({ data: { sha: 'new-tree-sha' } });
  const createCommit = vi.fn().mockResolvedValue({ data: { sha: 'new-commit-sha' } });
  const updateRef = vi.fn().mockResolvedValue({});

  return {
    git: { getRef, createRef, getCommit, createBlob, createTree, createCommit, updateRef },
    repos: { createOrUpdateFileContents, getContent },
    pulls: { create: prCreate, list: prList, requestReviewers, update },
  };
}

const baseInput: CreatePRInput = {
  repo: 'o/continuo-demo',
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

    it('deletes no path for files edited in place', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: {
          'release-sha': { 'models/orders_d.sql': 'blob-d' },
          'main-head': { 'models/orders_d.sql': 'blob-d' },
        },
      });
      await makePullRequestCreator(octokit).create({ ...baseInput, baseSha: 'release-sha' });

      expect(octokit.repos.getContent).toHaveBeenCalledTimes(2);
      const tree = octokit.git.createTree.mock.calls[0][0].tree;
      expect(tree).toEqual([{ path: 'models/orders_d.sql', mode: '100644', type: 'blob', sha: 'blob-1' }]);
    });
  });

  // The head branch is cut from the base branch's head. baseSha is the commit
  // the fix was made against: every file the fix edits must be the same there
  // as on the base branch's head, or the commit would overwrite a newer version.
  describe('branch point — the base branch head, once the edited files are unchanged since baseSha', () => {
    const twoFiles: CreatePRInput = {
      ...baseInput,
      baseSha: 'release-sha',
      files: [
        { path: 'models/orders_d.sql', content: 'SELECT 1' },
        { path: 'models/orders_e.sql', content: 'SELECT 2' },
      ],
    };

    function expectNothingWritten(octokit: ReturnType<typeof buildFakeOctokit>) {
      expect(octokit.git.createRef).not.toHaveBeenCalled();
      expect(octokit.git.createBlob).not.toHaveBeenCalled();
      expect(octokit.git.createTree).not.toHaveBeenCalled();
      expect(octokit.git.createCommit).not.toHaveBeenCalled();
      expect(octokit.git.updateRef).not.toHaveBeenCalled();
      expect(octokit.pulls.create).not.toHaveBeenCalled();
    }

    it('cuts the branch from the base branch head when every edited file is unchanged since baseSha', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: {
          'release-sha': { 'models/orders_d.sql': 'blob-d', 'models/orders_e.sql': 'blob-e' },
          'main-head': { 'models/orders_d.sql': 'blob-d', 'models/orders_e.sql': 'blob-e' },
        },
      });
      await makePullRequestCreator(octokit).create(twoFiles);

      expect(octokit.git.getRef).toHaveBeenCalledWith({ owner: 'o', repo: 'continuo-demo', ref: 'heads/main' });
      for (const ref of ['release-sha', 'main-head']) {
        for (const path of ['models/orders_d.sql', 'models/orders_e.sql']) {
          expect(octokit.repos.getContent).toHaveBeenCalledWith({ owner: 'o', repo: 'continuo-demo', path, ref });
        }
      }
      expect(octokit.git.createRef).toHaveBeenCalledWith(
        expect.objectContaining({ ref: 'refs/heads/remediation/r-1/orders_d-attempt1', sha: 'main-head' }),
      );
      expect(octokit.git.getCommit).toHaveBeenCalledWith(expect.objectContaining({ commit_sha: 'main-head' }));
      expect(octokit.git.createTree).toHaveBeenCalledWith(
        expect.objectContaining({ base_tree: 'tree-of-main-head' }),
      );
      expect(octokit.git.createCommit).toHaveBeenCalledWith(
        expect.objectContaining({ parents: ['main-head'] }),
      );
    });

    it('refuses with FileChangedOnBaseBranchError naming the changed file, before writing anything', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: {
          'release-sha': { 'models/orders_d.sql': 'blob-d', 'models/orders_e.sql': 'blob-e' },
          'main-head': { 'models/orders_d.sql': 'blob-d', 'models/orders_e.sql': 'blob-e-newer' },
        },
      });
      const err = await makePullRequestCreator(octokit).create(twoFiles).catch((e: unknown) => e);

      expect(err).toBeInstanceOf(FileChangedOnBaseBranchError);
      expect((err as FileChangedOnBaseBranchError).paths).toEqual(['models/orders_e.sql']);
      expect((err as FileChangedOnBaseBranchError).baseBranch).toBe('main');
      expect((err as Error).message).toMatch(/models\/orders_e\.sql/);
      expect((err as Error).message).not.toMatch(/orders_d/);
      expectNothingWritten(octokit);
    });

    it('names every changed file, in the order of the edits', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: {
          'release-sha': { 'models/orders_d.sql': 'blob-d', 'models/orders_e.sql': 'blob-e' },
          'main-head': { 'models/orders_d.sql': 'blob-d-newer', 'models/orders_e.sql': 'blob-e-newer' },
        },
      });
      const err = await makePullRequestCreator(octokit).create(twoFiles).catch((e: unknown) => e);

      expect((err as FileChangedOnBaseBranchError).paths).toEqual(['models/orders_d.sql', 'models/orders_e.sql']);
      expectNothingWritten(octokit);
    });

    it('counts a file absent at both commits as unchanged', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'main-head', contents: {} });
      await makePullRequestCreator(octokit).create({ ...baseInput, baseSha: 'release-sha' });

      expect(octokit.repos.getContent).toHaveBeenCalledTimes(2);
      expect(octokit.git.createRef).toHaveBeenCalledWith(expect.objectContaining({ sha: 'main-head' }));
      expect(octokit.git.createCommit).toHaveBeenCalledWith(expect.objectContaining({ parents: ['main-head'] }));
    });

    it('counts a file present at baseSha and deleted on the base branch as changed', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: { 'release-sha': { 'models/orders_d.sql': 'blob-d' } },
      });
      const err = await makePullRequestCreator(octokit)
        .create({ ...baseInput, baseSha: 'release-sha' })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(FileChangedOnBaseBranchError);
      expect((err as FileChangedOnBaseBranchError).paths).toEqual(['models/orders_d.sql']);
      expectNothingWritten(octokit);
    });

    it('counts a file absent at baseSha and added on the base branch as changed', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: { 'main-head': { 'models/orders_d.sql': 'blob-d' } },
      });
      const err = await makePullRequestCreator(octokit)
        .create({ ...baseInput, baseSha: 'release-sha' })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(FileChangedOnBaseBranchError);
      expect((err as FileChangedOnBaseBranchError).paths).toEqual(['models/orders_d.sql']);
      expectNothingWritten(octokit);
    });

    it('cuts the branch from the base branch head, comparing no file, when baseSha is omitted', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'main-head' });
      await makePullRequestCreator(octokit).create(baseInput);

      expect(octokit.repos.getContent).not.toHaveBeenCalled();
      expect(octokit.git.createRef).toHaveBeenCalledWith(expect.objectContaining({ sha: 'main-head' }));
      expect(octokit.git.createTree).toHaveBeenCalledWith(expect.objectContaining({ base_tree: 'tree-of-main-head' }));
      expect(octokit.git.createCommit).toHaveBeenCalledWith(expect.objectContaining({ parents: ['main-head'] }));
    });

    it('compares no file when baseSha is the base branch head', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'main-head' });
      await makePullRequestCreator(octokit).create({ ...baseInput, baseSha: 'main-head' });

      expect(octokit.repos.getContent).not.toHaveBeenCalled();
      expect(octokit.git.createRef).toHaveBeenCalledWith(expect.objectContaining({ sha: 'main-head' }));
      expect(octokit.git.createCommit).toHaveBeenCalledWith(expect.objectContaining({ parents: ['main-head'] }));
    });

    // GitHub answers 404 for a file read at a commit it does not know, which
    // would otherwise read as every file absent at baseSha.
    it('rethrows when baseSha names no commit, comparing no file and writing nothing', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: { 'main-head': { 'models/orders_d.sql': 'blob-d' } },
      });
      octokit.git.getCommit.mockRejectedValueOnce({ status: 404, message: 'Not Found' });
      const err = await makePullRequestCreator(octokit)
        .create({ ...baseInput, baseSha: 'no-such-commit' })
        .catch((e: unknown) => e);

      expect(octokit.git.getCommit).toHaveBeenCalledWith(expect.objectContaining({ commit_sha: 'no-such-commit' }));
      expect(err).toMatchObject({ status: 404 });
      expect(err).not.toBeInstanceOf(FileChangedOnBaseBranchError);
      expect(octokit.repos.getContent).not.toHaveBeenCalled();
      expectNothingWritten(octokit);
    });

    it('rethrows a file read that fails with another status than 404, before writing anything', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        getContentError: { status: 500, message: 'server error' },
      });
      const err = await makePullRequestCreator(octokit)
        .create({ ...baseInput, baseSha: 'release-sha' })
        .catch((e: unknown) => e);

      expect(err).toMatchObject({ status: 500 });
      expect(err).not.toBeInstanceOf(FileChangedOnBaseBranchError);
      expectNothingWritten(octokit);
    });
  });

  // A renamed file is written at its path and deleted at renamed_from, in the
  // one tree of the commit. Both paths must be as they were at baseSha on the
  // base branch head: the old one the same blob, the new one absent at both or
  // the same blob, so the rename neither deletes a file the base branch has
  // changed nor overwrites one it has added.
  describe('renamed file — written at its path and deleted at renamed_from', () => {
    const oldPath = 'services/marketing/models/table_b.sql';
    const newPath = 'services/marketing/models/table_b_v2.sql';
    const renamed: CreatePRInput = {
      ...baseInput,
      baseSha: 'release-sha',
      files: [{ path: newPath, content: 'SELECT 2', renamed_from: oldPath }],
    };

    function expectNothingWritten(octokit: ReturnType<typeof buildFakeOctokit>) {
      expect(octokit.git.createRef).not.toHaveBeenCalled();
      expect(octokit.git.createBlob).not.toHaveBeenCalled();
      expect(octokit.git.createTree).not.toHaveBeenCalled();
      expect(octokit.git.createCommit).not.toHaveBeenCalled();
      expect(octokit.git.updateRef).not.toHaveBeenCalled();
      expect(octokit.pulls.create).not.toHaveBeenCalled();
    }

    it('writes the new path and deletes the old one in one tree', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: {
          'release-sha': { [oldPath]: 'blob-old' },
          'main-head': { [oldPath]: 'blob-old' },
        },
      });
      await makePullRequestCreator(octokit).create(renamed);

      expect(octokit.git.createBlob).toHaveBeenCalledTimes(1);
      expect(octokit.git.createTree).toHaveBeenCalledTimes(1);
      expect(octokit.git.createTree).toHaveBeenCalledWith(
        expect.objectContaining({
          base_tree: 'tree-of-main-head',
          tree: [
            { path: newPath, mode: '100644', type: 'blob', sha: 'blob-1' },
            { path: oldPath, mode: '100644', type: 'blob', sha: null },
          ],
        }),
      );
      expect(octokit.git.createCommit).toHaveBeenCalledTimes(1);
    });

    it('reads both paths at baseSha and on the base branch head', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: {
          'release-sha': { [oldPath]: 'blob-old' },
          'main-head': { [oldPath]: 'blob-old' },
        },
      });
      await makePullRequestCreator(octokit).create(renamed);

      expect(octokit.repos.getContent).toHaveBeenCalledTimes(4);
      for (const ref of ['release-sha', 'main-head']) {
        for (const path of [oldPath, newPath]) {
          expect(octokit.repos.getContent).toHaveBeenCalledWith({ owner: 'o', repo: 'continuo-demo', path, ref });
        }
      }
    });

    it('refuses, writing nothing, when the old path changed on the base branch', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: {
          'release-sha': { [oldPath]: 'blob-old' },
          'main-head': { [oldPath]: 'blob-old-newer' },
        },
      });
      const err = await makePullRequestCreator(octokit).create(renamed).catch((e: unknown) => e);

      expect(err).toBeInstanceOf(FileChangedOnBaseBranchError);
      expect((err as FileChangedOnBaseBranchError).paths).toEqual([oldPath]);
      expectNothingWritten(octokit);
    });

    it('refuses, writing nothing, when the old path was deleted on the base branch', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: { 'release-sha': { [oldPath]: 'blob-old' } },
      });
      const err = await makePullRequestCreator(octokit).create(renamed).catch((e: unknown) => e);

      expect(err).toBeInstanceOf(FileChangedOnBaseBranchError);
      expect((err as FileChangedOnBaseBranchError).paths).toEqual([oldPath]);
      expectNothingWritten(octokit);
    });

    it('refuses, writing nothing, when the new path exists on the base branch only', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: {
          'release-sha': { [oldPath]: 'blob-old' },
          'main-head': { [oldPath]: 'blob-old', [newPath]: 'blob-added-on-main' },
        },
      });
      const err = await makePullRequestCreator(octokit).create(renamed).catch((e: unknown) => e);

      expect(err).toBeInstanceOf(FileChangedOnBaseBranchError);
      expect((err as FileChangedOnBaseBranchError).paths).toEqual([newPath]);
      expectNothingWritten(octokit);
    });

    it('names the old path before the new one when both changed', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: {
          'release-sha': { [oldPath]: 'blob-old' },
          'main-head': { [oldPath]: 'blob-old-newer', [newPath]: 'blob-added-on-main' },
        },
      });
      const err = await makePullRequestCreator(octokit).create(renamed).catch((e: unknown) => e);

      expect((err as FileChangedOnBaseBranchError).paths).toEqual([oldPath, newPath]);
      expectNothingWritten(octokit);
    });

    it('commits a rename beside an edit in place, deleting only the renamed file', async () => {
      const octokit = buildFakeOctokit({
        defaultBranchSha: 'main-head',
        contents: {
          'release-sha': { [oldPath]: 'blob-old', 'models/orders_d.sql': 'blob-d' },
          'main-head': { [oldPath]: 'blob-old', 'models/orders_d.sql': 'blob-d' },
        },
      });
      await makePullRequestCreator(octokit).create({
        ...renamed,
        files: [
          { path: 'models/orders_d.sql', content: 'SELECT 1' },
          { path: newPath, content: 'SELECT 2', renamed_from: oldPath },
        ],
      });

      expect(octokit.git.createTree).toHaveBeenCalledWith(
        expect.objectContaining({
          tree: [
            { path: 'models/orders_d.sql', mode: '100644', type: 'blob', sha: 'blob-1' },
            { path: newPath, mode: '100644', type: 'blob', sha: 'blob-2' },
            { path: oldPath, mode: '100644', type: 'blob', sha: null },
          ],
        }),
      );
      expect(octokit.repos.getContent).toHaveBeenCalledTimes(6);
    });

    it('deletes the old path when baseSha is omitted, comparing no file', async () => {
      const octokit = buildFakeOctokit({ defaultBranchSha: 'main-head' });
      const { baseSha: _omitted, ...withoutBaseSha } = renamed;
      await makePullRequestCreator(octokit).create(withoutBaseSha);

      expect(octokit.repos.getContent).not.toHaveBeenCalled();
      expect(octokit.git.createTree.mock.calls[0][0].tree).toEqual([
        { path: newPath, mode: '100644', type: 'blob', sha: 'blob-1' },
        { path: oldPath, mode: '100644', type: 'blob', sha: null },
      ]);
    });

    // A tree that both writes and deletes one path says two things about it.
    it('refuses, before touching GitHub, a file renamed from a path the pull request also writes', async () => {
      const octokit = buildFakeOctokit({});
      const sameFile = makePullRequestCreator(octokit).create({
        ...renamed,
        files: [{ path: newPath, content: 'x', renamed_from: newPath }],
      });
      await expect(sameFile).rejects.toThrow(`${newPath} is both written and deleted`);

      const otherFile = makePullRequestCreator(octokit).create({
        ...renamed,
        files: [
          { path: oldPath, content: 'x' },
          { path: newPath, content: 'y', renamed_from: oldPath },
        ],
      });
      await expect(otherFile).rejects.toThrow(`${oldPath} is both written and deleted`);

      expect(octokit.git.getRef).not.toHaveBeenCalled();
      expectNothingWritten(octokit);
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
          base_tree: 'tree-of-sha-base',
          tree: [
            { path: 'contracts/a.yml', mode: '100644', type: 'blob', sha: 'blob-1' },
            { path: 'scripts/a.py', mode: '100644', type: 'blob', sha: 'blob-2' },
          ],
        })
      );
      expect(octokit.git.createCommit).toHaveBeenCalledWith(
        expect.objectContaining({ message: 'm', tree: 'new-tree-sha', parents: ['sha-base'] })
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

  describe('reviewers', () => {
    it('requests reviews after creating the pull request', async () => {
      const octokit = buildFakeOctokit({ existingPR: { html_url: 'https://github.com/o/r/pull/9', number: 9 } });
      const creator = makePullRequestCreator(octokit);
      const res = await creator.create({ ...baseInput, reviewers: ['alice', 'bob'], teamReviewers: ['finance'] });

      expect(octokit.pulls.requestReviewers).toHaveBeenCalledTimes(1);
      expect(octokit.pulls.requestReviewers).toHaveBeenCalledWith({
        owner: 'o',
        repo: 'continuo-demo',
        pull_number: 9,
        reviewers: ['alice', 'bob'],
        team_reviewers: ['finance'],
      });
      expect(octokit.pulls.create.mock.invocationCallOrder[0]).toBeLessThan(
        octokit.pulls.requestReviewers.mock.invocationCallOrder[0],
      );
      expect(res).toEqual({ url: 'https://github.com/o/r/pull/9', number: 9 });
    });

    it('requests the reviews of teams alone, and of users alone', async () => {
      const teamsOnly = buildFakeOctokit({});
      await makePullRequestCreator(teamsOnly).create({ ...baseInput, teamReviewers: ['finance'] });
      expect(teamsOnly.pulls.requestReviewers).toHaveBeenCalledWith(
        expect.objectContaining({ reviewers: [], team_reviewers: ['finance'] }),
      );

      const usersOnly = buildFakeOctokit({});
      await makePullRequestCreator(usersOnly).create({ ...baseInput, reviewers: ['alice'] });
      expect(usersOnly.pulls.requestReviewers).toHaveBeenCalledWith(
        expect.objectContaining({ reviewers: ['alice'], team_reviewers: [] }),
      );
    });

    it('asks for no review when there is nobody to ask', async () => {
      const octokit = buildFakeOctokit({});
      const creator = makePullRequestCreator(octokit);
      await creator.create(baseInput);
      await creator.create({ ...baseInput, reviewers: [], teamReviewers: [] });
      expect(octokit.pulls.requestReviewers).not.toHaveBeenCalled();
    });

    it('opens the pull request when a reviewer request is refused', async () => {
      const octokit = buildFakeOctokit({
        existingPR: { html_url: 'https://github.com/o/r/pull/9', number: 9 },
        requestReviewersError: { status: 422, message: 'Reviews may only be requested from collaborators' },
      });
      const creator = makePullRequestCreator(octokit);
      const res = await creator.create({ ...baseInput, reviewers: ['alice'] });

      expect(res.url).toBe('https://github.com/o/r/pull/9');
      expect(res.number).toBe(9);
      expect(res.warnings).toHaveLength(1);
      expect(res.warnings?.[0]).toMatch(/alice \(status 422\)/);
      expect(res.warnings?.[0]).toMatch(/Reviews may only be requested from collaborators/);
    });

    it('opens the pull request when a reviewer request fails without a status', async () => {
      const octokit = buildFakeOctokit({ requestReviewersError: { message: 'socket hang up' } });
      const res = await makePullRequestCreator(octokit).create({ ...baseInput, teamReviewers: ['finance'] });
      expect(res.number).toBe(7);
      expect(res.warnings?.[0]).toMatch(/team finance: socket hang up/);
    });

    // A refusal of the combined request says nothing about which owner caused
    // it: GitHub validates the whole list, so one user who is not a collaborator
    // refuses the others too. Each owner is asked on its own then.
    describe('when the combined request is refused', () => {
      const refuseBob = (params: { reviewers: string[]; team_reviewers: string[] }) => {
        if (params.reviewers.includes('bob')) {
          throw { status: 422, message: 'Reviews may only be requested from collaborators' };
        }
      };
      const mixed = { ...baseInput, reviewers: ['alice', 'bob'], teamReviewers: ['finance'] };
      const open = { existingPR: { html_url: 'https://github.com/o/r/pull/9', number: 9 } };

      it('asks each user and each team on its own and names only the refused owner', async () => {
        const octokit = buildFakeOctokit({ ...open, requestReviewersImpl: refuseBob });
        const res = await makePullRequestCreator(octokit).create(mixed);

        const calls = octokit.pulls.requestReviewers.mock.calls.map((c: any[]) => [c[0].reviewers, c[0].team_reviewers]);
        expect(calls).toEqual([
          [['alice', 'bob'], ['finance']],
          [['alice'], []],
          [['bob'], []],
          [[], ['finance']],
        ]);
        expect(res.url).toBe('https://github.com/o/r/pull/9');
        expect(res.warnings).toHaveLength(1);
        expect(res.warnings?.[0]).toMatch(/refused the review request for bob \(status 422\)/);
        expect(res.warnings?.[0]).toMatch(/Reviews may only be requested from collaborators/);
        expect(res.warnings?.[0]).not.toMatch(/alice|finance/);
      });

      it('rewrites the body with the body the caller builds for the refused owners', async () => {
        const octokit = buildFakeOctokit({ ...open, requestReviewersImpl: refuseBob });
        const bodyWithRefused = vi.fn().mockReturnValue('body with bob refused');
        await makePullRequestCreator(octokit).create({ ...mixed, bodyWithRefused });

        expect(bodyWithRefused).toHaveBeenCalledWith({ users: ['bob'], teams: [] });
        expect(octokit.pulls.update).toHaveBeenCalledTimes(1);
        expect(octokit.pulls.update).toHaveBeenCalledWith({
          owner: 'o',
          repo: 'continuo-demo',
          pull_number: 9,
          body: 'body with bob refused',
        });
      });

      it('names every refused user and team', async () => {
        const octokit = buildFakeOctokit({
          ...open,
          requestReviewersImpl: () => {
            throw { status: 422, message: 'refused' };
          },
        });
        const bodyWithRefused = vi.fn().mockReturnValue('b');
        const res = await makePullRequestCreator(octokit).create({ ...mixed, bodyWithRefused });

        expect(octokit.pulls.requestReviewers).toHaveBeenCalledTimes(4);
        expect(bodyWithRefused).toHaveBeenCalledWith({ users: ['alice', 'bob'], teams: ['finance'] });
        expect(res.warnings).toHaveLength(1);
        expect(res.warnings?.[0]).toMatch(/alice \(status 422\).*bob \(status 422\).*team finance \(status 422\)/);
      });

      it('says nothing and leaves the body when each owner is taken on its own', async () => {
        let first = true;
        const octokit = buildFakeOctokit({
          ...open,
          requestReviewersImpl: () => {
            if (first) {
              first = false;
              throw { status: 502, message: 'bad gateway' };
            }
          },
        });
        const bodyWithRefused = vi.fn();
        const res = await makePullRequestCreator(octokit).create({ ...mixed, bodyWithRefused });

        expect(octokit.pulls.requestReviewers).toHaveBeenCalledTimes(4);
        expect(res).toEqual({ url: 'https://github.com/o/r/pull/9', number: 9 });
        expect(bodyWithRefused).not.toHaveBeenCalled();
        expect(octokit.pulls.update).not.toHaveBeenCalled();
      });

      it('does not ask a single owner twice', async () => {
        const octokit = buildFakeOctokit({ ...open, requestReviewersImpl: refuseBob });
        const bodyWithRefused = vi.fn().mockReturnValue('b');
        const res = await makePullRequestCreator(octokit).create({ ...baseInput, reviewers: ['bob'], bodyWithRefused });

        expect(octokit.pulls.requestReviewers).toHaveBeenCalledTimes(1);
        expect(bodyWithRefused).toHaveBeenCalledWith({ users: ['bob'], teams: [] });
        expect(octokit.pulls.update).toHaveBeenCalledTimes(1);
        expect(res.warnings?.[0]).toMatch(/refused the review request for bob \(status 422\)/);
      });

      it('opens the pull request and warns once more when the body cannot be rewritten', async () => {
        const octokit = buildFakeOctokit({
          ...open,
          requestReviewersImpl: refuseBob,
          updateError: { status: 403, message: 'Resource not accessible by integration' },
        });
        const res = await makePullRequestCreator(octokit).create({ ...mixed, bodyWithRefused: () => 'b' });

        expect(res.url).toBe('https://github.com/o/r/pull/9');
        expect(res.number).toBe(9);
        expect(res.warnings).toHaveLength(2);
        expect(res.warnings?.[0]).toMatch(/bob/);
        expect(res.warnings?.[1]).toMatch(/body/);
        expect(res.warnings?.[1]).toMatch(/403/);
        expect(res.warnings?.[1]).toMatch(/Resource not accessible by integration/);
      });

      it('leaves the body when the caller gave no way to rewrite it', async () => {
        const octokit = buildFakeOctokit({ ...open, requestReviewersImpl: refuseBob });
        const res = await makePullRequestCreator(octokit).create(mixed);

        expect(octokit.pulls.update).not.toHaveBeenCalled();
        expect(res.warnings).toHaveLength(1);
      });
    });

    it('does not ask again for a pull request that was already open', async () => {
      const octokit = buildFakeOctokit({ prCreate422: true });
      const res = await makePullRequestCreator(octokit).create({ ...baseInput, reviewers: ['alice'] });
      expect(res).toEqual({ url: 'https://github.com/o/r/pull/7', number: 7 });
      expect(octokit.pulls.requestReviewers).not.toHaveBeenCalled();
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
