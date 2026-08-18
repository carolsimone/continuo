import { Octokit } from '@octokit/rest';
import { createAppAuth } from '@octokit/auth-app';
import { privateKeyCanSign } from './private-key';

export interface CreatePRInput {
  /** GitHub repository in "owner/name" format */
  repo: string;
  /** Branch to merge into */
  baseBranch: string;
  /**
   * Commit the head branch is cut from. When set (the proposal's commit_sha),
   * the branch is created from this exact commit so the PR diff is precisely the
   * proposed change and GitHub surfaces a conflict if the file drifted on the
   * base branch since the proposal was generated. When omitted, the base branch
   * HEAD is used.
   */
  baseSha?: string;
  /** Branch to create for the PR */
  headBranch: string;
  /**
   * Every file the pull request changes, as repository-relative path plus the
   * full replacement content. All of them land in a single commit.
   */
  files: Array<{ path: string; content: string }>;
  /** Git commit message for the commit carrying the files */
  commitMessage: string;
  /** PR title */
  title: string;
  /** PR body / description */
  body: string;
}

export interface PullRequestCreator {
  create(input: CreatePRInput): Promise<{ url: string; number: number }>;
}

/**
 * Minimal structural type covering only the Octokit methods used by makePullRequestCreator.
 * Allows tests to inject a hand-written fake without depending on real Octokit types.
 */
export interface OctokitLike {
  git: {
    getRef(params: { owner: string; repo: string; ref: string }): Promise<{
      data: { object: { sha: string } };
    }>;
    createRef(params: { owner: string; repo: string; ref: string; sha: string }): Promise<unknown>;
    getCommit(params: { owner: string; repo: string; commit_sha: string }): Promise<{
      data: { tree: { sha: string } };
    }>;
    createBlob(params: {
      owner: string;
      repo: string;
      content: string;
      encoding: string;
    }): Promise<{ data: { sha: string } }>;
    createTree(params: {
      owner: string;
      repo: string;
      base_tree: string;
      tree: Array<{ path: string; mode: '100644'; type: 'blob'; sha: string }>;
    }): Promise<{ data: { sha: string } }>;
    createCommit(params: {
      owner: string;
      repo: string;
      message: string;
      tree: string;
      parents: string[];
    }): Promise<{ data: { sha: string } }>;
    updateRef(params: {
      owner: string;
      repo: string;
      ref: string;
      sha: string;
      force: boolean;
    }): Promise<unknown>;
  };
  pulls: {
    create(params: {
      owner: string;
      repo: string;
      base: string;
      head: string;
      title: string;
      body: string;
    }): Promise<{ data: { html_url: string; number: number } }>;
    list(params: {
      owner: string;
      repo: string;
      head: string;
      state: 'open' | 'closed' | 'all';
    }): Promise<{ data: Array<{ html_url: string; number: number }> }>;
  };
}

/**
 * Returns a PullRequestCreator backed by the provided octokit-compatible client.
 *
 * Sequence for each `create` call:
 *   1. Resolve the commit to branch from — input.baseSha when set (the proposal's
 *      commit), otherwise the base branch HEAD via git.getRef.
 *   2. Create the head branch via git.createRef — on 422 (already exists) continue.
 *   3. Write every input file as a blob, assemble them into one tree layered on
 *      the base commit's tree, and commit that tree with the base commit as parent.
 *   4. Point the head branch at the new commit via git.updateRef.
 *   5. Open the PR via pulls.create — on 422 (PR already exists) resolve the existing PR via pulls.list.
 */
export function makePullRequestCreator(octokit: OctokitLike): PullRequestCreator {
  return {
    async create(input: CreatePRInput): Promise<{ url: string; number: number }> {
      const [owner, repoName] = input.repo.split('/');

      // Step 1: resolve the commit to branch from — the proposal's commit when
      // provided, otherwise the base branch HEAD.
      let baseSha = input.baseSha;
      if (!baseSha) {
        const refData = await octokit.git.getRef({
          owner,
          repo: repoName,
          ref: `heads/${input.baseBranch}`,
        });
        baseSha = refData.data.object.sha;
      }

      // Step 2: create the head branch — ignore 422 (branch already exists)
      try {
        await octokit.git.createRef({
          owner,
          repo: repoName,
          ref: `refs/heads/${input.headBranch}`,
          sha: baseSha,
        });
      } catch (err: unknown) {
        const e = err as { status?: number };
        if (e.status !== 422) throw err;
        // Branch already exists — continue
      }

      // Step 3: commit every file at once. Each file becomes a blob; the blobs
      // are layered onto the base commit's tree so untouched paths are carried
      // over unchanged, and the resulting tree is committed in one go. Writing
      // the files individually would produce one commit per file, and a diff
      // that is only correct once the last of them lands.
      const baseCommit = await octokit.git.getCommit({
        owner,
        repo: repoName,
        commit_sha: baseSha,
      });

      const blobShas: string[] = [];
      for (const file of input.files) {
        const blob = await octokit.git.createBlob({
          owner,
          repo: repoName,
          content: Buffer.from(file.content, 'utf8').toString('base64'),
          encoding: 'base64',
        });
        blobShas.push(blob.data.sha);
      }

      const tree = await octokit.git.createTree({
        owner,
        repo: repoName,
        base_tree: baseCommit.data.tree.sha,
        tree: input.files.map((file, i) => ({
          path: file.path,
          mode: '100644' as const,
          type: 'blob' as const,
          sha: blobShas[i],
        })),
      });

      const commit = await octokit.git.createCommit({
        owner,
        repo: repoName,
        message: input.commitMessage,
        tree: tree.data.sha,
        parents: [baseSha],
      });

      // Step 4: move the head branch onto the new commit. force is safe because
      // the branch name is scoped to this proposal attempt: the only tip it can
      // overwrite is one an earlier attempt at the same proposal left behind.
      await octokit.git.updateRef({
        owner,
        repo: repoName,
        ref: `heads/${input.headBranch}`,
        sha: commit.data.sha,
        force: true,
      });

      // Step 5: open the PR — on 422 (already exists) fetch and return the existing one
      try {
        const prData = await octokit.pulls.create({
          owner,
          repo: repoName,
          base: input.baseBranch,
          head: input.headBranch,
          title: input.title,
          body: input.body,
        });
        return { url: prData.data.html_url, number: prData.data.number };
      } catch (err: unknown) {
        const e = err as { status?: number };
        if (e.status !== 422) throw err;

        // A PR for this head branch already exists — retrieve and return it
        const listData = await octokit.pulls.list({
          owner,
          repo: repoName,
          head: `${owner}:${input.headBranch}`,
          state: 'open',
        });
        const existing = listData.data[0];
        if (!existing) {
          throw new Error(
            `pulls.create returned 422 but no open PR found for head ${owner}:${input.headBranch}`
          );
        }
        return { url: existing.html_url, number: existing.number };
      }
    },
  };
}

/**
 * Creates a PullRequestCreator authenticated as a GitHub App installation.
 * Builds a real Octokit client with App authentication and delegates to makePullRequestCreator.
 * Pass baseUrl to override the GitHub API endpoint (useful for e2e stubs).
 */
export function createGithubAppPullRequestCreator(cfg: {
  appId: string;
  privateKey: string;
  installationId: string;
  baseUrl?: string;
}): PullRequestCreator {
  const octokit = new Octokit({
    ...(cfg.baseUrl ? { baseUrl: cfg.baseUrl } : {}),
    authStrategy: createAppAuth,
    auth: {
      appId: cfg.appId,
      privateKey: cfg.privateKey,
      installationId: cfg.installationId,
    },
  });
  return makePullRequestCreator(octokit as unknown as OctokitLike);
}

/**
 * Resolves the optional GitHub App PullRequestCreator from configuration.
 *
 * Returns undefined ("feature disabled") both when credentials are absent and
 * when the private key fails a startup signing check — but these two cases
 * are not equivalent, and only the second one calls `log`. GitHub App
 * credentials are unconfigured by design in some deployments, so absence on
 * its own is not an error. A key that fails to sign despite an app ID and
 * installation ID being present means someone tried to configure this
 * integration and the key material didn't survive the trip intact, which is
 * worth surfacing loudly rather than behaving identically to "not set up".
 */
export function resolveGithubAppPullRequestCreator(
  cfg: { appId: string; privateKey: string; installationId: string; baseUrl?: string },
  log: (message: string) => void = console.error,
): PullRequestCreator | undefined {
  if (!cfg.appId || !cfg.privateKey || !cfg.installationId) {
    return undefined;
  }
  if (!privateKeyCanSign(cfg.privateKey)) {
    log(
      'GITHUB_APP_PRIVATE_KEY does not parse into a key that can sign — PR creation is disabled. ' +
        'The app ID and installation ID are configured, so this is a malformed key, not an absent ' +
        'integration. A common cause is a quoted (non-block) scalar in the Helm values file, which ' +
        'folds every newline in the PEM into a space without any YAML parse error. Re-issue or ' +
        're-encode the key with real line breaks preserved (a literal "|" block scalar) and restart.',
    );
    return undefined;
  }
  return createGithubAppPullRequestCreator(cfg);
}
