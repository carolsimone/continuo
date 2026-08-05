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
  /** Path of the file to create or update inside the repository */
  filePath: string;
  /** File content (raw string, encoded to base64 before upload) */
  content: string;
  /** Git commit message for the file upsert */
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
  };
  repos: {
    getContent(params: {
      owner: string;
      repo: string;
      path: string;
      ref: string;
    }): Promise<{ data: { sha: string } }>;
    createOrUpdateFileContents(params: {
      owner: string;
      repo: string;
      path: string;
      message: string;
      content: string;
      branch: string;
      sha?: string;
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
 *   3. Resolve the existing file sha on the head branch via repos.getContent — on 404 (new file) omit sha.
 *   4. Upsert the file via repos.createOrUpdateFileContents.
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

      // Step 3: get existing file sha on the head branch (404 = new file)
      let existingFileSha: string | undefined;
      try {
        const contentData = await octokit.repos.getContent({
          owner,
          repo: repoName,
          path: input.filePath,
          ref: input.headBranch,
        });
        existingFileSha = contentData.data.sha;
      } catch (err: unknown) {
        const e = err as { status?: number };
        if (e.status !== 404) throw err;
        // File does not exist yet — no sha needed
      }

      // Step 4: upsert the file on the head branch
      const upsertParams: Parameters<OctokitLike['repos']['createOrUpdateFileContents']>[0] = {
        owner,
        repo: repoName,
        path: input.filePath,
        message: input.commitMessage,
        content: Buffer.from(input.content).toString('base64'),
        branch: input.headBranch,
      };
      if (existingFileSha !== undefined) {
        upsertParams.sha = existingFileSha;
      }
      await octokit.repos.createOrUpdateFileContents(upsertParams);

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
