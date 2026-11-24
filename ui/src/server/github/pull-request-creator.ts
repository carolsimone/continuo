import { Octokit } from '@octokit/rest';
import { createAppAuth } from '@octokit/auth-app';
import { privateKeyCanSign } from './private-key';

export interface CreatePRInput {
  /** GitHub repository in "owner/name" format */
  repo: string;
  /** Branch to merge into */
  baseBranch: string;
  /**
   * The commit the files were written against (the proposal's commit_sha).
   * The head branch is always cut from the base branch's head; when this is
   * set and is not that head, every file in `files` must be the same at both
   * commits (the same blob, or absent from both), or create throws
   * FileChangedOnBaseBranchError and writes nothing, since the commit would
   * replace the base branch's newer version of the file. When omitted, no
   * file is compared.
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
  /** GitHub logins of the users asked to review the new pull request. */
  reviewers?: string[];
  /** Slugs of the teams of the repository's organization asked to review it. */
  teamReviewers?: string[];
  /**
   * The body the pull request has once GitHub has refused to ask some of the
   * reviewers above, given the users and team slugs it refused. When GitHub
   * refuses any, the creator replaces the body it opened the pull request with
   * by this one, so the body does not say a review was requested that was not.
   * Without it the body is left as it was opened.
   */
  bodyWithRefused?: (refused: RefusedReviewers) => string;
}

/** The users (logins) and teams (slugs) GitHub would not take as reviewers. */
export interface RefusedReviewers {
  users: string[];
  teams: string[];
}

export interface CreatedPullRequest {
  url: string;
  number: number;
  /**
   * What went wrong after the pull request opened: the owners GitHub refused as
   * reviewers (with its status and message for each), and a failure to update
   * the body to say so. Absent when every owner was asked, when nobody was to
   * be asked, and for a pull request that was already open.
   */
  warnings?: string[];
}

export interface PullRequestCreator {
  create(input: CreatePRInput): Promise<CreatedPullRequest>;
}

/**
 * Thrown by create, before it writes anything to the repository, when files
 * the pull request would change differ between the commit they were written
 * against (CreatePRInput.baseSha) and the base branch's head: committing them
 * on the base branch's head would replace its newer version of each.
 */
export class FileChangedOnBaseBranchError extends Error {
  /** The branch the pull request would merge into. */
  readonly baseBranch: string;
  /** The changed files, as repository-relative paths in the order of CreatePRInput.files. */
  readonly paths: string[];

  constructor(baseBranch: string, paths: string[], baseSha: string, baseBranchHead: string) {
    super(`${paths.join(', ')} changed on ${baseBranch} between ${baseSha} and ${baseBranchHead}`);
    this.name = 'FileChangedOnBaseBranchError';
    this.baseBranch = baseBranch;
    this.paths = paths;
  }
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
  repos: {
    // A file answers one object carrying its blob sha; a directory answers a
    // list of its entries. A path the commit does not hold is a 404.
    getContent(params: { owner: string; repo: string; path: string; ref: string }): Promise<{
      data: { sha: string } | unknown[];
    }>;
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
    requestReviewers(params: {
      owner: string;
      repo: string;
      pull_number: number;
      reviewers: string[];
      team_reviewers: string[];
    }): Promise<unknown>;
    update(params: { owner: string; repo: string; pull_number: number; body: string }): Promise<unknown>;
  };
}

// refusalText says what GitHub answered to a call: its status and the message
// of its answer, which is all an Octokit error is read for here (the error
// object itself can carry the outgoing request and its credentials).
function refusalText(err: unknown): string {
  const e = (err ?? {}) as { status?: number; message?: string };
  const status = e.status ? ` (status ${e.status})` : '';
  const message = (e.message ?? 'no message').replace(/\s+/g, ' ').trim().slice(0, 300);
  return `${status}: ${message}`;
}

// requestReviews asks the users and teams to review a pull request. GitHub
// validates the list as a whole, so one user who is not a collaborator, or one
// team without access to the repository, refuses the others with it. When the
// combined request is refused and there is more than one owner, each user and
// each team is asked on its own, so every owner GitHub does take is asked and
// only the ones it refuses are reported.
async function requestReviews(
  octokit: OctokitLike,
  pr: { owner: string; repo: string; number: number },
  users: string[],
  teams: string[],
): Promise<{ refused: RefusedReviewers; refusals: string[] }> {
  const ask = async (reviewers: string[], team_reviewers: string[]): Promise<unknown> => {
    try {
      await octokit.pulls.requestReviewers({
        owner: pr.owner,
        repo: pr.repo,
        pull_number: pr.number,
        reviewers,
        team_reviewers,
      });
      return undefined;
    } catch (err: unknown) {
      return err ?? new Error('request failed');
    }
  };

  const owners = [
    ...users.map((name) => ({ kind: 'user' as const, name, label: name })),
    ...teams.map((name) => ({ kind: 'team' as const, name, label: `team ${name}` })),
  ];
  const refused: RefusedReviewers = { users: [], teams: [] };
  const refusals: string[] = [];
  const refuse = (owner: (typeof owners)[number], err: unknown) => {
    (owner.kind === 'user' ? refused.users : refused.teams).push(owner.name);
    refusals.push(`${owner.label}${refusalText(err)}`);
  };

  const combined = await ask(users, teams);
  if (combined === undefined) return { refused, refusals };
  // A single owner's refusal is the combined request's: asking again changes nothing.
  if (owners.length === 1) {
    refuse(owners[0], combined);
    return { refused, refusals };
  }
  for (const owner of owners) {
    const err = await ask(owner.kind === 'user' ? [owner.name] : [], owner.kind === 'team' ? [owner.name] : []);
    if (err !== undefined) refuse(owner, err);
  }
  return { refused, refusals };
}

// blobAt names what commit `ref` holds at `path`: the blob sha of a file,
// 'directory' for a directory, and null when the commit has no such path
// (GitHub answers 404). Any other failure is thrown.
async function blobAt(
  octokit: OctokitLike,
  owner: string,
  repo: string,
  path: string,
  ref: string,
): Promise<string | null> {
  try {
    const res = await octokit.repos.getContent({ owner, repo, path, ref });
    return Array.isArray(res.data) ? 'directory' : res.data.sha;
  } catch (err: unknown) {
    if ((err as { status?: number } | undefined)?.status === 404) return null;
    throw err;
  }
}

// changedPaths lists, in the order given, the paths whose content differs
// between commits `from` and `to`: another blob at each, or a file at one and
// none at the other. A path neither commit holds is unchanged.
async function changedPaths(
  octokit: OctokitLike,
  owner: string,
  repo: string,
  paths: string[],
  from: string,
  to: string,
): Promise<string[]> {
  const changed = await Promise.all(
    paths.map(async (path) => {
      const [before, after] = await Promise.all([
        blobAt(octokit, owner, repo, path, from),
        blobAt(octokit, owner, repo, path, to),
      ]);
      return before === after ? undefined : path;
    }),
  );
  return changed.filter((path): path is string => path !== undefined);
}

/**
 * Returns a PullRequestCreator backed by the provided octokit-compatible client.
 *
 * Sequence for each `create` call:
 *   1. Resolve the base branch head via git.getRef. When input.baseSha is set
 *      and is another commit, check that it exists (git.getCommit) and compare
 *      every input file at both commits (repos.getContent): when any differs,
 *      throw FileChangedOnBaseBranchError before anything is written.
 *   2. Create the head branch at the base branch head via git.createRef — on
 *      422 (already exists) continue.
 *   3. Write every input file as a blob, assemble them into one tree layered on
 *      the base branch head's tree, and commit that tree with the base branch
 *      head as parent.
 *   4. Point the head branch at the new commit via git.updateRef.
 *   5. Open the PR via pulls.create — on 422 (PR already exists) resolve the existing PR via pulls.list.
 *   6. For a PR just opened, ask the reviewers and teams of the input to review it
 *      via pulls.requestReviewers (see requestReviews for a refused request). A
 *      refusal does not fail the call: the PR is open, the owners GitHub refused
 *      come back in warnings, and the body is rewritten (pulls.update) through
 *      input.bodyWithRefused so it does not claim their reviews were requested.
 *      A PR that already existed is returned as it is: its reviewers are not asked
 *      again, since that would request a review again from an owner who gave one.
 */
export function makePullRequestCreator(octokit: OctokitLike): PullRequestCreator {
  return {
    async create(input: CreatePRInput): Promise<CreatedPullRequest> {
      // A commit over no files produces a tree identical to its parent, which
      // GitHub then refuses to open a pull request for — surfacing several
      // steps later as an unexplained 422 with no PR to fall back to. Say what
      // is actually wrong here instead.
      if (input.files.length === 0) {
        throw new Error('cannot open a pull request: no files to commit');
      }

      const [owner, repoName] = input.repo.split('/');

      // Step 1: the head branch is cut from the base branch head, so the pull
      // request is never behind its base and carries no older copy of a file
      // the base branch has changed since (GitHub refuses a branch that would
      // carry an older workflow file to an App without the workflows
      // permission). The files were written against input.baseSha, so each of
      // them must still be the same on the base branch head: committing one
      // that changed there would replace the newer version. Nothing is written
      // to the repository before this check passes.
      const refData = await octokit.git.getRef({
        owner,
        repo: repoName,
        ref: `heads/${input.baseBranch}`,
      });
      const headSha = refData.data.object.sha;
      if (input.baseSha && input.baseSha !== headSha) {
        // getContent answers 404 both for a path the commit lacks and for a
        // commit that does not exist; reading the commit first makes the
        // second fail here rather than read as files it never had.
        await octokit.git.getCommit({ owner, repo: repoName, commit_sha: input.baseSha });
        const changed = await changedPaths(
          octokit,
          owner,
          repoName,
          input.files.map((file) => file.path),
          input.baseSha,
          headSha,
        );
        if (changed.length > 0) {
          throw new FileChangedOnBaseBranchError(input.baseBranch, changed, input.baseSha, headSha);
        }
      }

      // Step 2: create the head branch — ignore 422 (branch already exists)
      try {
        await octokit.git.createRef({
          owner,
          repo: repoName,
          ref: `refs/heads/${input.headBranch}`,
          sha: headSha,
        });
      } catch (err: unknown) {
        const e = err as { status?: number };
        if (e.status !== 422) throw err;
        // Branch already exists — continue
      }

      // Step 3: commit every file at once. Each file becomes a blob; the blobs
      // are layered onto the base branch head's tree so untouched paths are
      // carried over unchanged, and the resulting tree is committed in one go.
      // Writing the files individually would produce one commit per file, and
      // a diff that is only correct once the last of them lands.
      const headCommit = await octokit.git.getCommit({
        owner,
        repo: repoName,
        commit_sha: headSha,
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

      // Every entry is written as a regular non-executable file. A tree entry
      // states the mode outright rather than inheriting the path's existing
      // one, so remediating a file that is executable in the repository drops
      // its executable bit.
      const tree = await octokit.git.createTree({
        owner,
        repo: repoName,
        base_tree: headCommit.data.tree.sha,
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
        parents: [headSha],
      });

      // Step 4: move the head branch onto the new commit. The branch name is
      // scoped to this proposal attempt, so the only tip force can overwrite
      // is one left by an earlier run of this same attempt. That is normally a
      // discarded commit of the same proposal — but not always: if the earlier
      // run opened the pull request and then failed to record it, the claim
      // can be retried while the branch already carries a pull request someone
      // may have pushed to. The reconciler's opening sweep records rather than
      // releases a claim whose pull request exists, which keeps that window
      // narrow rather than closing it.
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
        const opened = { url: prData.data.html_url, number: prData.data.number };

        // Step 6: ask the owners to review. The pull request is open whatever
        // GitHub answers here, so a refusal is reported, not thrown.
        const reviewers = input.reviewers ?? [];
        const teamReviewers = input.teamReviewers ?? [];
        if (reviewers.length === 0 && teamReviewers.length === 0) return opened;
        const { refused, refusals } = await requestReviews(
          octokit,
          { owner, repo: repoName, number: opened.number },
          reviewers,
          teamReviewers,
        );
        if (refusals.length === 0) return opened;

        const warnings = [`GitHub refused the review request for ${refusals.join('; ')}`];
        if (input.bodyWithRefused) {
          try {
            await octokit.pulls.update({
              owner,
              repo: repoName,
              pull_number: opened.number,
              body: input.bodyWithRefused(refused),
            });
          } catch (updateErr: unknown) {
            warnings.push(
              `The pull request body still says the refused reviews were requested: GitHub would not update it${refusalText(updateErr)}`,
            );
          }
        }
        return { ...opened, warnings };
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
