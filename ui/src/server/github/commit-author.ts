import { Octokit } from '@octokit/rest';
import { createAppAuth } from '@octokit/auth-app';
import { privateKeyCanSign } from './private-key';

/**
 * The commit author shown against a release. When the commit email is linked to
 * a GitHub account, `login` (plus `avatar_url` and the profile `html_url`) is
 * populated; when it is not, only `name` — the git commit author metadata — is
 * available. Exactly one of the two shapes is returned; null means the commit
 * could not be resolved at all.
 */
export interface ReleaseAuthor {
  login?: string;
  name?: string;
  avatar_url?: string;
  html_url?: string;
}

export interface CommitAuthorResolver {
  resolve(repo: string, sha: string): Promise<ReleaseAuthor | null>;
}

/**
 * Minimal structural type covering the single Octokit method used here, so tests
 * can inject a hand-written fake without depending on real Octokit types.
 */
export interface CommitOctokitLike {
  repos: {
    getCommit(params: { owner: string; repo: string; ref: string }): Promise<{
      data: {
        author: { login: string; avatar_url: string; html_url: string } | null;
        commit: { author: { name: string } | null };
      };
    }>;
  };
}

/**
 * Resolves a release's commit author from the (repo, commit_sha) already stored
 * on the release, via a single GitHub getCommit call.
 *
 * A commit's author never changes, so each (repo, sha) is resolved at most once
 * and cached for the process lifetime — many releases share a commit, and a page
 * of releases repeats commits, so this collapses to roughly one call per distinct
 * commit rather than one per row. A failed lookup resolves to null and is NOT
 * cached, so a transient GitHub error is retried on the next request.
 */
export function makeCommitAuthorResolver(octokit: CommitOctokitLike): CommitAuthorResolver {
  const cache = new Map<string, Promise<ReleaseAuthor | null>>();
  return {
    resolve(repo, sha) {
      if (!repo || !sha) return Promise.resolve(null);
      const [owner, name] = repo.split('/');
      if (!owner || !name) return Promise.resolve(null);

      const key = `${repo}@${sha}`;
      const cached = cache.get(key);
      if (cached) return cached;

      const pending = octokit.repos
        .getCommit({ owner, repo: name, ref: sha })
        .then(({ data }): ReleaseAuthor | null => {
          if (data.author) {
            return {
              login: data.author.login,
              avatar_url: data.author.avatar_url,
              html_url: data.author.html_url,
            };
          }
          const gitName = data.commit?.author?.name;
          return gitName ? { name: gitName } : null;
        })
        .catch(() => {
          // Drop the failed lookup so the next request can retry rather than
          // serving a cached null forever.
          cache.delete(key);
          return null;
        });

      cache.set(key, pending);
      return pending;
    },
  };
}

/**
 * Resolves the optional GitHub App commit-author resolver from configuration,
 * mirroring resolveGithubAppPullRequestCreator's gate: absent credentials or a
 * key that cannot sign both disable the feature (undefined).
 *
 * The malformed-key case is intentionally NOT logged here — index.ts resolves
 * the PR creator from the same config, which already logs that case loudly, and
 * a second identical line at startup would only be noise.
 */
export function resolveGithubAppCommitAuthorResolver(cfg: {
  appId: string;
  privateKey: string;
  installationId: string;
  baseUrl?: string;
}): CommitAuthorResolver | undefined {
  if (!cfg.appId || !cfg.privateKey || !cfg.installationId) return undefined;
  if (!privateKeyCanSign(cfg.privateKey)) return undefined;

  const octokit = new Octokit({
    ...(cfg.baseUrl ? { baseUrl: cfg.baseUrl } : {}),
    authStrategy: createAppAuth,
    auth: {
      appId: cfg.appId,
      privateKey: cfg.privateKey,
      installationId: cfg.installationId,
    },
  });
  return makeCommitAuthorResolver(octokit as unknown as CommitOctokitLike);
}
