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
    getCommit(params: {
      owner: string;
      repo: string;
      ref: string;
      request?: { signal?: AbortSignal };
    }): Promise<{
      data: {
        author: { login: string; avatar_url: string; html_url: string } | null;
        commit: { author: { name: string } | null };
      };
    }>;
  };
}

// Default per-lookup deadline. Author enrichment is optional and best-effort, so
// a slow GitHub call must never hold the release-list response open — past this
// the lookup is abandoned (and the request aborted) and the release renders
// without that author.
const DEFAULT_TIMEOUT_MS = 2500;

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
export function makeCommitAuthorResolver(
  octokit: CommitOctokitLike,
  opts: { timeoutMs?: number } = {},
): CommitAuthorResolver {
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const cache = new Map<string, Promise<ReleaseAuthor | null>>();
  return {
    resolve(repo, sha) {
      if (!repo || !sha) return Promise.resolve(null);
      const [owner, name] = repo.split('/');
      if (!owner || !name) return Promise.resolve(null);

      const key = `${repo}@${sha}`;
      const cached = cache.get(key);
      if (cached) return cached;

      const controller = new AbortController();
      const pending = new Promise<ReleaseAuthor | null>((resolve) => {
        // The timer bounds the lookup even if the underlying request ignores
        // the abort: on expiry, abort (to release the socket), forget the
        // in-flight promise so a later request retries, and resolve null.
        const timer = setTimeout(() => {
          controller.abort();
          cache.delete(key);
          resolve(null);
        }, timeoutMs);

        octokit.repos
          .getCommit({ owner, repo: name, ref: sha, request: { signal: controller.signal } })
          .then(({ data }) => {
            clearTimeout(timer);
            if (data.author) {
              resolve({
                login: data.author.login,
                avatar_url: data.author.avatar_url,
                html_url: data.author.html_url,
              });
            } else {
              const gitName = data.commit?.author?.name;
              resolve(gitName ? { name: gitName } : null);
            }
          })
          .catch(() => {
            // Failed or aborted lookup — drop it so the next request retries
            // rather than serving a cached null forever.
            clearTimeout(timer);
            cache.delete(key);
            resolve(null);
          });
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
