import { describe, it, expect, vi } from 'vitest';
import { makeCommitAuthorResolver } from '../../src/server/github/commit-author';

// getCommit response shapes. GitHub returns `author` (the linked account, with a
// login) OR null when the commit email is not tied to a GitHub user; `commit.author`
// (the git metadata name) is always present.
function linked(login: string) {
  return {
    data: {
      author: { login, avatar_url: `https://avatars/${login}.png`, html_url: `https://github.com/${login}` },
      commit: { author: { name: 'Ada Lovelace' } },
    },
  };
}
function unlinked(name: string) {
  return { data: { author: null, commit: { author: { name } } } };
}

function octokitWith(getCommit: ReturnType<typeof vi.fn>) {
  return { repos: { getCommit } };
}

describe('commit-author resolver', () => {
  it('resolves the linked GitHub account (login, avatar, profile url)', async () => {
    const getCommit = vi.fn().mockResolvedValue(linked('octocat'));
    const author = await makeCommitAuthorResolver(octokitWith(getCommit)).resolve('acme/dbt', 'abc123');
    expect(author).toEqual({
      login: 'octocat',
      avatar_url: 'https://avatars/octocat.png',
      html_url: 'https://github.com/octocat',
    });
    expect(getCommit).toHaveBeenCalledWith({ owner: 'acme', repo: 'dbt', ref: 'abc123' });
  });

  it('falls back to the git commit author name when the account is unlinked', async () => {
    const getCommit = vi.fn().mockResolvedValue(unlinked('Grace Hopper'));
    const author = await makeCommitAuthorResolver(octokitWith(getCommit)).resolve('acme/dbt', 'sha');
    expect(author).toEqual({ name: 'Grace Hopper' });
  });

  it('returns null when neither a linked account nor a commit name is available', async () => {
    const getCommit = vi.fn().mockResolvedValue({ data: { author: null, commit: { author: null } } });
    const author = await makeCommitAuthorResolver(octokitWith(getCommit)).resolve('acme/dbt', 'sha');
    expect(author).toBeNull();
  });

  it('caches by repo@sha — a repeated commit is not fetched twice', async () => {
    const getCommit = vi.fn().mockResolvedValue(linked('octocat'));
    const resolver = makeCommitAuthorResolver(octokitWith(getCommit));
    await resolver.resolve('acme/dbt', 'abc123');
    await resolver.resolve('acme/dbt', 'abc123');
    expect(getCommit).toHaveBeenCalledTimes(1);
  });

  it('does not call GitHub for an empty repo or sha', async () => {
    const getCommit = vi.fn();
    const resolver = makeCommitAuthorResolver(octokitWith(getCommit));
    expect(await resolver.resolve('', 'sha')).toBeNull();
    expect(await resolver.resolve('acme/dbt', '')).toBeNull();
    expect(getCommit).not.toHaveBeenCalled();
  });

  it('resolves null on a fetch error and does not cache it (the next call retries)', async () => {
    const getCommit = vi.fn()
      .mockRejectedValueOnce(Object.assign(new Error('rate limited'), { status: 403 }))
      .mockResolvedValueOnce(linked('octocat'));
    const resolver = makeCommitAuthorResolver(octokitWith(getCommit));
    expect(await resolver.resolve('acme/dbt', 'abc123')).toBeNull();
    expect(await resolver.resolve('acme/dbt', 'abc123')).toEqual(
      expect.objectContaining({ login: 'octocat' }),
    );
    expect(getCommit).toHaveBeenCalledTimes(2);
  });
});
