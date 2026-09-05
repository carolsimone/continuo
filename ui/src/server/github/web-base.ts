// The GitHub web host that pages link out to, derived from the API base the
// server is configured with (GITHUB_API_BASE_URL), so a GitHub Enterprise
// install links to its own host rather than github.com. GitHub's own layout
// fixes the mapping: the public API lives at api.github.com for github.com,
// and an Enterprise Server API lives under /api/v3 of its web host.
export function githubWebBaseUrl(apiBaseUrl: string | undefined): string {
  const base = (apiBaseUrl ?? '').trim().replace(/\/+$/, '');
  if (!base || base === 'https://api.github.com') return 'https://github.com';
  return base.replace(/\/api\/v3$/, '');
}

// commitUrl is the page of one commit under the web host. Empty unless both
// halves are known and the repo is the owner/name form release-controller
// records, so a page never renders a link that cannot resolve.
export function commitUrl(webBaseUrl: string, repo: string, sha: string): string {
  if (!repo || !sha) return '';
  const parts = repo.split('/');
  if (parts.length !== 2 || !parts[0] || !parts[1]) return '';
  return `${webBaseUrl}/${parts[0]}/${parts[1]}/commit/${sha}`;
}
