// The body release-controller's POST /releases takes for a candidate release.
export interface ReleaseSubmission {
  release_id: string;
  project: string;
  image_tag: string;
  bootstrap?: boolean;
  rebaseline?: boolean;
  kind?: string;
  repo?: string;
  commit_sha?: string;
}

export interface ReleaseClient {
  listReleases(query: Record<string, string>): Promise<any>;
  getRelease(id: string): Promise<any>;
  getLiveRelease(): Promise<any>;
  retryRemediation(id: string): Promise<{ status: number; body: unknown }>;
  getReleaseCheck(id: string): Promise<any>;
  listReleaseChecks(releaseId: string): Promise<any>;
  getReleaseQueue(): Promise<any>;
  submitRelease(body: ReleaseSubmission): Promise<{ status: number; text: string }>;
}

export class HttpError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

export function createReleaseClient(baseUrl: string): ReleaseClient {
  const base = baseUrl.replace(/\/$/, '');

  async function getJson(path: string): Promise<any> {
    const resp = await fetch(`${base}${path}`);
    if (!resp.ok) {
      throw new HttpError(resp.status, `release-controller ${path} -> ${resp.status}`);
    }
    return resp.json();
  }

  return {
    listReleases(query) {
      const qs = new URLSearchParams(query).toString();
      return getJson(`/releases${qs ? `?${qs}` : ''}`);
    },
    getRelease(id) {
      return getJson(`/releases/${encodeURIComponent(id)}`);
    },
    getLiveRelease() {
      return getJson('/live-release');
    },
    getReleaseCheck(id) {
      return getJson(`/release-checks/${encodeURIComponent(id)}`);
    },
    listReleaseChecks(releaseId) {
      return getJson(`/release-checks?verifies=${encodeURIComponent(releaseId)}`);
    },
    getReleaseQueue() {
      return getJson('/release-queue');
    },
    // Forwards a release submission. release-controller answers 202 with JSON,
    // or a plain-text error body for 400/409; the caller maps both.
    async submitRelease(body) {
      const resp = await fetch(`${base}/releases`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      });
      return { status: resp.status, text: await resp.text() };
    },
    async retryRemediation(id) {
      const resp = await fetch(`${base}/releases/${encodeURIComponent(id)}/retry-remediation`, { method: 'POST' });
      const text = await resp.text();
      let body: unknown = {};
      try {
        body = text ? JSON.parse(text) : {};
      } catch {
        body = { error: 'invalid_response' };
      }
      return { status: resp.status, body };
    },
  };
}
