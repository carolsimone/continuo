export interface ReleaseClient {
  listReleases(query: Record<string, string>): Promise<any>;
  getRelease(id: string): Promise<any>;
  getCurrentProd(): Promise<any>;
  retryRemediation(id: string): Promise<{ status: number; body: unknown }>;
  getVerificationRun(id: string): Promise<any>;
  listVerificationRuns(releaseId: string): Promise<any>;
  getPipeline(): Promise<any>;
}

class HttpError extends Error {
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
    getCurrentProd() {
      return getJson('/current-prod');
    },
    getVerificationRun(id) {
      return getJson(`/verification-runs/${encodeURIComponent(id)}`);
    },
    listVerificationRuns(releaseId) {
      return getJson(`/verification-runs?verifies=${encodeURIComponent(releaseId)}`);
    },
    getPipeline() {
      return getJson('/pipeline');
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

export { HttpError };
