export interface ReleaseClient {
  listReleases(query: Record<string, string>): Promise<any>;
  getRelease(id: string): Promise<any>;
  getCurrentProd(): Promise<any>;
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
  };
}

export { HttpError };
