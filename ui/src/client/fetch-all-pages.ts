export const PAGE_SIZE = 200;

// Upper bound on pages walked in one call. The loop trusts a server-supplied
// total_count; a wrong value would otherwise spin forever inside a component
// that re-polls every few seconds. On hitting the cap we return what we have.
export const MAX_PAGES = 25;

// Fetches every page of a paged list endpoint and returns the concatenated
// rows. The endpoint must answer { total_count: number, [key]: T[] }.
export async function fetchAllPages<T>(
  url: string,
  key: string,
  opts?: { signal?: AbortSignal }
): Promise<T[]> {
  const signal = opts?.signal;
  const out: T[] = [];

  for (let pageIndex = 0; pageIndex < MAX_PAGES; pageIndex++) {
    if (signal?.aborted) return out;

    const offset = pageIndex * PAGE_SIZE;
    const response = await fetch(`${url}?limit=${PAGE_SIZE}&offset=${offset}`, { signal });
    if (!response.ok) {
      throw new Error(`${url} responded ${response.status}`);
    }

    const body = await response.json();
    const rows: T[] = body[key] ?? [];
    out.push(...rows);

    if (rows.length === 0) return out;
    if (out.length >= Number(body.total_count ?? 0)) return out;
    if (signal?.aborted) return out;
  }

  console.warn(`fetchAllPages: hit page cap of ${MAX_PAGES} for ${url}`);
  return out;
}
