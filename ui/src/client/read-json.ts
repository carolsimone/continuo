// Reads a JSON endpoint. An answer that is not 2xx rejects, so a caller's
// success path only ever sees the endpoint's own payload, never an error body.
export async function readJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`${url} responded ${response.status}`);
  }
  return response.json() as Promise<T>;
}
