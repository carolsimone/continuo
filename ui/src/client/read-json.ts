// How long a read may go unanswered before it counts as failed. A backend that
// has stopped answering can otherwise hold a read open for as long as the
// connection lasts, with nothing on screen saying so.
const READ_TIMEOUT_SECONDS = 10;

// Reads a JSON endpoint. An answer that is not 2xx rejects, so a caller's
// success path only ever sees the endpoint's own payload, never an error body.
// The read also rejects, and its request is aborted, after READ_TIMEOUT_SECONDS
// or when the caller's signal aborts.
export async function readJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const request = new AbortController();
  const abort = () => request.abort(signal?.reason);
  const timer = setTimeout(
    () => request.abort(new Error(`${url} did not answer within ${READ_TIMEOUT_SECONDS} s`)),
    READ_TIMEOUT_SECONDS * 1000,
  );
  signal?.addEventListener('abort', abort);
  try {
    const response = await fetch(url, { signal: request.signal });
    if (!response.ok) {
      throw new Error(`${url} responded ${response.status}`);
    }
    return (await response.json()) as T;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', abort);
  }
}
