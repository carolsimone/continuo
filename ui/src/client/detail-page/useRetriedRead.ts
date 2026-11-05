import { useEffect, useRef, useState } from 'react';
import { readJson } from '../read-json';

const RETRY_INTERVAL_MS = 5000;

// Reads url and, while the read fails, reads it again every 5 s until one
// lands; the answer goes to onData. A null url reads nothing, and a new url or
// a new refreshKey starts over. Returns whether the latest attempt failed, so
// the page can say it is retrying.
export function useRetriedRead<T>(
  url: string | null,
  onData: (data: T) => void,
  refreshKey?: number,
): boolean {
  const [failed, setFailed] = useState(false);
  const onDataRef = useRef(onData);
  onDataRef.current = onData;

  useEffect(() => {
    setFailed(false);
    if (!url) return;
    // Set once a read has landed or the url has changed: an answer that
    // arrives after that, from a read still in flight, changes nothing.
    let settled = false;
    let timer: number | undefined;

    const read = () => {
      readJson<T>(url)
        .then((data) => {
          if (settled) return;
          settled = true;
          window.clearInterval(timer);
          setFailed(false);
          onDataRef.current(data);
        })
        .catch(() => {
          if (settled) return;
          setFailed(true);
          if (timer === undefined) timer = window.setInterval(read, RETRY_INTERVAL_MS);
        });
    };
    read();

    return () => {
      settled = true;
      window.clearInterval(timer);
    };
  }, [url, refreshKey]);

  return failed;
}
