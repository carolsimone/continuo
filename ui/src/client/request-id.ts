import { useCallback, useRef, useState } from 'react';

// newRequestId mints a version 4 UUID. crypto.randomUUID exists only in a
// secure context (https or localhost), so a page served over plain http mints
// its id from crypto.getRandomValues, which every context has.
export function newRequestId(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

// settlesRequest reports whether a response is the server's final word on a
// request: it succeeded or it was refused. A 5xx may come from a gateway or a
// timed-out call whose command still committed, so the request stays open and
// its retry sends the same request id.
export function settlesRequest(res: { ok: boolean; status: number }): boolean {
  return res.ok || res.status < 500;
}

export interface ActionRequestId {
  // The id a dialog sends for its whole lifetime: run-creating commands run
  // once per request id, so a send repeated with the same id returns the run
  // the first send made instead of making another.
  id: string;
  // renew mints the id of the next action. It is stable across renders.
  renew: () => void;
  // take returns the id for a press made now. fingerprint names what the
  // press asks for; observed names the newest run the page shows that the
  // action could have made (a schedule's last run id, the newest run of a
  // node), or nothing when the page shows none. The press repeats the one
  // already sent, and gets the current id, while both are what that press
  // sent: its outcome is still unknown, however long ago it was sent, so the
  // retry must not make a second run. A press with other parameters, or one
  // made once the page shows a newer run (the earlier press's effect is
  // visible), is another action and gets a new id. It is stable across
  // renders.
  take: (fingerprint?: string, observed?: string | null) => string;
}

interface Sent {
  id: string;
  sent: boolean;
  fingerprint: string;
  observed: string;
}

// useActionRequestId holds the request id of one user action. The id is
// minted when the component mounts and kept across re-renders, so a retry
// (the same press repeated after a timeout, or a dialog submitted again)
// sends the id the first try sent. A component calls renew once the server
// has given its final answer, so the next press sends a new id.
export function useActionRequestId(): ActionRequestId {
  const [id, setId] = useState(newRequestId);
  const sent = useRef<Sent | null>(null);
  if (sent.current === null) sent.current = { id, sent: false, fingerprint: '', observed: '' };

  const renew = useCallback(() => {
    const next = newRequestId();
    sent.current = { id: next, sent: false, fingerprint: '', observed: '' };
    setId(next);
  }, []);

  const take = useCallback((fingerprint = '', observed: string | null = '') => {
    const seen = observed ?? '';
    const cur = sent.current as Sent;
    if (cur.sent && (cur.fingerprint !== fingerprint || cur.observed !== seen)) {
      renew();
    }
    const next = sent.current as Sent;
    if (!next.sent) {
      next.sent = true;
      next.fingerprint = fingerprint;
      next.observed = seen;
    }
    return next.id;
  }, [renew]);

  return { id, renew, take };
}
