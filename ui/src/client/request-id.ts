import { useCallback, useState } from 'react';

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
  // The request id every send of the current action carries. Run-creating
  // commands run once per request id, so a send that is repeated with the
  // same id returns the run the first send made instead of making another.
  id: string;
  // renew mints the id of the next action. It is stable across renders.
  renew: () => void;
}

// useActionRequestId holds the request id of one user action. The id is
// minted when the component mounts and kept across re-renders, so a retry
// (the same press repeated after a timeout, or a dialog submitted again)
// sends the id the first try sent. A component calls renew once the action
// has ended or what it asks for has changed, so the next press sends a new id.
export function useActionRequestId(): ActionRequestId {
  const [id, setId] = useState(newRequestId);
  const renew = useCallback(() => setId(newRequestId()), []);
  return { id, renew };
}
