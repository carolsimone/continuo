import type { Request, Response } from 'express';

// REQUEST_ID_MESSAGE is the 400 a mutating route answers when the body holds no
// request_id, or one that is not a UUID.
export const REQUEST_ID_MESSAGE = 'request_id must be a UUID';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// isRequestId reports whether v is a UUID in its canonical 8-4-4-4-12 form.
export function isRequestId(v: unknown): v is string {
  return typeof v === 'string' && UUID.test(v);
}

// requireRequestId reads the request_id of a mutating route's JSON body, the id
// the browser minted for the user's action, which the route forwards to run-controller
// so a repeat of the action creates no second run. A body without a UUID
// request_id is answered 400 and null is returned; the caller returns without
// calling run-controller.
export function requireRequestId(req: Request, res: Response): string | null {
  const id = (req.body as { request_id?: unknown } | undefined | null)?.request_id;
  if (!isRequestId(id)) {
    res.status(400).json({ error: REQUEST_ID_MESSAGE });
    return null;
  }
  return id;
}
