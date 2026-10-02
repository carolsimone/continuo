import type { Request } from 'express';
import type { Principal } from './principal';

// The principal of an /api request: the one bearerAuth attached, or a human
// built from the session or dev identity in req.user. Undefined when the
// request is unauthenticated.
export function principalOf(req: Request): Principal | undefined {
  if (req.principal) return req.principal;
  return req.user ? { kind: 'human', user: req.user } : undefined;
}
