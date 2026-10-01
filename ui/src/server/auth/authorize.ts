import type { Principal } from './principal';

export type Action =
  | { kind: 'api.read' }
  | { kind: 'api.mutate' }
  | { kind: 'release.read'; service: string }
  | { kind: 'prod.read' }
  | { kind: 'release.submit'; service: string; bootstrap: boolean };

export type Decision =
  | { allow: true }
  | { allow: false; code: 'forbidden' | 'bootstrap_not_allowed'; reason: string };

const ALLOW: Decision = { allow: true };
const deny = (reason: string, code: 'forbidden' | 'bootstrap_not_allowed' = 'forbidden'): Decision => ({ allow: false, code, reason });

// Every permission decision for /api. Humans are governed by role: viewers
// read, operators do everything. A CI identity reaches only the v1 release
// actions, and only for the services its bindings grant; one whose repository
// matches no binding may do nothing at all.
export function authorize(p: Principal, a: Action): Decision {
  if (p.kind === 'human') {
    if (p.user.role === 'operator') return ALLOW;
    if (a.kind === 'api.read' || a.kind === 'release.read' || a.kind === 'prod.read') return ALLOW;
    return deny('operator role required');
  }
  if (p.grants.size === 0) return deny(`repository ${p.claims.repository} is not bound to any service`);
  switch (a.kind) {
    case 'api.read':
    case 'api.mutate':
      return deny('CI tokens may only call /api/v1');
    case 'prod.read':
      return ALLOW;
    case 'release.read':
      return p.grants.has(a.service) ? ALLOW : deny(`repository ${p.claims.repository} is not bound to service "${a.service}"`);
    case 'release.submit': {
      const grant = p.grants.get(a.service);
      if (!grant) return deny(`repository ${p.claims.repository} is not bound to service "${a.service}"`);
      if (a.bootstrap && !grant.allowBootstrap) {
        return deny(`the binding for service "${a.service}" does not allow bootstrap`, 'bootstrap_not_allowed');
      }
      return ALLOW;
    }
  }
}
