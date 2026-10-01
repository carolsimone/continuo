import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { authorize, type Action } from '../../src/server/auth/authorize';
import { bindingMatches, resolveGrants, principalKey, type Principal } from '../../src/server/auth/principal';
import { githubClaimsFrom } from '../../src/server/auth/bearer';
import type { CiBinding } from '../../src/server/auth/ci-config';
import { githubClaims } from './stub-issuer';

const claims = githubClaimsFrom(githubClaims());
const viewer: Principal = { kind: 'human', user: { userId: 'i|v', email: 'v@c.com', name: 'V', role: 'viewer' } };
const operator: Principal = { kind: 'human', user: { userId: 'i|o', email: 'o@c.com', name: 'O', role: 'operator' } };
const ci: Principal = {
  kind: 'ci', subject: 'repo:x', claims,
  grants: new Map([['core', { allowBootstrap: false }], ['finance', { allowBootstrap: true }]]),
};
// A verified GitHub token whose repository matches no binding.
const unboundCi: Principal = { kind: 'ci', subject: 'repo:y', claims, grants: new Map() };

const allow = { allow: true };
const cases: Array<[string, Principal, Action, boolean, string?]> = [
  ['viewer api.read', viewer, { kind: 'api.read' }, true],
  ['viewer api.mutate', viewer, { kind: 'api.mutate' }, false, 'forbidden'],
  ['viewer release.read', viewer, { kind: 'release.read', service: 'core' }, true],
  ['viewer prod.read', viewer, { kind: 'prod.read' }, true],
  ['viewer submit', viewer, { kind: 'release.submit', service: 'core', bootstrap: false }, false, 'forbidden'],
  ['operator api.mutate', operator, { kind: 'api.mutate' }, true],
  ['operator submit bootstrap any service', operator, { kind: 'release.submit', service: 'anything', bootstrap: true }, true],
  ['ci api.read', ci, { kind: 'api.read' }, false, 'forbidden'],
  ['ci api.mutate', ci, { kind: 'api.mutate' }, false, 'forbidden'],
  ['ci release.read bound', ci, { kind: 'release.read', service: 'core' }, true],
  ['ci release.read unbound', ci, { kind: 'release.read', service: 'marketing' }, false, 'forbidden'],
  ['ci prod.read', ci, { kind: 'prod.read' }, true],
  ['ci submit bound', ci, { kind: 'release.submit', service: 'core', bootstrap: false }, true],
  ['ci submit unbound', ci, { kind: 'release.submit', service: 'marketing', bootstrap: false }, false, 'forbidden'],
  ['ci bootstrap without flag', ci, { kind: 'release.submit', service: 'core', bootstrap: true }, false, 'bootstrap_not_allowed'],
  ['ci bootstrap with flag', ci, { kind: 'release.submit', service: 'finance', bootstrap: true }, true],
  ['zero-grant ci prod.read', unboundCi, { kind: 'prod.read' }, false, 'forbidden'],
  ['zero-grant ci release.read', unboundCi, { kind: 'release.read', service: 'core' }, false, 'forbidden'],
  ['zero-grant ci submit', unboundCi, { kind: 'release.submit', service: 'core', bootstrap: false }, false, 'forbidden'],
  ['zero-grant ci api.read', unboundCi, { kind: 'api.read' }, false, 'forbidden'],
];

describe('authorize', () => {
  it.each(cases)('%s', (_n, p, a, allowed, code) => {
    const d = authorize(p, a);
    if (allowed) expect(d).toEqual(allow);
    else expect(d).toMatchObject({ allow: false, code });
  });
});

describe('binding match', () => {
  const b = (o: Partial<CiBinding>): CiBinding => ({ repositoryId: '812345678', allowBootstrap: false, ...o });
  it('matches on repositoryId alone', () => expect(bindingMatches(b({}), claims)).toBe(true));
  it('never matches another repository id', () => expect(bindingMatches(b({ repositoryId: '1' }), claims)).toBe(false));
  it('enforces ref lists', () => {
    expect(bindingMatches(b({ ref: ['refs/heads/main'] }), claims)).toBe(true);
    expect(bindingMatches(b({ ref: ['refs/heads/release'] }), claims)).toBe(false);
  });
  it('enforces refProtected', () => {
    expect(bindingMatches(b({ refProtected: true }), claims)).toBe(true);
    expect(bindingMatches(b({ refProtected: true }), githubClaimsFrom(githubClaims({ ref_protected: 'false' })))).toBe(false);
  });
  it('enforces environment and workflowRef', () => {
    expect(bindingMatches(b({ environment: ['prod'] }), claims)).toBe(false);
    expect(bindingMatches(b({ environment: ['prod'] }), githubClaimsFrom(githubClaims({ environment: 'prod' })))).toBe(true);
    expect(bindingMatches(b({ workflowRef: ['other@refs/heads/main'] }), claims)).toBe(false);
  });
  it('resolveGrants: a service is granted when any binding matches; bootstrap if any matching binding allows it', () => {
    const grants = resolveGrants(claims, new Map([
      ['core', [b({ ref: ['refs/heads/x'], allowBootstrap: true }), b({})]],
      ['finance', [b({ allowBootstrap: true })]],
      ['marketing', [b({ repositoryId: '1' })]],
    ]));
    expect([...grants.keys()].sort()).toEqual(['core', 'finance']);
    expect(grants.get('core')).toEqual({ allowBootstrap: false });
    expect(grants.get('finance')).toEqual({ allowBootstrap: true });
  });
  it('principalKey separates ci and humans', () => {
    expect(principalKey(ci)).toBe('ci:812345678');
    expect(principalKey(operator)).toBe('human:i|o');
  });
});

// The policy modules import types only, so authorize() and the binding rules
// never load the token verifier (jose) or Express. Every import or re-export
// that names a module must be type-only, and nothing may load one at runtime.
describe('policy module imports', () => {
  it.each(['principal.ts', 'authorize.ts'])('%s has only type imports', (file) => {
    const src = readFileSync(join(__dirname, '../../src/server/auth', file), 'utf8');
    const fromStatements = src.match(/(^|\n)\s*(import|export)\b[^;]*?\bfrom\s*['"][^'"]+['"]/g) ?? [];
    expect(src).toMatch(/\bimport type\b/);
    for (const stmt of fromStatements) expect(stmt.trim()).toMatch(/^(import|export) type\s/);
    expect(src).not.toMatch(/(^|\n)\s*import\s*['"]/);
    expect(src).not.toMatch(/\brequire\s*\(/);
    expect(src).not.toMatch(/\bimport\s*\(/);
  });
});
