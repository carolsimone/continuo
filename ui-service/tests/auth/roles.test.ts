import { describe, it, expect } from 'vitest';
import { resolveRole } from '../../src/server/auth/roles';
import type { Role } from '../../src/server/auth/types';

function cfg(over: Partial<{
  groupsClaim: string;
  roleMapping: Map<string, Role>;
  operatorEmails: Set<string>;
  viewerEmails: Set<string>;
  defaultRole: Role | 'none';
}> = {}) {
  return {
    groupsClaim: 'groups',
    roleMapping: new Map<string, Role>([['ops-team', 'operator'], ['eng', 'viewer']]),
    operatorEmails: new Set<string>(),
    viewerEmails: new Set<string>(),
    defaultRole: 'none' as const,
    ...over,
  };
}

describe('resolveRole', () => {
  it('maps a group to its role', () => {
    expect(resolveRole({ groups: ['eng'] }, 'a@b.com', cfg())).toBe('viewer');
  });

  it('strongest matching group role wins', () => {
    expect(resolveRole({ groups: ['eng', 'ops-team'] }, 'a@b.com', cfg())).toBe('operator');
  });

  it('reads the configured groups claim name', () => {
    const c = cfg({ groupsClaim: 'memberOf' });
    expect(resolveRole({ memberOf: ['ops-team'] }, 'a@b.com', c)).toBe('operator');
  });

  it('falls back to email overrides when no group matches (case-insensitive)', () => {
    const c = cfg({ operatorEmails: new Set(['ana@corp.com']) });
    expect(resolveRole({ groups: ['unmapped'] }, 'Ana@Corp.com', c)).toBe('operator');
  });

  it('group mapping takes priority over email overrides', () => {
    const c = cfg({ operatorEmails: new Set(['a@b.com']) });
    expect(resolveRole({ groups: ['eng'] }, 'a@b.com', c)).toBe('viewer');
  });

  it('returns the default role when nothing matches (deny by default)', () => {
    expect(resolveRole({}, 'nobody@corp.com', cfg())).toBe('none');
    expect(resolveRole({}, 'nobody@corp.com', cfg({ defaultRole: 'viewer' }))).toBe('viewer');
  });

  it('tolerates a non-array groups claim', () => {
    expect(resolveRole({ groups: 'not-an-array' }, 'x@y.z', cfg())).toBe('none');
  });
});
