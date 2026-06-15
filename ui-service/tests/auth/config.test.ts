import { describe, it, expect } from 'vitest';
import { loadAuthConfig, parseDuration } from '../../src/server/auth/config';

const oidcEnv = {
  AUTH_MODE: 'oidc',
  AUTH_OIDC_ISSUER_URL: 'https://idp.example.com',
  AUTH_OIDC_CLIENT_ID: 'continuo-ui',
  AUTH_OIDC_CLIENT_SECRET: 's3cret',
  AUTH_PUBLIC_URL: 'https://continuo.example.com/',
  REDIS_URL: 'redis://:pw@redis:6379',
};

describe('loadAuthConfig', () => {
  it('returns dev config for AUTH_MODE=dev', () => {
    expect(loadAuthConfig({ AUTH_MODE: 'dev' })).toEqual({ mode: 'dev' });
  });

  it('rejects missing or unknown AUTH_MODE', () => {
    expect(() => loadAuthConfig({})).toThrow(/AUTH_MODE/);
    expect(() => loadAuthConfig({ AUTH_MODE: 'disabled' })).toThrow(/AUTH_MODE/);
  });

  it('fails fast when an oidc variable is missing', () => {
    const { AUTH_OIDC_CLIENT_SECRET: _omitted, ...partial } = oidcEnv;
    expect(() => loadAuthConfig(partial)).toThrow(/AUTH_OIDC_CLIENT_SECRET/);
    expect(() => loadAuthConfig({ ...oidcEnv, AUTH_OIDC_CLIENT_SECRET: '' })).toThrow(/AUTH_OIDC_CLIENT_SECRET/);
  });

  it('parses a full oidc config with defaults and normalized publicUrl', () => {
    const cfg = loadAuthConfig(oidcEnv);
    if (cfg.mode !== 'oidc') throw new Error('expected oidc');
    expect(cfg.publicUrl).toBe('https://continuo.example.com'); // trailing slash stripped
    expect(cfg.scopes).toBe('openid email profile');
    expect(cfg.groupsClaim).toBe('groups');
    expect(cfg.defaultRole).toBe('none');
    expect(cfg.sessionIdleTtlSeconds).toBe(8 * 3600);
    expect(cfg.sessionMaxTtlSeconds).toBe(24 * 3600);
  });

  it('parses role mapping and email lists', () => {
    const cfg = loadAuthConfig({
      ...oidcEnv,
      AUTH_ROLE_MAPPING: 'data-platform=operator, data-eng=viewer',
      AUTH_OPERATOR_EMAILS: 'Ana@Corp.com',
      AUTH_VIEWER_EMAILS: 'bob@corp.com, carol@corp.com',
    });
    if (cfg.mode !== 'oidc') throw new Error('expected oidc');
    expect(cfg.roleMapping.get('data-platform')).toBe('operator');
    expect(cfg.roleMapping.get('data-eng')).toBe('viewer');
    expect(cfg.operatorEmails.has('ana@corp.com')).toBe(true); // lowercased
    expect(cfg.viewerEmails.has('carol@corp.com')).toBe(true);
  });

  it('rejects unknown roles in mapping and default', () => {
    expect(() => loadAuthConfig({ ...oidcEnv, AUTH_ROLE_MAPPING: 'g=admin' })).toThrow(/unknown role/);
    expect(() => loadAuthConfig({ ...oidcEnv, AUTH_DEFAULT_ROLE: 'root' })).toThrow(/AUTH_DEFAULT_ROLE/);
  });
});

describe('parseDuration', () => {
  it('parses s/m/h and rejects garbage', () => {
    expect(parseDuration('45s', 'X')).toBe(45);
    expect(parseDuration('10m', 'X')).toBe(600);
    expect(parseDuration('8h', 'X')).toBe(8 * 3600);
    expect(() => parseDuration('1d', 'X')).toThrow(/X/);
  });
});
