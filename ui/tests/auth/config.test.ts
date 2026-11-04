import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import path from 'path';
import { loadAuthConfig, loadPublicUrl, parseDuration } from '../../src/server/auth/config';

const oidcEnv = {
  AUTH_MODE: 'oidc',
  AUTH_OIDC_ISSUER_URL: 'https://idp.example.com',
  AUTH_OIDC_CLIENT_ID: 'continuo-ui',
  AUTH_OIDC_CLIENT_SECRET: 's3cret',
  AUTH_PUBLIC_URL: 'https://continuo.example.com/',
  REDIS_URL: 'redis://redis:6379',
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

  it('rejects a REDIS_URL that carries credentials', () => {
    const msg = 'REDIS_URL must not carry credentials; set REDIS_USERNAME and REDIS_PASSWORD';
    expect(() => loadAuthConfig({ ...oidcEnv, REDIS_URL: 'redis://:pw@redis:6379' })).toThrow(msg);
    expect(() => loadAuthConfig({ ...oidcEnv, REDIS_URL: 'redis://ui@redis:6379' })).toThrow(msg);
  });

  it('reads the Redis username from REDIS_USERNAME', () => {
    const cfg = loadAuthConfig({ ...oidcEnv, REDIS_USERNAME: 'ui' });
    if (cfg.mode !== 'oidc') throw new Error('expected oidc');
    expect(cfg.redisUrl).toBe('redis://redis:6379');
    expect(cfg.redisUsername).toBe('ui');
  });

  it('reads REDIS_ACL_MODE, shared when unset', () => {
    const unset = loadAuthConfig(oidcEnv);
    if (unset.mode !== 'oidc') throw new Error('expected oidc');
    expect(unset.redisAclMode).toBe('shared');
    const perService = loadAuthConfig({ ...oidcEnv, REDIS_ACL_MODE: 'perService' });
    if (perService.mode !== 'oidc') throw new Error('expected oidc');
    expect(perService.redisAclMode).toBe('perService');
  });

  it('rejects an unknown REDIS_ACL_MODE', () => {
    expect(() => loadAuthConfig({ ...oidcEnv, REDIS_ACL_MODE: 'perservice' })).toThrow(
      'REDIS_ACL_MODE must be "shared" or "perService", got "perservice"',
    );
  });

  it('reads AUTH_OIDC_CLIENT_SECRET from a file', () => {
    const dir = mkdtempSync(path.join(tmpdir(), 'oidc-secret-'));
    try {
      const file = path.join(dir, 'AUTH_OIDC_CLIENT_SECRET');
      writeFileSync(file, 'from-file\n');
      const { AUTH_OIDC_CLIENT_SECRET: _omitted, ...rest } = oidcEnv;
      const cfg = loadAuthConfig({ ...rest, AUTH_OIDC_CLIENT_SECRET_FILE: file });
      if (cfg.mode !== 'oidc') throw new Error('expected oidc');
      expect(cfg.clientSecret).toBe('from-file');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
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

describe('loadPublicUrl', () => {
  it('strips trailing slashes', () => {
    expect(loadPublicUrl({ AUTH_PUBLIC_URL: 'https://continuo.example.com//' })).toBe('https://continuo.example.com');
    expect(loadPublicUrl({ AUTH_PUBLIC_URL: 'https://continuo.example.com/ui' })).toBe('https://continuo.example.com/ui');
  });

  it('is undefined when unset or blank', () => {
    expect(loadPublicUrl({})).toBeUndefined();
    expect(loadPublicUrl({ AUTH_PUBLIC_URL: '  ' })).toBeUndefined();
    expect(loadPublicUrl({ AUTH_PUBLIC_URL: '/' })).toBeUndefined();
  });

  it('is the value loadAuthConfig uses, and oidc mode still requires it', () => {
    const cfg = loadAuthConfig(oidcEnv);
    expect(cfg.mode === 'oidc' && cfg.publicUrl).toBe(loadPublicUrl(oidcEnv));
    const { AUTH_PUBLIC_URL: _omitted, ...partial } = oidcEnv;
    expect(() => loadAuthConfig(partial)).toThrow(/AUTH_PUBLIC_URL is required/);
  });
});
