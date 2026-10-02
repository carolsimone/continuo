import { describe, it, expect } from 'vitest';
import { mkdtempSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import path from 'path';
import { BINDING_FIELDS, loadCiAuthConfig, parseCiAuthConfig } from '../../src/server/auth/ci-config';

const secure = { allowInsecureIssuer: false };
const base = {
  issuer: 'https://token.actions.githubusercontent.com',
  audience: 'https://continuo.example.com',
  bindings: { core: [{ repositoryId: '812345678', ref: ['refs/heads/main'], allowBootstrap: true }] },
};

describe('parseCiAuthConfig', () => {
  it('parses a valid config', () => {
    const cfg = parseCiAuthConfig(base, secure);
    expect(cfg.issuer).toBe('https://token.actions.githubusercontent.com');
    expect(cfg.audience).toBe('https://continuo.example.com');
    expect(cfg.bindings.get('core')).toEqual([{ repositoryId: '812345678', ref: ['refs/heads/main'], allowBootstrap: true }]);
  });

  it('defaults allowBootstrap to false', () => {
    const cfg = parseCiAuthConfig({ ...base, bindings: { core: [{ repositoryId: '1' }] } }, secure);
    expect(cfg.bindings.get('core')![0].allowBootstrap).toBe(false);
  });

  it('trims a trailing slash from the issuer so it matches GitHub iss exactly', () => {
    const cfg = parseCiAuthConfig({ ...base, issuer: 'https://token.actions.githubusercontent.com/' }, secure);
    expect(cfg.issuer).toBe('https://token.actions.githubusercontent.com');
  });

  it('accepts empty bindings (no CI identity can do anything)', () => {
    expect(parseCiAuthConfig({ ...base, bindings: {} }, secure).bindings.size).toBe(0);
  });

  it.each([
    ['missing repositoryId', { core: [{ ref: ['refs/heads/main'] }] }, /repositoryId/],
    ['non-digit repositoryId', { core: [{ repositoryId: 'carolsimone/demo' }] }, /repositoryId/],
    ['numeric repositoryId', { core: [{ repositoryId: 812345678 }] }, /repositoryId/],
    ['unknown field', { core: [{ repositoryId: '1', branch: 'main' }] }, /unknown field "branch"/],
    ['empty ref list', { core: [{ repositoryId: '1', ref: [] }] }, /ref/],
    ['non-boolean refProtected', { core: [{ repositoryId: '1', refProtected: 'true' }] }, /refProtected/],
    ['binding list not an array', { core: { repositoryId: '1' } }, /core/],
  ])('rejects %s', (_name, bindings, msg) => {
    expect(() => parseCiAuthConfig({ ...base, bindings }, secure)).toThrow(msg);
  });

  it('rejects an empty audience and unknown top-level keys', () => {
    expect(() => parseCiAuthConfig({ ...base, audience: '' }, secure)).toThrow(/audience/);
    expect(() => parseCiAuthConfig({ ...base, extra: 1 }, secure)).toThrow(/unknown key "extra"/);
  });

  it('rejects an http issuer unless insecure issuers are allowed', () => {
    const http = { ...base, issuer: 'http://stub-github:9200' };
    expect(() => parseCiAuthConfig(http, secure)).toThrow(/https/);
    expect(parseCiAuthConfig(http, { allowInsecureIssuer: true }).issuer).toBe('http://stub-github:9200');
  });

  it('exposes the accepted binding fields', () => {
    expect([...BINDING_FIELDS].sort()).toEqual(['allowBootstrap', 'environment', 'ref', 'refProtected', 'repositoryId', 'workflowRef']);
  });
});

describe('loadCiAuthConfig', () => {
  function file(content: unknown): string {
    const dir = mkdtempSync(path.join(tmpdir(), 'ci-auth-'));
    const p = path.join(dir, 'ci-auth.json');
    writeFileSync(p, JSON.stringify(content));
    return p;
  }

  it('returns null when CI_AUTH_CONFIG_PATH is unset', () => {
    expect(loadCiAuthConfig({}, 'oidc')).toBeNull();
  });

  it('loads the file and allows http in dev mode', () => {
    const p = file({ ...base, issuer: 'http://stub-github:9200' });
    expect(loadCiAuthConfig({ CI_AUTH_CONFIG_PATH: p }, 'dev')?.issuer).toBe('http://stub-github:9200');
  });

  it('allows http in oidc mode only with CI_AUTH_ALLOW_INSECURE_ISSUER=true', () => {
    const p = file({ ...base, issuer: 'http://stub-github:9200' });
    expect(() => loadCiAuthConfig({ CI_AUTH_CONFIG_PATH: p }, 'oidc')).toThrow(/https/);
    expect(loadCiAuthConfig({ CI_AUTH_CONFIG_PATH: p, CI_AUTH_ALLOW_INSECURE_ISSUER: 'true' }, 'oidc')).not.toBeNull();
  });

  it('fails when the configured file is missing or not JSON', () => {
    expect(() => loadCiAuthConfig({ CI_AUTH_CONFIG_PATH: '/nonexistent/ci-auth.json' }, 'oidc')).toThrow(/CI_AUTH_CONFIG_PATH/);
    const dir = mkdtempSync(path.join(tmpdir(), 'ci-auth-'));
    const bad = path.join(dir, 'bad.json');
    writeFileSync(bad, 'not json');
    expect(() => loadCiAuthConfig({ CI_AUTH_CONFIG_PATH: bad }, 'oidc')).toThrow(/CI_AUTH_CONFIG_PATH/);
  });
});
