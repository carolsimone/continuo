import { describe, it, expect, afterEach } from 'vitest';
import { discoverOidc } from '../../src/server/auth/oidc';
import { startStubIdp, type StubIdp } from './stub-idp';
import type { OidcAuthConfig } from '../../src/server/auth/config';

let idp: StubIdp | undefined;
afterEach(() => idp?.close());

function cfgFor(issuer: string): OidcAuthConfig {
  return {
    mode: 'oidc',
    issuerUrl: issuer,
    clientId: 'continuo-ui',
    clientSecret: 'test-secret',
    publicUrl: 'http://app.local:8090',
    scopes: 'openid email profile',
    groupsClaim: 'groups',
    roleMapping: new Map(),
    operatorEmails: new Set(),
    viewerEmails: new Set(),
    defaultRole: 'none',
    sessionIdleTtlSeconds: 3600,
    sessionMaxTtlSeconds: 7200,
    redisUrl: 'redis://unused',
  };
}

describe('discoverOidc', () => {
  it('builds an authorization URL with PKCE, state and nonce', async () => {
    idp = await startStubIdp();
    const flow = await discoverOidc(cfgFor(idp.issuer));
    const { url, pending } = await flow.buildLoginRedirect('/schedule/daily');
    const u = new URL(url);
    expect(u.origin + u.pathname).toBe(`${idp.issuer}/authorize`);
    expect(u.searchParams.get('client_id')).toBe('continuo-ui');
    expect(u.searchParams.get('redirect_uri')).toBe('http://app.local:8090/auth/callback');
    expect(u.searchParams.get('code_challenge_method')).toBe('S256');
    expect(u.searchParams.get('state')).toBe(pending.state);
    expect(u.searchParams.get('nonce')).toBe(pending.nonce);
    expect(pending.returnTo).toBe('/schedule/daily');
  });

  it('handleCallback exchanges the code, validates the ID token and returns identity', async () => {
    idp = await startStubIdp();
    const flow = await discoverOidc(cfgFor(idp.issuer));
    const { pending } = await flow.buildLoginRedirect('/');
    idp.setNextClaims({ sub: 'u-42', email: 'ana@corp.com', name: 'Ana', groups: ['eng'], nonce: pending.nonce });

    const callbackUrl = new URL(`http://app.local:8090/auth/callback?code=fake-code&state=${pending.state}`);
    const identity = await flow.handleCallback(callbackUrl, pending);

    expect(identity.userId).toBe(`${new URL(idp.issuer).host}|u-42`);
    expect(identity.email).toBe('ana@corp.com');
    expect(identity.claims.groups).toEqual(['eng']);
    // PKCE verifier reached the token endpoint:
    expect(idp.lastTokenRequest()?.get('code_verifier')).toBe(pending.codeVerifier);
  });

  it('rejects a state mismatch', async () => {
    idp = await startStubIdp();
    const flow = await discoverOidc(cfgFor(idp.issuer));
    const { pending } = await flow.buildLoginRedirect('/');
    const callbackUrl = new URL('http://app.local:8090/auth/callback?code=fake-code&state=WRONG');
    await expect(flow.handleCallback(callbackUrl, pending)).rejects.toThrow();
  });

  it('rejects a nonce mismatch in the ID token', async () => {
    idp = await startStubIdp();
    const flow = await discoverOidc(cfgFor(idp.issuer));
    const { pending } = await flow.buildLoginRedirect('/');
    idp.setNextClaims({ sub: 'u-42', email: 'a@b.c', nonce: 'WRONG-NONCE' });
    const callbackUrl = new URL(`http://app.local:8090/auth/callback?code=fake-code&state=${pending.state}`);
    await expect(flow.handleCallback(callbackUrl, pending)).rejects.toThrow();
  });

  it('exposes the end-session URL when the IdP advertises one', async () => {
    idp = await startStubIdp();
    const flow = await discoverOidc(cfgFor(idp.issuer));
    expect(flow.endSessionUrl('http://app.local:8090')).toContain(`${idp.issuer}/logout`);
  });

  it('retries discovery and throws after maxAttempts when the IdP is down', async () => {
    const cfg = cfgFor('http://127.0.0.1:1'); // nothing listens here
    await expect(discoverOidc(cfg, { maxAttempts: 2, backoffMs: 10 })).rejects.toThrow(/discovery failed after 2 attempts/);
  });
});
