import { describe, it, expect, afterEach } from 'vitest';
import { SignJWT, base64url, exportJWK, generateKeyPair } from 'jose';
import { generateKeyPairSync, sign as rsaSign } from 'node:crypto';
import { createBearerVerifier, discoveryUrl, githubClaimsFrom, InvalidTokenError, IssuerUnavailableError } from '../../src/server/auth/bearer';
import { startStubIssuer, githubClaims, type StubIssuer } from './stub-issuer';

const AUD = 'https://continuo.example.com';
let stub: StubIssuer | undefined;
afterEach(async () => { await stub?.close(); stub = undefined; });

async function setup(opts = {}) {
  stub = await startStubIssuer(AUD);
  return createBearerVerifier([{ issuer: stub.issuer, audience: AUD }], { cooldownMs: 0, ...opts });
}

describe('createBearerVerifier', () => {
  it('accepts a valid token and returns its payload', async () => {
    const v = await setup();
    const out = await v.verify(await stub!.sign(githubClaims()));
    expect(out.issuer).toBe(stub!.issuer);
    expect(out.payload.repository_id).toBe('812345678');
  });

  it('rejects a token from an untrusted issuer', async () => {
    const v = await setup();
    const other = await startStubIssuer(AUD);
    try {
      await expect(v.verify(await other.sign(githubClaims()))).rejects.toThrow(InvalidTokenError);
    } finally {
      await other.close();
    }
  });

  it('rejects the wrong audience', async () => {
    const v = await setup();
    await expect(v.verify(await stub!.sign(githubClaims(), { audience: 'https://other.example.com' }))).rejects.toThrow(InvalidTokenError);
  });

  it('rejects an expired token beyond the 60s tolerance', async () => {
    const v = await setup();
    const t = await stub!.sign(githubClaims(), { iatOffsetSeconds: -600, lifetimeSeconds: 300 });
    await expect(v.verify(t)).rejects.toThrow(InvalidTokenError);
  });

  it('rejects a token whose lifetime exceeds one hour', async () => {
    const v = await setup();
    await expect(v.verify(await stub!.sign(githubClaims(), { lifetimeSeconds: 3601 }))).rejects.toThrow(/lifetime/);
  });

  it('rejects alg none and HS256', async () => {
    const v = await setup();
    const header = base64url.encode(JSON.stringify({ alg: 'none' }));
    const body = base64url.encode(JSON.stringify({ ...githubClaims(), iss: stub!.issuer, aud: AUD, iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 300 }));
    await expect(v.verify(`${header}.${body}.`)).rejects.toThrow(InvalidTokenError);
    const hs = await new SignJWT(githubClaims())
      .setProtectedHeader({ alg: 'HS256' }).setIssuer(stub!.issuer).setAudience(AUD).setIssuedAt().setExpirationTime('5m')
      .sign(new TextEncoder().encode('a-shared-secret-of-sufficient-length!!'));
    await expect(v.verify(hs)).rejects.toThrow(InvalidTokenError);
  });

  it('rejects garbage', async () => {
    const v = await setup();
    await expect(v.verify('not-a-jwt')).rejects.toThrow(InvalidTokenError);
  });

  it('refetches the JWKS once when it sees an unknown kid (key rotation)', async () => {
    const v = await setup();
    await v.verify(await stub!.sign(githubClaims()));
    await stub!.rotateKey();
    await expect(v.verify(await stub!.sign(githubClaims()))).resolves.toBeDefined();
    expect(stub!.jwksFetches()).toBe(2);
  });

  it('reports an unreachable issuer as unavailable, not invalid', async () => {
    const v = await setup();
    const token = await stub!.sign(githubClaims());
    await stub!.close();
    const closedIssuer = stub!.issuer;
    stub = undefined;
    await expect(v.verify(token)).rejects.toThrow(IssuerUnavailableError);
    expect(closedIssuer).toMatch(/^http:/);
  });

  it('reports a failing JWKS endpoint as unavailable', async () => {
    const v = await setup();
    stub!.failJwks(true);
    await expect(v.verify(await stub!.sign(githubClaims()))).rejects.toThrow(IssuerUnavailableError);
  });

  it.each([
    ['HTML', '<html>bad gateway</html>'],
    ['a malformed key set', '{"keys": "nope"}'],
  ])('reports a 200 JWKS response carrying %s as unavailable', async (_n, body) => {
    const v = await setup();
    stub!.serveJwks({ kind: 'body', body });
    await expect(v.verify(await stub!.sign(githubClaims()))).rejects.toThrow(IssuerUnavailableError);
  });

  it('reports a JWKS body that stops arriving after the headers as unavailable', async () => {
    const v = await setup({ timeoutMs: 300 });
    const token = await stub!.sign(githubClaims());
    // Discovery succeeds; only the key set stalls.
    stub!.serveJwks({ kind: 'stall' });
    await expect(v.verify(token)).rejects.toThrow(IssuerUnavailableError);
  });

  it('rejects a token whose kid the issuer does not publish, after one refetch', async () => {
    const v = await setup();
    await v.verify(await stub!.sign(githubClaims()));
    await expect(v.verify(await stub!.sign(githubClaims(), { kid: 'unknown-kid' }))).rejects.toThrow(InvalidTokenError);
    expect(stub!.jwksFetches()).toBe(2);
  });

  it.each([
    ['an empty key set', async () => ({ keys: [] })],
    ['a key set with only an encryption key', async () => ({ keys: [{ ...(await exportJWK((await generateKeyPair('RS256')).publicKey)), kid: 'k1', use: 'enc' }] })],
    ['a key set with no RSA key', async () => ({ keys: [{ ...(await exportJWK((await generateKeyPair('ES256')).publicKey)), kid: 'k1' }] })],
  ])('reports %s as unavailable, not as an unknown kid', async (_n, set) => {
    const v = await setup();
    stub!.serveJwks({ kind: 'body', body: JSON.stringify(await set()) });
    await expect(v.verify(await stub!.sign(githubClaims()))).rejects.toThrow(IssuerUnavailableError);
  });

  it('reports a kid the issuer publishes twice as unavailable', async () => {
    const v = await setup();
    const twin = async () => ({ ...(await exportJWK((await generateKeyPair('RS256')).publicKey)), kid: 'k1', alg: 'RS256', use: 'sig' });
    stub!.serveJwks({ kind: 'body', body: JSON.stringify({ keys: [await twin(), await twin()] }) });
    await expect(v.verify(await stub!.sign(githubClaims()))).rejects.toThrow(IssuerUnavailableError);
  });

  it('rejects a token without a kid when the issuer publishes several keys', async () => {
    const v = await setup();
    const key = async (kid: string) => ({ ...(await exportJWK((await generateKeyPair('RS256')).publicKey)), kid, alg: 'RS256', use: 'sig' });
    stub!.serveJwks({ kind: 'body', body: JSON.stringify({ keys: [await key('a'), await key('b')] }) });
    await expect(v.verify(await stub!.sign(githubClaims(), { noKid: true }))).rejects.toThrow(InvalidTokenError);
  });

  it('reports a signing key shorter than 2048 bits as unavailable', async () => {
    const v = await setup();
    const { publicKey, privateKey } = generateKeyPairSync('rsa', { modulusLength: 1024 });
    stub!.serveJwks({ kind: 'body', body: JSON.stringify({ keys: [{ ...publicKey.export({ format: 'jwk' }), kid: 'weak', alg: 'RS256', use: 'sig' }] }) });
    const now = Math.floor(Date.now() / 1000);
    const header = base64url.encode(JSON.stringify({ alg: 'RS256', kid: 'weak' }));
    const body = base64url.encode(JSON.stringify({ ...githubClaims(), iss: stub!.issuer, aud: AUD, iat: now, exp: now + 300 }));
    const signature = base64url.encode(rsaSign('sha256', Buffer.from(`${header}.${body}`), privateKey));
    await expect(v.verify(`${header}.${body}.${signature}`)).rejects.toThrow(IssuerUnavailableError);
  });

  it('rejects a token issued more than 60s in the future, however short its lifetime', async () => {
    const v = await setup();
    await expect(v.verify(await stub!.sign(githubClaims(), { iatOffsetSeconds: 600 }))).rejects.toThrow(InvalidTokenError);
    await expect(v.verify(await stub!.sign(githubClaims(), { iatOffsetSeconds: 24 * 3600, lifetimeSeconds: 3600 }))).rejects.toThrow(InvalidTokenError);
  });

  it('accepts a token issued within the 60s clock tolerance', async () => {
    const v = await setup();
    await expect(v.verify(await stub!.sign(githubClaims(), { iatOffsetSeconds: 30 }))).resolves.toBeDefined();
  });

  it.each(['not-json', 'null-body'] as const)('reports a %s discovery body as unavailable and retries discovery afterwards', async (mode) => {
    const v = await setup();
    const token = await stub!.sign(githubClaims());
    stub!.failDiscovery(mode);
    await expect(v.verify(token)).rejects.toThrow(IssuerUnavailableError);
    stub!.failDiscovery('none');
    await expect(v.verify(token)).resolves.toBeDefined();
  });
});

describe('issuer identifier ending in a slash', () => {
  it('discovers at the single-slash path and verifies against the exact identifier', async () => {
    stub = await startStubIssuer(AUD, { trailingSlash: true });
    expect(stub.issuer.endsWith('/')).toBe(true);
    const v = createBearerVerifier([{ issuer: stub.issuer, audience: AUD }], { cooldownMs: 0 });
    const out = await v.verify(await stub.sign(githubClaims()));
    expect(out.issuer).toBe(stub.issuer);
    expect(stub.requestedPaths()[0]).toBe('/.well-known/openid-configuration');
    expect(stub.requestedPaths().some((p) => p.startsWith('//'))).toBe(false);
  });

  it('compares iss to the identifier exactly, with or without the trailing slash', async () => {
    stub = await startStubIssuer(AUD, { trailingSlash: true });
    const withSlash = createBearerVerifier([{ issuer: stub.issuer, audience: AUD }], { cooldownMs: 0 });
    const bare = stub.issuer.replace(/\/$/, '');
    await expect(withSlash.verify(await stub.sign(githubClaims(), { issuer: bare }))).rejects.toThrow(InvalidTokenError);

    const noSlash = createBearerVerifier([{ issuer: bare, audience: AUD }], { cooldownMs: 0 });
    await expect(noSlash.verify(await stub.sign(githubClaims()))).rejects.toThrow(InvalidTokenError);
    await expect(noSlash.verify(await stub.sign(githubClaims()))).rejects.toThrow(/untrusted issuer/);
  });

  it('builds the discovery URL without a double slash', () => {
    expect(discoveryUrl('https://idp.example.com/')).toBe('https://idp.example.com/.well-known/openid-configuration');
    expect(discoveryUrl('https://idp.example.com/tenant/')).toBe('https://idp.example.com/tenant/.well-known/openid-configuration');
    expect(discoveryUrl('https://token.actions.githubusercontent.com')).toBe('https://token.actions.githubusercontent.com/.well-known/openid-configuration');
  });
});

describe('githubClaimsFrom', () => {
  it('extracts the GitHub claims', () => {
    expect(githubClaimsFrom(githubClaims())).toMatchObject({ repositoryId: '812345678', repository: 'carolsimone/continuo-demo', sha: 'abc1234def', ref: 'refs/heads/main', refProtected: 'true', environment: '' });
  });
  it.each(['repository_id', 'repository', 'sha'])('rejects a token without %s', (k) => {
    expect(() => githubClaimsFrom(githubClaims({ [k]: undefined }))).toThrow(InvalidTokenError);
  });
  it('rejects a non-digit repository_id', () => {
    expect(() => githubClaimsFrom(githubClaims({ repository_id: 'abc' }))).toThrow(InvalidTokenError);
  });
});
