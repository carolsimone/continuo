import { describe, it, expect, afterEach } from 'vitest';
import { SignJWT, base64url } from 'jose';
import { createBearerVerifier, InvalidTokenError, IssuerUnavailableError } from '../../src/server/auth/bearer';
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
});
