import { createServer, type Server } from 'http';
import type { AddressInfo } from 'net';
import { SignJWT, exportJWK, generateKeyPair, type JWK } from 'jose';

export type DiscoveryFailure = 'none' | 'not-json' | 'null-body';

type KeyPair = Awaited<ReturnType<typeof generateKeyPair>>;

export interface StubIssuer {
  issuer: string;
  sign(claims: Record<string, unknown>, opts?: { audience?: string; lifetimeSeconds?: number; iatOffsetSeconds?: number }): Promise<string>;
  rotateKey(): Promise<void>;
  jwksFetches(): number;
  discoveryFetches(): number;
  failJwks(fail: boolean): void;
  // Makes the discovery document answer 200 with a body that is not a usable JSON object.
  failDiscovery(mode: DiscoveryFailure): void;
  close(): Promise<void>;
}

// GitHub Actions-shaped claims for a push to main of repository 812345678.
export function githubClaims(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    sub: 'repo:carolsimone/continuo-demo:ref:refs/heads/main',
    repository_id: '812345678',
    repository: 'carolsimone/continuo-demo',
    sha: 'abc1234def',
    ref: 'refs/heads/main',
    ref_protected: 'true',
    workflow_ref: 'carolsimone/continuo-demo/.github/workflows/release.yml@refs/heads/main',
    run_id: '42',
    ...overrides,
  };
}

export async function startStubIssuer(defaultAudience: string): Promise<StubIssuer> {
  let keys: KeyPair = await generateKeyPair('RS256');
  let kid = 'k1';
  let jwk: JWK = { ...(await exportJWK(keys.publicKey)), kid, alg: 'RS256', use: 'sig' };
  let fetches = 0;
  let discoveries = 0;
  let failing = false;
  let discoveryFailure: DiscoveryFailure = 'none';
  let issuer = '';

  const server: Server = createServer((req, res) => {
    const url = new URL(req.url ?? '/', issuer);
    if (url.pathname === '/.well-known/openid-configuration') {
      discoveries++;
      if (discoveryFailure !== 'none') {
        res.setHeader('content-type', 'application/json');
        res.end(discoveryFailure === 'not-json' ? '<html>bad gateway</html>' : 'null');
        return;
      }
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ issuer, jwks_uri: `${issuer}/.well-known/jwks` }));
      return;
    }
    if (url.pathname === '/.well-known/jwks') {
      fetches++;
      if (failing) {
        res.statusCode = 500;
        res.end();
        return;
      }
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ keys: [jwk] }));
      return;
    }
    res.statusCode = 404;
    res.end();
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  issuer = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

  return {
    issuer,
    async sign(claims, opts = {}) {
      const iat = Math.floor(Date.now() / 1000) + (opts.iatOffsetSeconds ?? 0);
      return new SignJWT({ ...claims })
        .setProtectedHeader({ alg: 'RS256', kid })
        .setIssuer(issuer)
        .setAudience(opts.audience ?? defaultAudience)
        .setIssuedAt(iat)
        .setExpirationTime(iat + (opts.lifetimeSeconds ?? 300))
        .sign(keys.privateKey);
    },
    async rotateKey() {
      keys = await generateKeyPair('RS256');
      kid = `k${Date.now()}`;
      jwk = { ...(await exportJWK(keys.publicKey)), kid, alg: 'RS256', use: 'sig' };
    },
    jwksFetches: () => fetches,
    discoveryFetches: () => discoveries,
    failJwks: (f) => { failing = f; },
    failDiscovery: (m) => { discoveryFailure = m; },
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}
