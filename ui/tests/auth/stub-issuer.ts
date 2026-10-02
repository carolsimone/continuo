import { createServer, type Server } from 'http';
import type { AddressInfo } from 'net';
import { SignJWT, exportJWK, generateKeyPair, type JWK } from 'jose';

export type DiscoveryFailure = 'none' | 'not-json' | 'null-body';
// How the JWKS endpoint answers: normally, 500, 200 with a fixed body, or 200
// headers followed by a body that never finishes.
export type JwksMode = { kind: 'ok' } | { kind: 'error' } | { kind: 'body'; body: string } | { kind: 'stall' };

type KeyPair = Awaited<ReturnType<typeof generateKeyPair>>;

export interface StubIssuer {
  issuer: string;
  sign(claims: Record<string, unknown>, opts?: { audience?: string; lifetimeSeconds?: number; iatOffsetSeconds?: number; kid?: string; noKid?: boolean; issuer?: string }): Promise<string>;
  rotateKey(): Promise<void>;
  jwksFetches(): number;
  discoveryFetches(): number;
  failJwks(fail: boolean): void;
  serveJwks(mode: JwksMode): void;
  // Makes the discovery document answer 200 with a body that is not a usable JSON object.
  failDiscovery(mode: DiscoveryFailure): void;
  // The raw request paths the issuer has received, in order.
  requestedPaths(): string[];
  close(): Promise<void>;
}

export interface StubIssuerOptions {
  // Ends the issuer identifier with "/", as some identity providers do. The
  // endpoints stay at the single-slash paths under the origin.
  trailingSlash?: boolean;
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

export async function startStubIssuer(defaultAudience: string, opts: StubIssuerOptions = {}): Promise<StubIssuer> {
  let keys: KeyPair = await generateKeyPair('RS256');
  let kid = 'k1';
  let jwk: JWK = { ...(await exportJWK(keys.publicKey)), kid, alg: 'RS256', use: 'sig' };
  let fetches = 0;
  let discoveries = 0;
  let jwksMode: JwksMode = { kind: 'ok' };
  let discoveryFailure: DiscoveryFailure = 'none';
  let issuer = '';
  let origin = '';
  const paths: string[] = [];

  const server: Server = createServer((req, res) => {
    paths.push(req.url ?? '');
    const url = new URL(req.url ?? '/', origin);
    if (url.pathname === '/.well-known/openid-configuration') {
      discoveries++;
      if (discoveryFailure !== 'none') {
        res.setHeader('content-type', 'application/json');
        res.end(discoveryFailure === 'not-json' ? '<html>bad gateway</html>' : 'null');
        return;
      }
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ issuer, jwks_uri: `${origin}/.well-known/jwks` }));
      return;
    }
    if (url.pathname === '/.well-known/jwks') {
      fetches++;
      if (jwksMode.kind === 'error') {
        res.statusCode = 500;
        res.end();
        return;
      }
      if (jwksMode.kind === 'body') {
        res.setHeader('content-type', 'application/json');
        res.end(jwksMode.body);
        return;
      }
      if (jwksMode.kind === 'stall') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.write('{"keys":');
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
  origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  issuer = opts.trailingSlash ? `${origin}/` : origin;

  return {
    issuer,
    async sign(claims, opts = {}) {
      const iat = Math.floor(Date.now() / 1000) + (opts.iatOffsetSeconds ?? 0);
      return new SignJWT({ ...claims })
        .setProtectedHeader(opts.noKid ? { alg: 'RS256' } : { alg: 'RS256', kid: opts.kid ?? kid })
        .setIssuer(opts.issuer ?? issuer)
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
    failJwks: (f) => { jwksMode = f ? { kind: 'error' } : { kind: 'ok' }; },
    serveJwks: (m) => { jwksMode = m; },
    failDiscovery: (m) => { discoveryFailure = m; },
    requestedPaths: () => [...paths],
    close: () => new Promise<void>((resolve) => {
      server.close(() => resolve());
      // A stalled JWKS response holds its connection open.
      server.closeAllConnections();
    }),
  };
}
