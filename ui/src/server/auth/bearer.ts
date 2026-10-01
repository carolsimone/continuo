import { createRemoteJWKSet, decodeJwt, errors, jwtVerify, type JWTPayload, type JWTVerifyGetKey } from 'jose';
import type { GithubClaims } from './principal';

export const MAX_TOKEN_LIFETIME_SECONDS = 3600;
const CLOCK_TOLERANCE_SECONDS = 60;

export interface TrustedIssuer {
  issuer: string; // compared to the token's iss exactly
  audience: string;
}

export interface VerifiedToken {
  issuer: string;
  payload: JWTPayload;
}

// The token is malformed, from an untrusted issuer, mis-addressed, expired,
// too long-lived, or badly signed. Answered with 401.
export class InvalidTokenError extends Error {}

// The issuer's discovery document or signing keys cannot be fetched. Answered
// with 503: an outage at the issuer never becomes a pass or a 401.
export class IssuerUnavailableError extends Error {}

// Reads the GitHub Actions claims continuo uses from a verified token's
// payload. A token without a numeric repository_id, a repository or a sha
// cannot be bound to a service, so it is an invalid token.
export function githubClaimsFrom(payload: Record<string, unknown>): GithubClaims {
  const str = (k: string) => (typeof payload[k] === 'string' ? (payload[k] as string) : '');
  const claims: GithubClaims = {
    repositoryId: str('repository_id'),
    repository: str('repository'),
    sha: str('sha'),
    ref: str('ref'),
    refProtected: str('ref_protected'),
    environment: str('environment'),
    workflowRef: str('workflow_ref'),
    runId: str('run_id'),
  };
  if (!/^[0-9]+$/.test(claims.repositoryId) || !claims.repository || !claims.sha) {
    throw new InvalidTokenError('GitHub token lacks repository_id, repository or sha');
  }
  return claims;
}

export interface BearerVerifier {
  verify(token: string): Promise<VerifiedToken>;
}

// Errors the key resolver raises because of the token rather than the key
// set: no key matches its kid, several do, or its alg has no JWK form.
function isTokenKeySelectionError(err: unknown): boolean {
  return err instanceof errors.JWKSNoMatchingKey
    || err instanceof errors.JWKSMultipleMatchingKeys
    || err instanceof errors.JOSENotSupported;
}

// Wraps the remote key set so an error is classified by where it came from.
// Anything the resolver throws while fetching, reading or parsing the key set
// (a timeout, a refused connection, a non-200, a body that is not a JWK set)
// is an issuer outage; only a key-selection failure caused by the token stays
// a token error. jwtVerify's own signature and claim checks run outside this
// wrapper and are always token errors.
function classifyingKeyResolver(remote: JWTVerifyGetKey, issuer: string): JWTVerifyGetKey {
  return async (header, token) => {
    try {
      return await remote(header, token);
    } catch (err) {
      if (isTokenKeySelectionError(err)) throw err;
      throw new IssuerUnavailableError(`signing keys for ${issuer} unavailable: ${String(err)}`);
    }
  };
}

// The discovery document lives at <issuer>/.well-known/openid-configuration.
// An issuer identifier may end in "/" (OIDC allows it), so trailing slashes
// are dropped here, and only here: the identifier itself is compared to the
// token's iss and to the discovery document's issuer exactly as configured.
export function discoveryUrl(issuer: string): string {
  return `${issuer.replace(/\/+$/, '')}/.well-known/openid-configuration`;
}

export function createBearerVerifier(
  trusted: TrustedIssuer[],
  opts: { cooldownMs?: number; timeoutMs?: number } = {},
): BearerVerifier {
  const timeoutMs = opts.timeoutMs ?? 5000;
  const byIssuer = new Map(trusted.map((t) => [t.issuer, t]));
  const keySets = new Map<string, Promise<JWTVerifyGetKey>>();

  async function discoverKeys(issuer: string): Promise<JWTVerifyGetKey> {
    // The request, the body read (which carries its own timeout) and the shape
    // check all classify as an issuer outage: a proxy answering 200 with HTML
    // or `null` is an unavailable issuer, not a bad token.
    let meta: { issuer?: unknown; jwks_uri?: unknown } | null;
    try {
      const resp = await fetch(discoveryUrl(issuer), { signal: AbortSignal.timeout(timeoutMs) });
      if (!resp.ok) throw new IssuerUnavailableError(`discovery for ${issuer} returned ${resp.status}`);
      meta = (await resp.json()) as { issuer?: unknown; jwks_uri?: unknown } | null;
    } catch (err) {
      if (err instanceof IssuerUnavailableError) throw err;
      throw new IssuerUnavailableError(`discovery for ${issuer} failed: ${String(err)}`);
    }
    if (meta?.issuer !== issuer || typeof meta.jwks_uri !== 'string') {
      throw new IssuerUnavailableError(`discovery for ${issuer} returned a different issuer or no jwks_uri`);
    }
    // createRemoteJWKSet caches keys and refetches once, rate-limited by the
    // cooldown, when a token names a kid it has not seen (key rotation).
    const remote = createRemoteJWKSet(new URL(meta.jwks_uri), {
      cooldownDuration: opts.cooldownMs ?? 30_000,
      timeoutDuration: timeoutMs,
    });
    return classifyingKeyResolver(remote, issuer);
  }

  // Discovery runs on the first bearer for an issuer, so an unreachable issuer
  // never blocks the ui from booting. A failed discovery is retried next time.
  function keysFor(issuer: string): Promise<JWTVerifyGetKey> {
    let p = keySets.get(issuer);
    if (!p) {
      p = discoverKeys(issuer);
      keySets.set(issuer, p);
      p.catch(() => keySets.delete(issuer));
    }
    return p;
  }

  return {
    async verify(token) {
      let iss: unknown;
      try {
        iss = decodeJwt(token).iss;
      } catch {
        throw new InvalidTokenError('malformed token');
      }
      const spec = typeof iss === 'string' ? byIssuer.get(iss) : undefined;
      if (!spec) throw new InvalidTokenError('untrusted issuer');
      const keys = await keysFor(spec.issuer);
      let payload: JWTPayload;
      try {
        ({ payload } = await jwtVerify(token, keys, {
          issuer: spec.issuer,
          audience: spec.audience,
          algorithms: ['RS256'],
          clockTolerance: CLOCK_TOLERANCE_SECONDS,
          requiredClaims: ['exp', 'iat'],
        }));
      } catch (err) {
        if (err instanceof IssuerUnavailableError) throw err;
        throw new InvalidTokenError(err instanceof Error ? err.message : 'invalid token');
      }
      // jwtVerify does not bound iat, and a far-future iat would stretch the
      // one-hour cap into a long-lived token.
      if ((payload.iat as number) > Math.floor(Date.now() / 1000) + CLOCK_TOLERANCE_SECONDS) {
        throw new InvalidTokenError('token issued in the future');
      }
      if ((payload.exp as number) - (payload.iat as number) > MAX_TOKEN_LIFETIME_SECONDS) {
        throw new InvalidTokenError('token lifetime exceeds 1 hour');
      }
      return { issuer: spec.issuer, payload };
    },
  };
}
