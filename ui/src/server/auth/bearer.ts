import { createRemoteJWKSet, decodeJwt, errors, jwtVerify, type JWTPayload, type JWTVerifyGetKey } from 'jose';

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

export interface BearerVerifier {
  verify(token: string): Promise<VerifiedToken>;
}

// jose reports a non-200 JWKS response as a generic JOSEError; network
// failures surface as the fetch TypeError.
function isKeyFetchFailure(err: unknown): boolean {
  if (err instanceof errors.JWKSTimeout || err instanceof TypeError) return true;
  return err instanceof errors.JOSEError && /Expected 200 OK/i.test(err.message);
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
      const resp = await fetch(`${issuer}/.well-known/openid-configuration`, { signal: AbortSignal.timeout(timeoutMs) });
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
    return createRemoteJWKSet(new URL(meta.jwks_uri), {
      cooldownDuration: opts.cooldownMs ?? 30_000,
      timeoutDuration: timeoutMs,
    });
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
        if (isKeyFetchFailure(err)) throw new IssuerUnavailableError(`signing keys for ${spec.issuer} unavailable: ${String(err)}`);
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
