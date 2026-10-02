import type { ErrorRequestHandler, Request, RequestHandler } from 'express';
import { parse as parseCookies } from 'cookie';
import { toAuthUser, type SessionStore } from './session';
import { audit } from './audit';
import { DEV_USER, SESSION_COOKIE } from './types';
import { resolveRole } from './roles';
import { authorize } from './authorize';
import { principalAuditFields, resolveGrants, type Principal } from './principal';
import { principalOf } from './request-principal';
import { githubClaimsFrom, InvalidTokenError, IssuerUnavailableError, type BearerVerifier } from './bearer';
import type { CiAuthConfig } from './ci-config';
import type { OidcAuthConfig } from './config';

const MUTATING = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

// The Authorization header is a credential only for the API. Elsewhere (the
// SPA, /auth/*) it is ignored, so a header an ingress basic-auth or an auth
// proxy forwards never locks a signed-in user out of the dashboard. Matched
// case-insensitively, as Express routes /API/... to the /api mount.
const API_PATH = /^\/api(?:\/|$)/i;
function bearerApplies(req: Request): boolean {
  return req.headers.authorization !== undefined && API_PATH.test(req.path);
}

// AUTH_MODE=dev: every request carries the fixed development identity.
export function devIdentity(): RequestHandler {
  return (req, _res, next) => {
    // An /api request carrying Authorization is authenticated by bearerAuth alone.
    if (bearerApplies(req)) return next();
    req.user = DEV_USER;
    next();
  };
}

// AUTH_MODE=oidc: resolve the opaque session cookie via Redis, attach the user.
// A store failure goes to the error handler — never an unauthenticated pass.
export function sessionAuth(sessions: SessionStore): RequestHandler {
  return (req, _res, next) => {
    // An /api request carrying Authorization is authenticated by bearerAuth alone.
    if (bearerApplies(req)) return next();
    const cookies = parseCookies(req.headers.cookie ?? '');
    const id = cookies[SESSION_COOKIE];
    if (!id) return next();
    sessions
      .load(id)
      .then((record) => {
        if (record) {
          req.user = toAuthUser(record);
        }
        next();
      })
      .catch(next);
  };
}

export interface BearerDeps {
  verifier: BearerVerifier;
  ci: CiAuthConfig | null;
  login: { issuer: string; roles: Pick<OidcAuthConfig, 'groupsClaim' | 'roleMapping' | 'operatorEmails' | 'viewerEmails' | 'defaultRole'> } | null;
}

class NoRoleError extends Error {}

const BEARER = /^bearer\s+(\S+)\s*$/i;

function rejectToken(res: Parameters<RequestHandler>[1], detail: string): void {
  res.setHeader('WWW-Authenticate', 'Bearer error="invalid_token"');
  res.status(401).json({ error: `invalid bearer token: ${detail}`, code: 'invalid_token' });
}

// Authenticates an /api request that carries an Authorization header. When the
// header is present it is the only credential considered: a token that fails
// here is a 401, never a fallback to the session cookie or the dev identity,
// and every such 401 is audited as bearer_rejected. A token from the CI issuer
// becomes a ci principal; one from the ui's login issuer becomes a human with
// the role resolveRole assigns. Outside /api the header is ignored.
export function bearerAuth(deps: BearerDeps): RequestHandler {
  async function resolve(token: string): Promise<Principal> {
    const { issuer, payload } = await deps.verifier.verify(token);
    const claims = payload as Record<string, unknown>;
    if (deps.ci && issuer === deps.ci.issuer) {
      const gh = githubClaimsFrom(claims);
      return { kind: 'ci', subject: String(claims.sub ?? ''), claims: gh, grants: resolveGrants(gh, deps.ci.bindings) };
    }
    if (deps.login && issuer === deps.login.issuer) {
      const email = typeof claims.email === 'string' ? claims.email : '';
      const role = resolveRole(claims, email, deps.login.roles);
      if (role === 'none') throw new NoRoleError(email);
      const sub = String(claims.sub ?? '');
      const name = typeof claims.name === 'string' ? claims.name : email || sub;
      return { kind: 'human', user: { userId: `${new URL(deps.login.issuer).host}|${sub}`, email, name, role } };
    }
    throw new InvalidTokenError('untrusted issuer');
  }

  return (req, res, next) => {
    if (!bearerApplies(req)) return next();
    const m = BEARER.exec(req.headers.authorization!);
    if (!m) {
      audit('bearer_rejected', { method: req.method, path: req.originalUrl, reason: 'malformed authorization header', outcome: 'unauthorized' });
      rejectToken(res, 'expected "Authorization: Bearer <token>"');
      return;
    }
    resolve(m[1]).then(
      (principal) => {
        req.principal = principal;
        if (principal.kind === 'human') req.user = principal.user;
        next();
      },
      (err: unknown) => {
        if (err instanceof InvalidTokenError) {
          audit('bearer_rejected', { method: req.method, path: req.originalUrl, reason: err.message, outcome: 'unauthorized' });
          rejectToken(res, err.message);
        } else if (err instanceof NoRoleError) {
          audit('role_denied', { email: err.message, method: req.method, path: req.originalUrl, outcome: 'forbidden' });
          res.status(403).json({ error: 'no role assigned to this identity', code: 'forbidden' });
        } else if (err instanceof IssuerUnavailableError) {
          console.error('bearer auth:', err.message);
          res.status(503).json({ error: 'token issuer unavailable', code: 'auth_unavailable' });
        } else {
          next(err);
        }
      },
    );
  };
}

// The public release routes (paths relative to the /api mount). They authorize
// per action against the resource they touch in routes/v1.ts, so this gate only
// requires a principal for them.
const PER_ACTION_ROUTES: ReadonlyArray<{ method: string; path: RegExp }> = [
  { method: 'POST', path: /^\/v1\/releases\/?$/ },
  { method: 'GET', path: /^\/v1\/releases\/[^/]+\/?$/ },
  { method: 'GET', path: /^\/v1\/current-prod\/?$/ },
];

// Gate for everything mounted under /api. Apart from the routes in
// PER_ACTION_ROUTES, every route is a method-based read/mutate decision, so any
// other endpoint (including any other /api/v1 one) is safe by default and
// closed to CI tokens.
export function requireApiAuth(): RequestHandler {
  return (req, res, next) => {
    const p = principalOf(req);
    if (!p) {
      res.status(401).json({ error: 'sign in required', code: 'unauthenticated' });
      return;
    }
    if (PER_ACTION_ROUTES.some((r) => r.method === req.method && r.path.test(req.path))) return next();
    const d = authorize(p, { kind: MUTATING.has(req.method) ? 'api.mutate' : 'api.read' });
    if (!d.allow) {
      audit('role_denied', { ...principalAuditFields(p), method: req.method, path: req.originalUrl, outcome: 'forbidden' });
      res.status(403).json({ error: d.reason, code: 'forbidden' });
      return;
    }
    next();
  };
}

// Second CSRF layer on top of SameSite=Lax: a mutating request that carries a
// browser Origin header must come from our own origin. Requests without an
// Origin header (curl, Go test clients) carry no ambient cross-site cookie and
// are not CSRF-able, so they pass through to the auth gate.
export function csrfOriginCheck(publicOrigin: string): RequestHandler {
  return (req, res, next) => {
    if (!MUTATING.has(req.method)) return next();
    const origin = req.headers.origin;
    if (origin && origin !== publicOrigin) {
      audit('csrf_rejected', { method: req.method, path: req.originalUrl, origin, outcome: 'forbidden' });
      res.status(403).json({ error: 'cross-origin request rejected', code: 'csrf_rejected' });
      return;
    }
    next();
  };
}

// One audit line per mutating /api call, emitted when the response finishes so
// the outcome (status code) is known.
export function auditMutations(): RequestHandler {
  return (req, res, next) => {
    if (!MUTATING.has(req.method)) return next();
    res.on('finish', () => {
      const p = principalOf(req);
      audit('api_mutation', {
        ...(p ? principalAuditFields(p) : {}),
        method: req.method, path: req.originalUrl, outcome: res.statusCode,
      });
    });
    next();
  };
}

// Terminal error handler. Client errors from upstream middleware (e.g. malformed
// JSON rejected by express.json() with status 400) are preserved at their own
// status. Only plain Errors with no HTTP status — or 5xx errors — indicate a
// genuine auth-backend failure (Redis/IdP unreachable) and become 503.
export function authErrorHandler(): ErrorRequestHandler {
  return (err, _req, res, next) => {
    if (res.headersSent) {
      next(err);
      return;
    }
    const status =
      typeof (err as { status?: unknown })?.status === 'number'
        ? (err as { status: number }).status
        : typeof (err as { statusCode?: unknown })?.statusCode === 'number'
          ? (err as { statusCode: number }).statusCode
          : undefined;
    if (status !== undefined && status < 500) {
      // A client error surfaced by upstream middleware (e.g. malformed JSON
      // rejected by express.json(), which carries status 400). Preserve its
      // status; only reveal the message when the error marks itself safe.
      const expose = (err as { expose?: unknown })?.expose === true;
      res.status(status).json({ error: expose ? (err as Error).message : 'invalid request', code: 'bad_request' });
      return;
    }
    // No HTTP status (or a 5xx) → treat as an auth-backend failure: Redis or the
    // identity provider is unreachable. Fail closed with 503.
    console.error('auth error:', err);
    res.status(503).json({ error: 'authentication backend unavailable', code: 'auth_unavailable' });
  };
}
