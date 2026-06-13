import type { ErrorRequestHandler, RequestHandler } from 'express';
import { parse as parseCookies } from 'cookie';
import { toAuthUser, type SessionStore } from './session';
import { audit } from './audit';
import { DEV_USER, SESSION_COOKIE } from './types';

const MUTATING = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

// AUTH_MODE=dev: every request carries the fixed development identity.
export function devIdentity(): RequestHandler {
  return (req, _res, next) => {
    req.user = DEV_USER;
    next();
  };
}

// AUTH_MODE=oidc: resolve the opaque session cookie via Redis, attach the user.
// A store failure goes to the error handler — never an unauthenticated pass.
export function sessionAuth(sessions: SessionStore): RequestHandler {
  return (req, _res, next) => {
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

// Gate for everything mounted under /api: reads need any authenticated user,
// mutations need the operator role. Method-based, so new endpoints are safe
// by default.
export function requireApiAuth(): RequestHandler {
  return (req, res, next) => {
    if (!req.user) {
      res.status(401).json({ error: 'sign in required', code: 'unauthenticated' });
      return;
    }
    if (MUTATING.has(req.method) && req.user.role !== 'operator') {
      audit('role_denied', {
        user_id: req.user.userId, email: req.user.email, role: req.user.role,
        method: req.method, path: req.originalUrl, outcome: 'forbidden',
      });
      res.status(403).json({ error: 'operator role required', code: 'forbidden' });
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
      audit('api_mutation', {
        user_id: req.user?.userId, email: req.user?.email, role: req.user?.role,
        method: req.method, path: req.originalUrl, outcome: res.statusCode,
      });
    });
    next();
  };
}

// Fail closed: a session-store error (Redis unreachable) becomes 503, never an
// unauthenticated pass-through.
export function authErrorHandler(): ErrorRequestHandler {
  return (err, _req, res, _next) => {
    console.error('auth error:', err);
    res.status(503).json({ error: { code: 'auth_unavailable', message: 'authentication backend unavailable' } });
  };
}
