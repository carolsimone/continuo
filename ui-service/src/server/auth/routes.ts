import { Router } from 'express';
import { parse as parseCookies } from 'cookie';
import type { OidcAuthConfig } from './config';
import type { OidcFlow, PendingAuth } from './oidc';
import { resolveRole } from './roles';
import type { SessionStore } from './session';
import { audit } from './audit';
import { DEV_USER, PENDING_COOKIE, SESSION_COOKIE } from './types';

// returnTo must stay inside our own origin: a relative path, never
// protocol-relative ("//evil.com") or absolute.
export function safeReturnTo(raw: unknown): string {
  if (typeof raw !== 'string' || !raw.startsWith('/') || raw.startsWith('//')) return '/';
  return raw;
}

export function createAuthRouter(deps: {
  flow: OidcFlow;
  sessions: SessionStore;
  cfg: OidcAuthConfig;
}): Router {
  const { flow, sessions, cfg } = deps;
  // Secure cookies only flow over https; plain-http deployments (local dev
  // against Dex, e2e) must omit the flag or the browser/jar drops the cookie.
  const baseCookie = {
    httpOnly: true,
    secure: cfg.publicUrl.startsWith('https://'),
    sameSite: 'lax' as const,
    path: '/',
  };
  const router = Router();

  router.get('/login', async (req, res, next) => {
    try {
      const returnTo = safeReturnTo(req.query.returnTo);
      const { url, pending } = await flow.buildLoginRedirect(returnTo);
      res.cookie(PENDING_COOKIE, Buffer.from(JSON.stringify(pending)).toString('base64url'), {
        ...baseCookie,
        maxAge: 10 * 60 * 1000,
      });
      res.redirect(url);
    } catch (err) {
      next(err);
    }
  });

  router.get('/callback', async (req, res, next) => {
    const rawPending = parseCookies(req.headers.cookie ?? '')[PENDING_COOKIE];
    res.clearCookie(PENDING_COOKIE, baseCookie);
    if (!rawPending) {
      audit('login_failed', { reason: 'missing_pending_state', outcome: 'rejected' });
      res.redirect('/?auth_error=login_failed');
      return;
    }
    let identity: Awaited<ReturnType<OidcFlow['handleCallback']>>;
    let returnTo: string;
    try {
      const pending = JSON.parse(Buffer.from(rawPending, 'base64url').toString()) as PendingAuth;
      returnTo = safeReturnTo(pending.returnTo);
      const currentUrl = new URL(req.originalUrl, cfg.publicUrl);
      identity = await flow.handleCallback(currentUrl, pending);
    } catch (err) {
      audit('login_failed', { reason: String(err), outcome: 'rejected' });
      res.redirect('/?auth_error=login_failed');
      return;
    }
    const role = resolveRole(identity.claims, identity.email, cfg);
    if (role === 'none') {
      audit('login_denied', { user_id: identity.userId, email: identity.email, outcome: 'no_role' });
      res.redirect('/?auth_error=no_role');
      return;
    }
    try {
      const sessionId = await sessions.create({
        userId: identity.userId,
        email: identity.email,
        name: identity.name,
        role,
      });
      res.cookie(SESSION_COOKIE, sessionId, { ...baseCookie, maxAge: cfg.sessionMaxTtlSeconds * 1000 });
      audit('login', { user_id: identity.userId, email: identity.email, role, outcome: 'success' });
      res.redirect(returnTo);
    } catch (err) {
      next(err);
    }
  });

  router.post('/logout', async (req, res, next) => {
    try {
      const id = parseCookies(req.headers.cookie ?? '')[SESSION_COOKIE] ?? '';
      await sessions.destroy(id);
      res.clearCookie(SESSION_COOKIE, baseCookie);
      if (req.user) audit('logout', { user_id: req.user.userId, email: req.user.email, outcome: 'success' });
      res.json({ redirectTo: flow.endSessionUrl(cfg.publicUrl) ?? '/' });
    } catch (err) {
      next(err);
    }
  });

  router.get('/me', (req, res) => {
    if (!req.user) {
      res.status(401).json({ error: { code: 'unauthenticated', message: 'sign in required' } });
      return;
    }
    res.json(req.user);
  });

  return router;
}

// AUTH_MODE=dev: the same /auth surface with the fixed identity and no IdP.
export function createDevAuthRouter(): Router {
  const router = Router();
  router.get('/login', (_req, res) => res.redirect('/'));
  router.post('/logout', (_req, res) => res.json({ redirectTo: '/' }));
  router.get('/me', (_req, res) => res.json(DEV_USER));
  return router;
}
