import Redis from 'ioredis';
import type { IncomingMessage } from 'http';
import { parse as parseCookies } from 'cookie';
import type { AuthConfig } from './config';
import { discoverOidc } from './oidc';
import { createBearerVerifier } from './bearer';
import type { CiAuthConfig } from './ci-config';
import {
  authErrorHandler,
  auditMutations,
  bearerAuth,
  csrfOriginCheck,
  devIdentity,
  requireApiAuth,
  sessionAuth,
} from './middleware';
import { createAuthRouter, createDevAuthRouter } from './routes';
import { SessionStore, toAuthUser } from './session';
import { DEV_USER, SESSION_COOKIE, type AppAuth, type AuthUser } from './types';

export interface BuiltAuth {
  app: AppAuth;
  // Session check for the WebSocket upgrade; null means reject.
  authenticateWs: (req: IncomingMessage) => Promise<AuthUser | null>;
  // The deployment origin (e.g. "https://app.example.com") used to validate the
  // browser Origin header on WebSocket upgrades. Undefined in dev mode, which
  // disables the cross-origin check entirely.
  publicOrigin?: string;
}

// A CI config with no bindings grants nothing, so its issuer is not trusted
// at all: a token from it is an untrusted-issuer 401 and the ui never fetches
// that issuer's discovery document.
function trustedCiConfig(ci: CiAuthConfig | null): CiAuthConfig | null {
  if (!ci) return null;
  const bindingCount = [...ci.bindings.values()].reduce((n, list) => n + list.length, 0);
  if (bindingCount === 0) {
    console.log(`CI auth is configured but has no bindings; tokens from ${ci.issuer} are not accepted`);
    return null;
  }
  console.log(`CI auth: issuer ${ci.issuer}, ${ci.bindings.size} bound service(s)`);
  return ci;
}

export async function buildAuth(cfg: AuthConfig, configuredCi: CiAuthConfig | null = null): Promise<BuiltAuth> {
  if (cfg.mode !== 'dev' && configuredCi && configuredCi.issuer === cfg.issuerUrl.replace(/\/+$/, '')) {
    throw new Error('ciAuth.issuer must differ from the login issuer (AUTH_OIDC_ISSUER_URL)');
  }
  const ci = trustedCiConfig(configuredCi);
  if (cfg.mode === 'dev') {
    console.warn('AUTH_MODE=dev — development-only placeholder identity; NEVER use in production');
    const verifier = createBearerVerifier(ci ? [{ issuer: ci.issuer, audience: ci.audience }] : []);
    return {
      app: {
        authn: [bearerAuth({ verifier, ci, login: null }), devIdentity()],
        apiGuards: [requireApiAuth(), auditMutations()],
        router: createDevAuthRouter(),
        errorHandler: authErrorHandler(),
      },
      authenticateWs: async () => DEV_USER,
      publicOrigin: undefined,
    };
  }

  const verifier = createBearerVerifier([
    ...(ci ? [{ issuer: ci.issuer, audience: ci.audience }] : []),
    { issuer: cfg.issuerUrl, audience: cfg.clientId },
  ]);
  const redis = new Redis(cfg.redisUrl);
  const sessions = new SessionStore(redis, cfg.sessionIdleTtlSeconds, cfg.sessionMaxTtlSeconds);
  const flow = await discoverOidc(cfg);
  const origin = new URL(cfg.publicUrl).origin;

  return {
    app: {
      authn: [bearerAuth({ verifier, ci, login: { issuer: cfg.issuerUrl, roles: cfg } }), sessionAuth(sessions)],
      apiGuards: [csrfOriginCheck(origin), requireApiAuth(), auditMutations()],
      router: createAuthRouter({ flow, sessions, cfg }),
      errorHandler: authErrorHandler(),
    },
    authenticateWs: async (req) => {
      const id = parseCookies(req.headers.cookie ?? '')[SESSION_COOKIE] ?? '';
      const record = await sessions.load(id);
      return record ? toAuthUser(record) : null;
    },
    publicOrigin: origin,
  };
}
