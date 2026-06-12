import Redis from 'ioredis';
import type { IncomingMessage } from 'http';
import { parse as parseCookies } from 'cookie';
import type { AuthConfig } from './config';
import { discoverOidc } from './oidc';
import {
  authErrorHandler,
  auditMutations,
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
}

export async function buildAuth(cfg: AuthConfig): Promise<BuiltAuth> {
  if (cfg.mode === 'dev') {
    console.warn('AUTH_MODE=dev — development-only placeholder identity; NEVER use in production');
    return {
      app: {
        authn: [devIdentity()],
        apiGuards: [requireApiAuth(), auditMutations()],
        router: createDevAuthRouter(),
        errorHandler: authErrorHandler(),
      },
      authenticateWs: async () => DEV_USER,
    };
  }

  const redis = new Redis(cfg.redisUrl);
  const sessions = new SessionStore(redis, cfg.sessionIdleTtlSeconds, cfg.sessionMaxTtlSeconds);
  const flow = await discoverOidc(cfg);
  const origin = new URL(cfg.publicUrl).origin;

  return {
    app: {
      authn: [sessionAuth(sessions)],
      apiGuards: [csrfOriginCheck(origin), requireApiAuth(), auditMutations()],
      router: createAuthRouter({ flow, sessions, cfg }),
      errorHandler: authErrorHandler(),
    },
    authenticateWs: async (req) => {
      const id = parseCookies(req.headers.cookie ?? '')[SESSION_COOKIE] ?? '';
      const record = await sessions.load(id);
      return record ? toAuthUser(record) : null;
    },
  };
}
