import type { ErrorRequestHandler, RequestHandler, Router } from 'express';

export type Role = 'viewer' | 'operator';

export interface AuthUser {
  userId: string;
  email: string;
  name: string;
  role: Role;
}

// Placeholder identity injected by the dev-mode auth handler. The operator
// role gives local development full access without an identity provider.
export const DEV_USER: AuthUser = {
  userId: 'dev|local',
  email: 'dev@localhost',
  name: 'Dev User',
  role: 'operator',
};

export const SESSION_COOKIE = 'continuo_session';
export const PENDING_COOKIE = 'continuo_auth_pending';

// What createApp needs from the auth subsystem, regardless of mode.
export interface AppAuth {
  authn: RequestHandler[]; // attaches req.user (dev identity or Redis session)
  apiGuards: RequestHandler[]; // CSRF + role gate + mutation audit, mounted on /api
  router: Router; // /auth/*
  errorHandler: ErrorRequestHandler; // fail-closed 503 on auth-backend errors
}

declare module 'express-serve-static-core' {
  interface Request {
    user?: AuthUser;
  }
}
