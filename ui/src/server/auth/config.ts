import type { Role } from './types';
import { readSecret } from '../secrets';

export interface OidcAuthConfig {
  mode: 'oidc';
  issuerUrl: string;
  clientId: string;
  clientSecret: string;
  publicUrl: string; // normalized: no trailing slash
  scopes: string;
  groupsClaim: string;
  roleMapping: Map<string, Role>;
  operatorEmails: Set<string>;
  viewerEmails: Set<string>;
  defaultRole: Role | 'none';
  sessionIdleTtlSeconds: number;
  sessionMaxTtlSeconds: number;
  redisUrl: string; // carries no credentials
  redisUsername?: string;
}

export interface DevAuthConfig {
  mode: 'dev';
}

export type AuthConfig = OidcAuthConfig | DevAuthConfig;

const ROLES: ReadonlySet<Role> = new Set<Role>(['viewer', 'operator']);

export function parseDuration(raw: string, name: string): number {
  const m = /^(\d+)(s|m|h)$/.exec(raw.trim());
  if (!m) throw new Error(`${name}: expected "<n>s", "<n>m" or "<n>h", got "${raw}"`);
  const n = parseInt(m[1], 10);
  return m[2] === 'h' ? n * 3600 : m[2] === 'm' ? n * 60 : n;
}

function parseRoleMapping(raw: string): Map<string, Role> {
  const mapping = new Map<string, Role>();
  for (const pair of raw.split(',').map((p) => p.trim()).filter(Boolean)) {
    const eq = pair.indexOf('=');
    if (eq <= 0) throw new Error(`AUTH_ROLE_MAPPING: expected "group=role", got "${pair}"`);
    const group = pair.slice(0, eq).trim();
    const role = pair.slice(eq + 1).trim();
    if (!ROLES.has(role as Role)) throw new Error(`AUTH_ROLE_MAPPING: unknown role "${role}" for group "${group}"`);
    mapping.set(group, role as Role);
  }
  return mapping;
}

function parseEmails(raw: string): Set<string> {
  return new Set(raw.split(',').map((e) => e.trim().toLowerCase()).filter(Boolean));
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  const v = env[name];
  if (v === undefined || v.trim() === '') throw new Error(`${name} is required when AUTH_MODE=oidc`);
  return v;
}

// The credential variable's value, or an error naming it when neither the
// variable nor its _FILE form yields a non-blank value.
function requiredSecret(env: NodeJS.ProcessEnv, name: string): string {
  const v = readSecret(env, name);
  if (v === undefined || v.trim() === '') throw new Error(`${name} is required when AUTH_MODE=oidc`);
  return v;
}

// The Redis URL, which must not embed a username or password: those come from
// REDIS_USERNAME and REDIS_PASSWORD(_FILE), so the password can be re-read on
// every reconnect.
function redisUrlWithoutCredentials(env: NodeJS.ProcessEnv): string {
  const raw = required(env, 'REDIS_URL');
  let u: URL;
  try {
    u = new URL(raw);
  } catch {
    return raw; // not a URL (e.g. a socket path): it holds no userinfo
  }
  if (u.username !== '' || u.password !== '') {
    throw new Error('REDIS_URL must not carry credentials; set REDIS_USERNAME and REDIS_PASSWORD');
  }
  return raw;
}

// The ui's public base URL from AUTH_PUBLIC_URL, without trailing slashes, or
// undefined when it is unset or blank. It is the single parse of the setting:
// the login redirect, the CSRF origin and the cookie Secure flag (through
// loadAuthConfig) and the public API's ui_url links all read it from here.
export function loadPublicUrl(env: NodeJS.ProcessEnv): string | undefined {
  const v = env.AUTH_PUBLIC_URL;
  if (v === undefined || v.trim() === '') return undefined;
  return v.replace(/\/+$/, '') || undefined;
}

export function loadAuthConfig(env: NodeJS.ProcessEnv): AuthConfig {
  const mode = env.AUTH_MODE;
  if (mode === 'dev') return { mode: 'dev' };
  if (mode !== 'oidc') {
    throw new Error(`AUTH_MODE must be "oidc" or "dev", got "${mode ?? ''}" (there is no unauthenticated mode)`);
  }
  const defaultRole = env.AUTH_DEFAULT_ROLE ?? 'none';
  if (defaultRole !== 'none' && !ROLES.has(defaultRole as Role)) {
    throw new Error(`AUTH_DEFAULT_ROLE must be none, viewer or operator, got "${defaultRole}"`);
  }
  const publicUrl = loadPublicUrl(env);
  if (!publicUrl) throw new Error('AUTH_PUBLIC_URL is required when AUTH_MODE=oidc');
  return {
    mode: 'oidc',
    issuerUrl: required(env, 'AUTH_OIDC_ISSUER_URL'),
    clientId: required(env, 'AUTH_OIDC_CLIENT_ID'),
    clientSecret: requiredSecret(env, 'AUTH_OIDC_CLIENT_SECRET'),
    publicUrl,
    scopes: env.AUTH_OIDC_SCOPES ?? 'openid email profile',
    groupsClaim: env.AUTH_GROUPS_CLAIM ?? 'groups',
    roleMapping: parseRoleMapping(env.AUTH_ROLE_MAPPING ?? ''),
    operatorEmails: parseEmails(env.AUTH_OPERATOR_EMAILS ?? ''),
    viewerEmails: parseEmails(env.AUTH_VIEWER_EMAILS ?? ''),
    defaultRole: defaultRole as Role | 'none',
    sessionIdleTtlSeconds: parseDuration(env.AUTH_SESSION_IDLE_TTL ?? '8h', 'AUTH_SESSION_IDLE_TTL'),
    sessionMaxTtlSeconds: parseDuration(env.AUTH_SESSION_MAX_TTL ?? '24h', 'AUTH_SESSION_MAX_TTL'),
    redisUrl: redisUrlWithoutCredentials(env),
    redisUsername: env.REDIS_USERNAME || undefined,
  };
}
