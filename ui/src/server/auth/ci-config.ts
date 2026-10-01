import { readFileSync } from 'fs';

// One GitHub identity allowed to release a service. Every set field must match
// the token's claim; repositoryId is mandatory so a binding never trusts a name.
export interface CiBinding {
  repositoryId: string;
  ref?: string[];
  refProtected?: boolean;
  environment?: string[];
  workflowRef?: string[];
  allowBootstrap: boolean;
}

export interface CiAuthConfig {
  issuer: string; // no trailing slash; compared to the token's iss exactly
  audience: string;
  bindings: Map<string, CiBinding[]>; // service -> allowed identities
}

export const BINDING_FIELDS = ['repositoryId', 'ref', 'refProtected', 'environment', 'workflowRef', 'allowBootstrap'] as const;
const TOP_LEVEL = new Set(['issuer', 'audience', 'bindings']);
const LIST_FIELDS = ['ref', 'environment', 'workflowRef'] as const;

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function parseBinding(service: string, i: number, raw: unknown): CiBinding {
  const where = `ciAuth.bindings.${service}[${i}]`;
  if (!isObject(raw)) throw new Error(`${where}: expected an object`);
  for (const key of Object.keys(raw)) {
    if (!(BINDING_FIELDS as readonly string[]).includes(key)) throw new Error(`${where}: unknown field "${key}"`);
  }
  if (typeof raw.repositoryId !== 'string' || !/^[0-9]+$/.test(raw.repositoryId)) {
    throw new Error(`${where}: repositoryId is required and must be a quoted string of digits (GitHub repository_id)`);
  }
  const binding: CiBinding = { repositoryId: raw.repositoryId, allowBootstrap: false };
  for (const field of LIST_FIELDS) {
    const v = raw[field];
    if (v === undefined) continue;
    if (!Array.isArray(v) || v.length === 0 || !v.every((s) => typeof s === 'string' && s !== '')) {
      throw new Error(`${where}: ${field} must be a non-empty list of non-empty strings`);
    }
    binding[field] = v as string[];
  }
  if (raw.refProtected !== undefined) {
    if (typeof raw.refProtected !== 'boolean') throw new Error(`${where}: refProtected must be a boolean`);
    binding.refProtected = raw.refProtected;
  }
  if (raw.allowBootstrap !== undefined) {
    if (typeof raw.allowBootstrap !== 'boolean') throw new Error(`${where}: allowBootstrap must be a boolean`);
    binding.allowBootstrap = raw.allowBootstrap;
  }
  return binding;
}

export function parseCiAuthConfig(raw: unknown, opts: { allowInsecureIssuer: boolean }): CiAuthConfig {
  if (!isObject(raw)) throw new Error('ciAuth: expected a JSON object');
  for (const key of Object.keys(raw)) {
    if (!TOP_LEVEL.has(key)) throw new Error(`ciAuth: unknown key "${key}"`);
  }
  if (typeof raw.issuer !== 'string') throw new Error('ciAuth.issuer is required');
  let issuerUrl: URL;
  try {
    issuerUrl = new URL(raw.issuer);
  } catch {
    throw new Error(`ciAuth.issuer is not a URL: "${raw.issuer}"`);
  }
  if (issuerUrl.protocol !== 'https:' && !(opts.allowInsecureIssuer && issuerUrl.protocol === 'http:')) {
    throw new Error(`ciAuth.issuer must use https, got "${raw.issuer}"`);
  }
  if (typeof raw.audience !== 'string' || raw.audience.trim() === '') throw new Error('ciAuth.audience must be a non-empty string');
  const rawBindings = raw.bindings ?? {};
  if (!isObject(rawBindings)) throw new Error('ciAuth.bindings must be an object of service -> list');
  const bindings = new Map<string, CiBinding[]>();
  for (const [service, list] of Object.entries(rawBindings)) {
    if (service === '' || !Array.isArray(list)) throw new Error(`ciAuth.bindings.${service}: expected a list of bindings`);
    bindings.set(service, list.map((b, i) => parseBinding(service, i, b)));
  }
  return { issuer: raw.issuer.replace(/\/+$/, ''), audience: raw.audience, bindings };
}

// Reads the file the chart renders from ciAuth values. Unset path = no CI
// access; a set path that cannot be read or parsed stops the boot.
export function loadCiAuthConfig(env: NodeJS.ProcessEnv, authMode: 'dev' | 'oidc'): CiAuthConfig | null {
  const p = env.CI_AUTH_CONFIG_PATH;
  if (!p) return null;
  let raw: unknown;
  try {
    raw = JSON.parse(readFileSync(p, 'utf8'));
  } catch (err) {
    throw new Error(`CI_AUTH_CONFIG_PATH=${p}: cannot read JSON (${err instanceof Error ? err.message : 'unknown error'})`);
  }
  const allowInsecureIssuer = authMode === 'dev' || env.CI_AUTH_ALLOW_INSECURE_ISSUER === 'true';
  return parseCiAuthConfig(raw, { allowInsecureIssuer });
}
