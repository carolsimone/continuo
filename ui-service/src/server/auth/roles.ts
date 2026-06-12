import type { OidcAuthConfig } from './config';
import type { Role } from './types';

// Spec resolution order: group mapping (strongest matching role wins), then
// email overrides, then the configured default. 'none' means access denied.
export function resolveRole(
  claims: Record<string, unknown>,
  email: string,
  cfg: Pick<OidcAuthConfig, 'groupsClaim' | 'roleMapping' | 'operatorEmails' | 'viewerEmails' | 'defaultRole'>,
): Role | 'none' {
  const rawGroups = claims[cfg.groupsClaim];
  if (Array.isArray(rawGroups)) {
    const roles = rawGroups
      .filter((g): g is string => typeof g === 'string')
      .map((g) => cfg.roleMapping.get(g))
      .filter((r): r is Role => r !== undefined);
    if (roles.includes('operator')) return 'operator';
    if (roles.includes('viewer')) return 'viewer';
  }
  const e = email.toLowerCase();
  if (e && cfg.operatorEmails.has(e)) return 'operator';
  if (e && cfg.viewerEmails.has(e)) return 'viewer';
  return cfg.defaultRole;
}
