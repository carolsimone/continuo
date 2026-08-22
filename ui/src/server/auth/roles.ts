import type { OidcAuthConfig } from './config';
import type { Role } from './types';

// Spec resolution order: group mapping (strongest matching role wins), then
// email overrides (only when email_verified is true), then the configured
// default. 'none' means access denied. Group membership comes from the IdP
// directory and is not gated on email verification; email overrides require a
// verified email to prevent privilege escalation via unverified email aliases.
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
  const ev = claims.email_verified;
  const emailVerified = ev === true || ev === 'true';
  const e = email.toLowerCase();
  if (emailVerified && e && cfg.operatorEmails.has(e)) return 'operator';
  if (emailVerified && e && cfg.viewerEmails.has(e)) return 'viewer';
  return cfg.defaultRole;
}
