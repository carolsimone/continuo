import type { Request } from 'express';
import type { AuthUser } from './types';
import type { CiBinding } from './ci-config';
import { InvalidTokenError } from './bearer';

// The GitHub Actions OIDC claims continuo reads. Optional claims GitHub omits
// (environment outside a deployment job) are empty strings.
export interface GithubClaims {
  repositoryId: string;
  repository: string;
  sha: string;
  ref: string;
  refProtected: string;
  environment: string;
  workflowRef: string;
  runId: string;
}

export interface CiGrant {
  allowBootstrap: boolean;
}

export type Principal =
  | { kind: 'human'; user: AuthUser }
  | { kind: 'ci'; subject: string; claims: GithubClaims; grants: Map<string, CiGrant> };

export function githubClaimsFrom(payload: Record<string, unknown>): GithubClaims {
  const str = (k: string) => (typeof payload[k] === 'string' ? (payload[k] as string) : '');
  const claims: GithubClaims = {
    repositoryId: str('repository_id'),
    repository: str('repository'),
    sha: str('sha'),
    ref: str('ref'),
    refProtected: str('ref_protected'),
    environment: str('environment'),
    workflowRef: str('workflow_ref'),
    runId: str('run_id'),
  };
  if (!/^[0-9]+$/.test(claims.repositoryId) || !claims.repository || !claims.sha) {
    throw new InvalidTokenError('GitHub token lacks repository_id, repository or sha');
  }
  return claims;
}

export function bindingMatches(b: CiBinding, c: GithubClaims): boolean {
  if (b.repositoryId !== c.repositoryId) return false;
  if (b.ref && !b.ref.includes(c.ref)) return false;
  if (b.refProtected !== undefined && String(b.refProtected) !== c.refProtected) return false;
  if (b.environment && !b.environment.includes(c.environment)) return false;
  if (b.workflowRef && !b.workflowRef.includes(c.workflowRef)) return false;
  return true;
}

// The services this CI identity may release: those with at least one matching
// binding. Bootstrap is allowed when any matching binding allows it.
export function resolveGrants(c: GithubClaims, bindings: Map<string, CiBinding[]>): Map<string, CiGrant> {
  const grants = new Map<string, CiGrant>();
  for (const [service, list] of bindings) {
    const matching = list.filter((b) => bindingMatches(b, c));
    if (matching.length > 0) grants.set(service, { allowBootstrap: matching.some((b) => b.allowBootstrap) });
  }
  return grants;
}

export function principalOf(req: Request): Principal | undefined {
  if (req.principal) return req.principal;
  return req.user ? { kind: 'human', user: req.user } : undefined;
}

export function principalKey(p: Principal): string {
  return p.kind === 'ci' ? `ci:${p.claims.repositoryId}` : `human:${p.user.userId}`;
}

export function principalAuditFields(p: Principal): Record<string, unknown> {
  if (p.kind === 'human') return { principal: 'human', user_id: p.user.userId, email: p.user.email, role: p.user.role };
  return {
    principal: 'ci',
    repository: p.claims.repository,
    repository_id: p.claims.repositoryId,
    ref: p.claims.ref,
    run_id: p.claims.runId,
    workflow_ref: p.claims.workflowRef,
  };
}
