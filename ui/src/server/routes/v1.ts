import { Router, type Response } from 'express';
import { rateLimit } from 'express-rate-limit';
import { HttpError, type ReleaseClient, type ReleaseSubmission } from '../release-client';
import { authorize } from '../auth/authorize';
import { principalAuditFields, principalKey, type Principal } from '../auth/principal';
import { principalOf } from '../auth/request-principal';
import { audit } from '../auth/audit';

// The public, versioned release API that CD pipelines call. Its request and
// response shapes are owned here, not passed through from release-controller,
// so internal fields can change without breaking a pipeline.

export const SUBMIT_RATE_LIMIT_PER_MINUTE = 30;
// Sized for a pipeline polling its release to a terminal status: one read
// every few seconds per job, with headroom for parallel jobs of one repository.
export const READ_RATE_LIMIT_PER_MINUTE = 300;
const SUBMIT_FIELDS = new Set(['release_id', 'service', 'image_tag', 'bootstrap', 'kind', 'repo', 'commit_sha']);
const TERMINAL = new Set(['promoted', 'rejected', 'superseded']);
// A release id is a single URL path segment and an object-key component
// downstream, so the public API admits only this shape.
export const RELEASE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

// A release as GET /api/v1/releases/:id answers it.
export interface PublicRelease {
  release_id: string;
  service: string;
  status: string;
  terminal: boolean;
  bootstrap: boolean;
  repo: string;
  commit_sha: string;
  reject_reason: string;
  reject_detail: string;
  ui_url: string | null;
}

// The production pointer as GET /api/v1/current-prod answers it.
export interface PublicCurrentProd {
  current_prod_release_id: string;
  node_count: number;
  updated_at: string;
}

// The accepted-submission body of POST /api/v1/releases.
interface PublicSubmitAccepted {
  release_id: string;
  status: string;
}

// release-controller's JSON crosses into the public contract field by field:
// a value of the wrong type becomes the field's empty value rather than
// changing the shape a pipeline reads.
const str = (v: unknown): string => (typeof v === 'string' ? v : '');
const isObject = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

function fail(res: Response, status: number, code: string, error: string): void {
  res.status(status).json({ error, code });
}

function parseSubmit(raw: unknown): { body: ReleaseSubmission } | { error: string } {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return { error: 'body must be a JSON object' };
  const o = raw as Record<string, unknown>;
  const unknown = Object.keys(o).filter((k) => !SUBMIT_FIELDS.has(k));
  if (unknown.length > 0) return { error: `unknown field(s): ${unknown.join(', ')}` };
  for (const k of ['release_id', 'service', 'image_tag']) {
    if (typeof o[k] !== 'string' || o[k] === '') return { error: `${k} is required and must be a non-empty string` };
  }
  if (!RELEASE_ID_PATTERN.test(o.release_id as string)) {
    return { error: `release_id must match ${RELEASE_ID_PATTERN.source}` };
  }
  if (o.bootstrap !== undefined && typeof o.bootstrap !== 'boolean') return { error: 'bootstrap must be a boolean' };
  for (const k of ['kind', 'repo', 'commit_sha']) {
    if (o[k] !== undefined && typeof o[k] !== 'string') return { error: `${k} must be a string` };
  }
  const body: ReleaseSubmission = { release_id: o.release_id as string, service: o.service as string, image_tag: o.image_tag as string };
  if (o.bootstrap !== undefined) body.bootstrap = o.bootstrap as boolean;
  for (const k of ['kind', 'repo', 'commit_sha'] as const) {
    if (o[k] !== undefined) body[k] = o[k] as string;
  }
  return { body };
}

export function projectRelease(rel: Record<string, unknown>, publicUrl?: string): PublicRelease {
  const id = str(rel.release_id);
  const status = str(rel.status);
  return {
    release_id: id,
    service: str(rel.changed_service),
    status,
    terminal: TERMINAL.has(status),
    bootstrap: rel.bootstrap === true,
    repo: str(rel.repo),
    commit_sha: str(rel.commit_sha),
    reject_reason: str(rel.reject_reason),
    reject_detail: str(rel.reject_detail),
    ui_url: publicUrl ? `${publicUrl}/releases/${encodeURIComponent(id)}` : null,
  };
}

export function projectCurrentProd(cp: Record<string, unknown>): PublicCurrentProd {
  return {
    current_prod_release_id: str(cp.current_prod_release_id),
    node_count: typeof cp.node_count === 'number' ? cp.node_count : 0,
    updated_at: str(cp.updated_at),
  };
}

// The 202 body for an accepted submission. release-controller answers 202
// with {release_id, status}; a body that is not that JSON falls back to the
// submitted id and "received".
function acceptedBody(text: string, releaseId: string): PublicSubmitAccepted {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = undefined;
  }
  const o = isObject(parsed) ? parsed : {};
  return { release_id: str(o.release_id) || releaseId, status: str(o.status) || 'received' };
}

// The audit line for a submit. The body may not have passed validation yet, so
// only values that are strings (and a literal true for bootstrap) are recorded.
function auditSubmit(p: Principal, raw: unknown, outcome: number): void {
  const o = typeof raw === 'object' && raw !== null ? (raw as Record<string, unknown>) : {};
  audit('release_submit', {
    ...principalAuditFields(p),
    service: typeof o.service === 'string' ? o.service : undefined,
    release_id: typeof o.release_id === 'string' ? o.release_id : undefined,
    bootstrap: o.bootstrap === true,
    outcome,
  });
}

export function createV1Router(releases: ReleaseClient, publicUrl?: string): Router {
  const router = Router();

  // Each submission queues a release and its Kubernetes Jobs, so a looping
  // pipeline or a leaked token is capped per principal (per repository for
  // CI, per user for humans) rather than per client IP, which behind the
  // ingress is shared by every caller. The counter is in-memory per ui pod.
  const submitLimiter = rateLimit({
    windowMs: 60_000,
    limit: SUBMIT_RATE_LIMIT_PER_MINUTE,
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => principalKey(principalOf(req)!),
    handler: (req, res) => {
      auditSubmit(principalOf(req)!, req.body, 429);
      fail(res, 429, 'rate_limited', 'too many release submissions; retry in a minute');
    },
  });

  // Each read costs a release-controller call, so a polling loop that never
  // stops or a leaked token is capped per principal too, on a counter separate
  // from the submit limit so polling never uses up a pipeline's submissions.
  const readLimiter = rateLimit({
    windowMs: 60_000,
    limit: READ_RATE_LIMIT_PER_MINUTE,
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => principalKey(principalOf(req)!),
    handler: (_req, res) => fail(res, 429, 'rate_limited', 'too many reads; retry in a minute'),
  });

  router.post('/releases', submitLimiter, async (req, res) => {
    const p = principalOf(req)!;
    const parsed = parseSubmit(req.body);
    if ('error' in parsed) {
      auditSubmit(p, req.body, 400);
      return fail(res, 400, 'bad_request', parsed.error);
    }
    const body = parsed.body;

    const decision = authorize(p, { kind: 'release.submit', service: body.service, bootstrap: body.bootstrap === true });
    if (!decision.allow) {
      auditSubmit(p, body, 403);
      return fail(res, 403, decision.code, decision.reason);
    }
    // A CI submission's provenance is the token's, not the caller's word.
    if (p.kind === 'ci') {
      const provenance: Array<['repo' | 'commit_sha', string]> = [['repo', p.claims.repository], ['commit_sha', p.claims.sha]];
      for (const [field, claim] of provenance) {
        if (body[field] !== undefined && body[field] !== claim) {
          auditSubmit(p, body, 403);
          return fail(res, 403, 'claim_mismatch', `${field} "${body[field]}" does not match the token's "${claim}"`);
        }
        body[field] = claim;
      }
    }

    let upstream: { status: number; text: string };
    try {
      upstream = await releases.submitRelease(body);
    } catch {
      auditSubmit(p, body, 503);
      return fail(res, 503, 'upstream_unavailable', 'release service unavailable');
    }
    if (upstream.status === 202) {
      auditSubmit(p, body, 202);
      return res.status(202).json(acceptedBody(upstream.text, body.release_id));
    }
    auditSubmit(p, body, upstream.status);
    // release-controller answers 409 when the release id already names a run
    // of another kind or a candidate with a different service, image tag,
    // kind, bootstrap flag or source change; its message is passed through.
    if (upstream.status === 409) return fail(res, 409, 'release_kind_conflict', upstream.text.trim());
    if (upstream.status === 400) return fail(res, 400, 'bad_request', upstream.text.trim());
    return fail(res, 503, 'upstream_unavailable', 'release service unavailable');
  });

  // A CI identity reading a release of a service it is not bound to gets the
  // same 404 as a missing release, so the route never confirms that a release
  // id exists or names the service it belongs to.
  router.get('/releases/:id', readLimiter, async (req, res) => {
    const p = principalOf(req)!;
    if (!RELEASE_ID_PATTERN.test(req.params.id)) return fail(res, 404, 'not_found', 'release not found');
    let rel: unknown;
    try {
      rel = await releases.getRelease(req.params.id);
    } catch (err) {
      if (err instanceof HttpError && err.status === 404) return fail(res, 404, 'not_found', 'release not found');
      return fail(res, 503, 'upstream_unavailable', 'release service unavailable');
    }
    if (!isObject(rel)) return fail(res, 503, 'upstream_unavailable', 'release service unavailable');
    const d = authorize(p, { kind: 'release.read', service: str(rel.changed_service) });
    if (!d.allow) {
      if (p.kind === 'ci') return fail(res, 404, 'not_found', 'release not found');
      return fail(res, 403, d.code, d.reason);
    }
    res.json(projectRelease(rel, publicUrl));
  });

  router.get('/current-prod', readLimiter, async (req, res) => {
    const d = authorize(principalOf(req)!, { kind: 'prod.read' });
    if (!d.allow) return fail(res, 403, d.code, d.reason);
    let cp: unknown;
    try {
      cp = await releases.getCurrentProd();
    } catch {
      return fail(res, 503, 'upstream_unavailable', 'release service unavailable');
    }
    if (!isObject(cp)) return fail(res, 503, 'upstream_unavailable', 'release service unavailable');
    res.json(projectCurrentProd(cp));
  });

  router.use((_req, res) => fail(res, 404, 'not_found', 'no such /api/v1 route'));
  return router;
}
