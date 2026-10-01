import { Router, type Response } from 'express';
import { rateLimit } from 'express-rate-limit';
import { HttpError, type ReleaseClient } from '../release-client';
import { authorize } from '../auth/authorize';
import { principalAuditFields, principalKey, principalOf, type Principal } from '../auth/principal';
import { audit } from '../auth/audit';

// The public, versioned release API that CD pipelines call. Its request and
// response shapes are owned here, not passed through from release-controller,
// so internal fields can change without breaking a pipeline.

export const SUBMIT_RATE_LIMIT_PER_MINUTE = 30;
const SUBMIT_FIELDS = new Set(['release_id', 'service', 'image_tag', 'bootstrap', 'kind', 'repo', 'commit_sha']);
const TERMINAL = new Set(['promoted', 'rejected', 'superseded']);

type SubmitBody = {
  release_id: string;
  service: string;
  image_tag: string;
  bootstrap?: boolean;
  kind?: string;
  repo?: string;
  commit_sha?: string;
};

function fail(res: Response, status: number, code: string, error: string): void {
  res.status(status).json({ error, code });
}

function parseSubmit(raw: unknown): { body: SubmitBody } | { error: string } {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return { error: 'body must be a JSON object' };
  const o = raw as Record<string, unknown>;
  const unknown = Object.keys(o).filter((k) => !SUBMIT_FIELDS.has(k));
  if (unknown.length > 0) return { error: `unknown field(s): ${unknown.join(', ')}` };
  for (const k of ['release_id', 'service', 'image_tag']) {
    if (typeof o[k] !== 'string' || o[k] === '') return { error: `${k} is required and must be a non-empty string` };
  }
  if (o.bootstrap !== undefined && typeof o.bootstrap !== 'boolean') return { error: 'bootstrap must be a boolean' };
  for (const k of ['kind', 'repo', 'commit_sha']) {
    if (o[k] !== undefined && typeof o[k] !== 'string') return { error: `${k} must be a string` };
  }
  return { body: { ...o } as SubmitBody };
}

export function projectRelease(rel: Record<string, unknown>, publicUrl?: string): Record<string, unknown> {
  const id = String(rel.release_id ?? '');
  return {
    release_id: id,
    service: rel.changed_service ?? '',
    status: rel.status ?? '',
    terminal: TERMINAL.has(String(rel.status)),
    bootstrap: rel.bootstrap === true,
    repo: rel.repo ?? '',
    commit_sha: rel.commit_sha ?? '',
    reject_reason: rel.reject_reason ?? '',
    reject_detail: rel.reject_detail ?? '',
    ui_url: publicUrl ? `${publicUrl}/releases/${encodeURIComponent(id)}` : null,
  };
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

// release-controller accepts a release_id it already holds without comparing
// it to the submission, so a 202 only means this submission was recorded when
// the stored release is the one that was sent.
function matchesStored(body: SubmitBody, stored: Record<string, unknown>): boolean {
  if (stored.changed_service !== body.service) return false;
  if ((stored.bootstrap === true) !== (body.bootstrap === true)) return false;
  if (body.repo !== undefined && stored.repo !== body.repo) return false;
  if (body.commit_sha !== undefined && stored.commit_sha !== body.commit_sha) return false;
  return true;
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
      let stored: Record<string, unknown>;
      try {
        stored = await releases.getRelease(body.release_id);
      } catch {
        // The submit is idempotent on release_id, so the caller can retry.
        auditSubmit(p, body, 503);
        return fail(res, 503, 'upstream_unavailable', 'release service unavailable');
      }
      if (!matchesStored(body, stored)) {
        auditSubmit(p, body, 409);
        return fail(res, 409, 'release_kind_conflict', `release id "${body.release_id}" already exists with a different service or provenance`);
      }
      auditSubmit(p, body, 202);
      let parsedResp: { release_id?: string; status?: string } = {};
      try {
        parsedResp = JSON.parse(upstream.text);
      } catch {
        // release-controller always answers 202 with JSON; fall back to the request id.
      }
      return res.status(202).json({ release_id: parsedResp.release_id ?? body.release_id, status: parsedResp.status ?? 'received' });
    }
    auditSubmit(p, body, upstream.status);
    if (upstream.status === 409) return fail(res, 409, 'release_kind_conflict', upstream.text.trim());
    if (upstream.status === 400) return fail(res, 400, 'bad_request', upstream.text.trim());
    return fail(res, 503, 'upstream_unavailable', 'release service unavailable');
  });

  router.get('/releases/:id', async (req, res) => {
    const p = principalOf(req)!;
    let rel: Record<string, unknown>;
    try {
      rel = await releases.getRelease(req.params.id);
    } catch (err) {
      if (err instanceof HttpError && err.status === 404) return fail(res, 404, 'not_found', 'release not found');
      return fail(res, 503, 'upstream_unavailable', 'release service unavailable');
    }
    const d = authorize(p, { kind: 'release.read', service: String(rel.changed_service ?? '') });
    if (!d.allow) return fail(res, 403, d.code, d.reason);
    res.json(projectRelease(rel, publicUrl));
  });

  router.get('/current-prod', async (req, res) => {
    const d = authorize(principalOf(req)!, { kind: 'prod.read' });
    if (!d.allow) return fail(res, 403, d.code, d.reason);
    try {
      const cp = await releases.getCurrentProd();
      res.json({ current_prod_release_id: cp.current_prod_release_id, node_count: cp.node_count, updated_at: cp.updated_at });
    } catch {
      fail(res, 503, 'upstream_unavailable', 'release service unavailable');
    }
  });

  router.use((_req, res) => fail(res, 404, 'not_found', 'no such /api/v1 route'));
  return router;
}
