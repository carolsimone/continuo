import { Router } from 'express';
import * as grpc from '@grpc/grpc-js';
import { GrpcClient, userMetadata } from '../clients/grpc-client';
import { grpcToHttpStatus, sendGrpcError } from './grpc-status';
import { parseLimit, parseOffset } from './paging';

interface ProtoTimestamp {
  seconds: string;
  nanos: number;
}

function toISO(ts: ProtoTimestamp | null | undefined): string | null {
  if (!ts) return null;
  const ms = Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000);
  return new Date(ms).toISOString();
}

function normalizeStatus(status: string): string {
  return status.replace(/^(SCHEDULER_STATUS_|TASK_STATUS_)/, '').toLowerCase();
}

// RELEASE_MOVED_MESSAGE is the exact text state's TriggerRebase refuses with
// (FAILED_PRECONDITION) when the live release is no longer the one the caller
// previewed. A deploycheck guard pins it to state's copy.
export const RELEASE_MOVED_MESSAGE = 'the live release changed; preview again';

// parseQueryFlag reads an optional boolean query parameter: absent is false,
// exactly "true" or "false" is itself, anything else (another spelling, an
// empty value, a repeated parameter) is refused with null.
function parseQueryFlag(value: unknown): boolean | null {
  if (value === undefined) return false;
  if (value === 'true') return true;
  if (value === 'false') return false;
  return null;
}

interface RebaseBody {
  include_changed_upstream: boolean;
  expected_release_id: string;
}

// parseRebaseBody reads POST …/rebase's JSON body: include_changed_upstream
// must be a boolean and expected_release_id a string when present; absent they
// default as their proto fields do (false, ""). Anything else is an error
// message for a 400.
function parseRebaseBody(body: unknown): RebaseBody | string {
  if (body === undefined || body === null) {
    return { include_changed_upstream: false, expected_release_id: '' };
  }
  if (typeof body !== 'object' || Array.isArray(body)) return 'the body must be a JSON object';
  const { include_changed_upstream: include = false, expected_release_id: expected = '' } =
    body as Record<string, unknown>;
  if (typeof include !== 'boolean') return 'include_changed_upstream must be a boolean';
  if (typeof expected !== 'string') return 'expected_release_id must be a string';
  return { include_changed_upstream: include, expected_release_id: expected };
}

// isReleaseMoved reports whether a TriggerRebase failure is state's refusal of
// a rebase whose previewed release is no longer live.
function isReleaseMoved(err: any): boolean {
  return err?.code === grpc.status.FAILED_PRECONDITION && err?.details === RELEASE_MOVED_MESSAGE;
}

// toRebasePreview maps a PreviewRebaseResponse onto the HTTP response: the
// int64 promotion_seq (a string under longs: String) becomes a number, and a
// changed node's last_success_seconds of 0, meaning it has never succeeded,
// becomes null.
function toRebasePreview(response: any) {
  return {
    release_id: response.release_id ?? '',
    promotion_seq: Number(response.promotion_seq ?? 0),
    total_nodes: response.total_nodes ?? 0,
    will_run: response.will_run ?? 0,
    failed_or_skipped: response.failed_or_skipped ?? 0,
    new_nodes: response.new_nodes ?? 0,
    changed_upstream: response.changed_upstream ?? 0,
    downstream: response.downstream ?? 0,
    inherited: response.inherited ?? 0,
    changed_nodes: (response.changed_nodes ?? []).map((n: any) => {
      const lastSuccess = Number(n.last_success_seconds ?? 0);
      return {
        service_name: n.service_name,
        schema_name: n.schema_name,
        table_name: n.table_name,
        downstream: n.downstream ?? 0,
        last_success_seconds: lastSuccess > 0 ? lastSuccess : null,
      };
    }),
  };
}

export function createSchedulersRouter(client: GrpcClient) {
  const router = Router();

  // GET /api/schedulers/:id/tasks?limit=&offset= — tasks for a run (paged).
  // `total_count` lets a caller detect truncation; state imposes no server-side
  // cap here, so the max is enforced on this side.
  router.get('/:id/tasks', (req, res) => {
    client.listTasks(
      {
        schedule_id: req.params.id,
        page_size: parseLimit(req.query.limit, { def: 200, max: 500 }),
        page_offset: parseOffset(req.query.offset),
      },
      (err: any, response: any) => {
        if (err) {
          return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        }
        const tasks = (response.tasks || []).map((t: any) => ({
          task_id: t.task_id,
          schedule_id: t.schedule_id,
          service_name: t.service_name,
          schema_name: t.schema_name,
          table_name: t.table_name,
          job_name: t.job_name,
          status: normalizeStatus(t.status),
          attempt: t.attempt,
          max_retries: t.max_retries,
          created_at: toISO(t.created_at),
          wait_reason: t.wait_reason || null,
          waiting_on_run_id: t.waiting_on_run_id || null,
          waiting_since: toISO(t.waiting_since),
        }));
        res.json({ total_count: Number(response.total_count ?? 0), tasks });
      }
    );
  });

  router.get('/:id', (req, res) => {
    client.getScheduler({ schedule_id: req.params.id }, (err: any, response: any) => {
      if (err) return res.status(500).json({ error: err.message });
      if (!response?.scheduler) return res.status(404).json({ error: 'not found' });
      const s = response.scheduler;
      res.json({
        schedule_id: s.schedule_id,
        schedule_name: s.schedule_name,
        status: normalizeStatus(s.status),
        started_at: toISO(s.started_at),
        completed_at: toISO(s.completed_at),
        cancelled_at: toISO(s.cancelled_at),
        failure_reason: s.failure_reason || null,
      });
    });
  });

  router.post('/:id/rerun', (req, res) => {
    client.triggerRerun(
      { source_run_id: req.params.id },
      userMetadata(req),
      (err: any) => {
        if (err) return sendGrpcError(res, err);
        res.sendStatus(200);
      }
    );
  });

  // GET /api/schedulers/:id/rebase-preview?include_changed_upstream=true|false
  // — what a rebase of run :id would run on the live release, without starting
  // it. A read: viewers may call it and maintenance mode does not refuse it.
  router.get('/:id/rebase-preview', (req, res) => {
    const include = parseQueryFlag(req.query.include_changed_upstream);
    if (include === null) {
      return res.status(400).json({ error: 'include_changed_upstream must be true or false' });
    }
    client.previewRebase(
      { source_run_id: req.params.id, include_changed_upstream: include },
      (err: any, response: any) => {
        if (err) return sendGrpcError(res, err);
        res.json(toRebasePreview(response ?? {}));
      }
    );
  });

  // POST /api/schedulers/:id/rebase  {include_changed_upstream?, expected_release_id?}
  // — restarts run :id from its failure on the live release. A refusal because
  // the live release is no longer the previewed one answers 409 with code
  // "release_moved", so the caller can preview again.
  router.post('/:id/rebase', (req, res) => {
    const body = parseRebaseBody(req.body);
    if (typeof body === 'string') return res.status(400).json({ error: body });
    client.triggerRebase(
      { source_run_id: req.params.id, ...body },
      userMetadata(req),
      (err: any) => {
        if (err) {
          if (isReleaseMoved(err)) {
            return res.status(409).json({ error: RELEASE_MOVED_MESSAGE, code: 'release_moved' });
          }
          return sendGrpcError(res, err);
        }
        res.sendStatus(200);
      }
    );
  });

  return router;
}
