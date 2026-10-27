import { Router } from 'express';
import * as grpc from '@grpc/grpc-js';
import { GrpcClient, userMetadata } from '../clients/grpc-client';
import { executionOrderEdges } from './graph-edges';
import { grpcToHttpStatus, sendGrpcError } from './grpc-status';
import { parseLimit, parseOffset } from './paging';
import { requireRequestId } from './request-id';
import { toISO } from './timestamp';

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

interface RebaseOptions {
  include_changed_upstream: boolean;
  expected_release_id: string;
}

// parseRebaseOptions reads POST …/rebase's JSON body besides its request_id:
// include_changed_upstream must be a boolean and expected_release_id a string
// when present; absent they default as their proto fields do (false, ""). A
// non-object body, or a field of another type, is an error message for a 400.
function parseRebaseOptions(body: unknown): RebaseOptions | string {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) return 'the body must be a JSON object';
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

// toRun maps a Run onto the HTTP response. State sends "" for a string it does
// not have, which crosses as null; the int64 promotion_seq (a string under
// longs: String) becomes a number. Statuses are the lowercase run_status
// values state sends and cross unchanged.
function toRun(r: any) {
  return {
    run_id: r.run_id,
    schedule_name: r.schedule_name,
    kind: r.kind,
    operation: r.operation,
    status: r.status,
    failure_reason: r.failure_reason || null,
    release_id: r.release_id || null,
    promotion_seq: Number(r.promotion_seq ?? 0),
    initiated_by: r.initiated_by || null,
    created_at: toISO(r.created_at),
    started_at: toISO(r.started_at),
    completed_at: toISO(r.completed_at),
    last_progress_at: toISO(r.last_progress_at),
    cancelled_at: toISO(r.cancelled_at),
    cancelled_by: r.cancelled_by || null,
    cancellation_reason: r.cancellation_reason || null,
  };
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

// createRunsRouter serves a run, its tasks, attempts and graph, and the
// commands on one run (rerun, rebase, cancel), all from RunService. A command
// that creates a run takes the browser-minted request_id in its JSON body and
// forwards it, so a repeat of the same action creates no second run.
export function createRunsRouter(client: GrpcClient) {
  const router = Router();

  // GET /api/runs/:id/tasks?limit=&offset= — tasks for a run (paged).
  // `total_count` lets a caller detect truncation; state imposes no server-side
  // cap here, so the max is enforced on this side.
  router.get('/:id/tasks', (req, res) => {
    client.listTasks(
      {
        run_id: req.params.id,
        page_size: parseLimit(req.query.limit, { def: 200, max: 500 }),
        page_offset: parseOffset(req.query.offset),
      },
      (err: any, response: any) => {
        if (err) {
          return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        }
        const tasks = (response.tasks || []).map((t: any) => ({
          task_id: t.task_id,
          run_id: t.run_id,
          service_name: t.service_name,
          schema_name: t.schema_name,
          table_name: t.table_name,
          job_name: t.job_name,
          status: t.status,
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

  // GET /api/runs/:id/attempts?task_id=&limit=&offset= — one page of a run's
  // task attempts in (task_id, attempt) order, or of one task's when task_id
  // is set. The max of 200 is state's own page cap, so the page size requested
  // is the page size returned. State sends "" and 0 for a value it does not
  // have; those cross this route as null.
  router.get('/:id/attempts', (req, res) => {
    client.listRunAttempts(
      {
        run_id: req.params.id,
        task_id: typeof req.query.task_id === 'string' ? req.query.task_id : '',
        page_size: parseLimit(req.query.limit, { def: 200, max: 200 }),
        page_offset: parseOffset(req.query.offset),
      },
      (err: any, response: any) => {
        if (err) {
          return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        }
        const attempts = (response.attempts || []).map((a: any) => ({
          task_id: a.task_id,
          attempt: Number(a.attempt ?? 0),
          job_name: a.job_name,
          status: a.status,
          failure_class: a.failure_class || null,
          failure_reason: a.failure_reason || null,
          dispatched_at: toISO(a.dispatched_at),
          queued_at: toISO(a.queued_at),
          started_at: toISO(a.started_at),
          finished_at: toISO(a.finished_at),
          execution_seconds: a.execution_seconds || null,
          error_message: a.error_message || null,
          log_s3_key: a.log_s3_key || null,
          run_results_uri: a.run_results_uri || null,
        }));
        res.json({ total_count: Number(response.total_count ?? 0), attempts });
      }
    );
  });

  // GET /api/runs/:id/graph — a run's tasks and dependencies from its own
  // release, with task statuses.
  router.get('/:id/graph', (req, res) => {
    client.getRunGraph({ run_id: req.params.id }, (err: any, resp: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      const nodes = (resp.nodes || []).map((n: any) => ({
        node_id: `${n.service_name}.${n.schema_name}.${n.table_name}`,
        node_type: n.node_type,
        schedule_name: n.schedule_name,
        status: n.status || null,
        wait_reason: n.wait_reason || null,
        waiting_on_run_id: n.waiting_on_run_id || null,
        waiting_since: n.waiting_since || null,
      }));
      res.json({
        nodes,
        edges: executionOrderEdges(resp.edges),
        run_promotion_seq: Number(resp.run_promotion_seq ?? 0),
        latest_promotion_seq: Number(resp.latest_promotion_seq ?? 0),
      });
    });
  });

  router.get('/:id', (req, res) => {
    client.getRun({ run_id: req.params.id }, (err: any, response: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      if (!response?.run) return res.status(404).json({ error: 'not found' });
      res.json(toRun(response.run));
    });
  });

  // POST /api/runs/:id/rerun  {request_id} — re-executes the failed or
  // cancelled run :id's tasks that did not succeed, in a new run.
  router.post('/:id/rerun', (req, res) => {
    const requestId = requireRequestId(req, res);
    if (requestId === null) return;
    client.triggerRerun(
      { request_id: requestId, source_run_id: req.params.id },
      userMetadata(req),
      (err: any, response: any) => {
        if (err) return sendGrpcError(res, err);
        res.json({ run_id: response.run_id, schedule_name: response.schedule_name });
      }
    );
  });

  // GET /api/runs/:id/rebase-preview?include_changed_upstream=true|false
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

  // POST /api/runs/:id/rebase  {request_id, include_changed_upstream?,
  // expected_release_id?} — restarts run :id from its failure on the live
  // release, in a new run. A refusal because the live release is no longer the
  // previewed one answers 409 with code "release_moved", so the caller can
  // preview again.
  router.post('/:id/rebase', (req, res) => {
    const requestId = requireRequestId(req, res);
    if (requestId === null) return;
    const options = parseRebaseOptions(req.body);
    if (typeof options === 'string') return res.status(400).json({ error: options });
    client.triggerRebase(
      { request_id: requestId, source_run_id: req.params.id, ...options },
      userMetadata(req),
      (err: any, response: any) => {
        if (err) {
          if (isReleaseMoved(err)) {
            return res.status(409).json({ error: RELEASE_MOVED_MESSAGE, code: 'release_moved' });
          }
          return sendGrpcError(res, err);
        }
        res.json({ run_id: response.run_id, schedule_name: response.schedule_name });
      }
    );
  });

  // POST /api/runs/:id/cancel  {cancellation_reason} — cancels run :id and
  // answers the run as the cancel left it. CancelRun takes no request_id: a run
  // that is already cancelling or cancelled is returned as it is. The
  // cancelling user is the authenticated caller, forwarded as gRPC metadata
  // (userMetadata).
  router.post('/:id/cancel', (req, res) => {
    const { cancellation_reason } = req.body ?? {};
    if (!cancellation_reason) {
      return res.status(400).json({ error: 'cancellation_reason is required' });
    }
    client.cancelRun(
      { run_id: req.params.id, cancellation_reason },
      userMetadata(req),
      (err: any, response: any) => {
        if (err) return sendGrpcError(res, err);
        if (!response?.run) return res.status(500).json({ error: 'state returned no run' });
        res.json(toRun(response.run));
      }
    );
  });

  return router;
}
