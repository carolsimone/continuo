import { Router } from 'express';
import { GrpcClient } from '../clients/grpc-client';
import { grpcToHttpStatus } from './grpc-status';
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

export function createAttemptsRouter(stateClient: GrpcClient) {
  const router = Router();

  // GET /api/schedulers/:id/attempts?task_id=&limit=&offset= — one page of a
  // run's task attempts in (task_id, attempt) order, or of one task's when
  // task_id is set. The max of 200 is state's own page cap, so the page size
  // requested is the page size returned. State sends "" and 0 for a value it
  // does not have; those cross this route as null.
  router.get('/:id/attempts', (req, res) => {
    stateClient.listTaskAttempts(
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

  return router;
}
