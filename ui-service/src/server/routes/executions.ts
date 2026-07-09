import { Router } from 'express';
import { GrpcClient } from '../grpc-client';
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

export function createExecutionsRouter(stateClient: GrpcClient) {
  const router = Router();

  // GET /api/schedulers/:id/executions?limit=&offset= — attempt history for a
  // run (paged). The max of 200 mirrors state's maxTaskExecutionsPageSize, so
  // the page size requested is the page size returned.
  router.get('/:id/executions', (req, res) => {
    stateClient.listTaskExecutions(
      {
        schedule_id: req.params.id,
        page_size: parseLimit(req.query.limit, { def: 200, max: 200 }),
        page_offset: parseOffset(req.query.offset),
      },
      (err: any, response: any) => {
        if (err) {
          return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        }
        const executions = (response.task_executions || []).map((e: any) => ({
          id: e.id,
          task_id: e.task_id,
          error_message: e.error_message || null,
          execution_time_seconds: e.execution_time_seconds || null,
          started_at: toISO(e.started_at),
          completed_at: toISO(e.completed_at),
          log_s3_key: e.log_s3_key || null,
        }));
        res.json({ total_count: Number(response.total_count ?? 0), executions });
      }
    );
  });

  return router;
}
