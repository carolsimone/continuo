import { Router } from 'express';
import { GrpcClient } from '../grpc-client';

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

  router.get('/:id/executions', (req, res) => {
    stateClient.listTaskExecutions(
      { schedule_id: req.params.id, page_size: 500, page_offset: 0 },
      (err: any, response: any) => {
        if (err) {
          return res.status(500).json({ error: err.message });
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
        res.json({ executions });
      }
    );
  });

  return router;
}
