import { Router } from 'express';
import { GrpcClient, userMetadata } from '../grpc-client';
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

function normalizeStatus(status: string): string {
  return status.replace(/^(SCHEDULER_STATUS_|TASK_STATUS_)/, '').toLowerCase();
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
          retry_count: t.retry_count,
          max_retries: t.max_retries,
          created_at: toISO(t.created_at),
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
      });
    });
  });

  router.post('/:id/rerun', (req, res) => {
    client.triggerRerun(
      { source_run_id: req.params.id },
      userMetadata(req),
      (err: any) => {
        if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        res.sendStatus(200);
      }
    );
  });

  router.post('/:id/rebase', (req, res) => {
    client.triggerRebase(
      { source_run_id: req.params.id },
      userMetadata(req),
      (err: any) => {
        if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        res.sendStatus(200);
      }
    );
  });

  return router;
}
