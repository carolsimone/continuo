import { Router } from 'express';
import { GrpcGraphClient } from '../grpc-graph-client';

function toISO(ts: any): string | null {
  if (!ts) return null;
  if (typeof ts === 'string') return ts;
  if (ts.seconds !== undefined) {
    const seconds = Number(ts.seconds);
    const nanos = Number(ts.nanos ?? 0);
    return new Date(seconds * 1000 + Math.floor(nanos / 1e6)).toISOString();
  }
  return null;
}

export function createTopologyRouter(graphClient: GrpcGraphClient) {
  const router = Router();

  // GET /api/topology/schedules — one tile per schedule with topology
  router.get('/schedules', (_req, res) => {
    graphClient.listScheduleTopologies({}, (err: any, resp: any) => {
      if (err) return res.status(500).json({ error: err.message });
      const schedules = (resp.schedules || []).map((s: any) => ({
        schedule_name: s.schedule_name,
        node_count: Number(s.node_count ?? 0),
        last_updated_at: toISO(s.last_updated_at),
      }));
      res.json({ schedules });
    });
  });

  return router;
}
