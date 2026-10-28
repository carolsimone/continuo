import { Router } from 'express';
import { GrpcClient } from '../clients/grpc-client';
import { toISO } from './timestamp';

export function createTopologyRouter(client: Pick<GrpcClient, 'listScheduleTopologies'>) {
  const router = Router();

  // GET /api/topology/schedules — one tile per schedule of the live release
  router.get('/schedules', (_req, res) => {
    client.listScheduleTopologies({}, (err: any, resp: any) => {
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
