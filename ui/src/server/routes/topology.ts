import { Router } from 'express';
import { GrpcClient } from '../clients/grpc-client';
import { toISO } from './timestamp';

export function createTopologyRouter(client: Pick<GrpcClient, 'listScheduleTopologies'>) {
  const router = Router();

  // GET /api/topology/schedules — one tile per schedule of the live release,
  // and how many of its nodes are dormant (no schedule tag, run only by hand).
  // dormant_node_count is int64, a string on the wire; an answer without it
  // counts none.
  router.get('/schedules', (_req, res) => {
    client.listScheduleTopologies({}, (err: any, resp: any) => {
      if (err) return res.status(500).json({ error: err.message });
      const schedules = (resp.schedules || []).map((s: any) => ({
        schedule_name: s.schedule_name,
        node_count: Number(s.node_count ?? 0),
        last_updated_at: toISO(s.last_updated_at),
      }));
      res.json({ schedules, dormant_node_count: Number(resp.dormant_node_count ?? 0) });
    });
  });

  return router;
}
