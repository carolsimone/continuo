import { Router } from 'express';
import { GrpcClient } from '../grpc-client';
import { GrpcGraphClient } from '../grpc-graph-client';

interface ProtoTimestamp {
  seconds: string;
  nanos: number;
}

function toISO(ts: ProtoTimestamp | null | undefined): string | null {
  if (!ts) return null;
  const ms = Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000);
  return new Date(ms).toISOString();
}

export function createSchedulesRouter(stateClient: GrpcClient, graphClient: GrpcGraphClient) {
  const router = Router();

  // GET /api/schedules — list all known schedules (including never-run)
  router.get('/', (req, res) => {
    stateClient.listAllSchedules({}, (err: any, response: any) => {
      if (err) return res.status(500).json({ error: err.message });
      const schedules = (response.schedules || []).map((s: any) => ({
        schedule_name: s.schedule_name,
        cron_expression: s.cron_expression,
        description: s.description,
        timezone: s.timezone,
        is_running: s.is_running,
        last_run_at: toISO(s.last_run_at),
        last_run_status: s.last_run_status,
        last_run_id: s.last_run_id || null,
      }));
      res.json({ schedules });
    });
  });

  // GET /api/schedules/:name/graph — fetch DAG directly by schedule name
  router.get('/:name/graph', (req, res) => {
    graphClient.getScheduleGraph({ schedule_name: req.params.name }, (err: any, graphResp: any) => {
      if (err) return res.status(500).json({ error: err.message });
      const nodes = (graphResp.nodes || []).map((n: any) => ({
        node_id: `${n.service_name}.${n.schema_name}.${n.table_name}`,
        node_type: n.node_type,
        schedule_name: n.schedule_name,
      }));
      const edges = (graphResp.edges || []).map((e: any) => ({
        from_node_id: e.from_node_id,
        to_node_id: e.to_node_id,
      }));
      res.json({ nodes, edges });
    });
  });

  // GET /api/schedules/:name/runs — list completed runs for a schedule (newest first)
  router.get('/:name/runs', (req, res) => {
    graphClient.listRuns({ schedule_name: req.params.name }, (err: any, resp: any) => {
      if (err) return res.status(500).json({ error: err.message });
      const runs = (resp.runs || []).map((r: any) => ({
        run_id: r.run_id,
        schedule_name: r.schedule_name,
        terminal_status: r.terminal_status,
        created_at: r.created_at || null,
        completed_at: r.completed_at || null,
      }));
      res.json({ runs });
    });
  });

  return router;
}

export function createRunsRouter(graphClient: GrpcGraphClient) {
  const router = Router();

  // GET /api/runs/:run_id/graph — per-run DAG topology + node statuses
  router.get('/:run_id/graph', (req, res) => {
    graphClient.getRunGraph({ run_id: req.params.run_id }, (err: any, resp: any) => {
      if (err) return res.status(500).json({ error: err.message });
      const nodes = (resp.nodes || []).map((n: any) => ({
        node_id: `${n.service_name}.${n.schema_name}.${n.table_name}`,
        node_type: n.node_type,
        schedule_name: n.schedule_name,
        status: typeof n.status === 'string' ? n.status.toLowerCase() : null,
      }));
      const edges = (resp.edges || []).map((e: any) => ({
        from_node_id: e.from_node_id,
        to_node_id: e.to_node_id,
      }));
      res.json({ nodes, edges });
    });
  });

  return router;
}
