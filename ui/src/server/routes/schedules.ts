import { Router } from 'express';
import { GrpcClient, userMetadata } from '../clients/grpc-client';
import { executionOrderEdges } from './graph-edges';
import { grpcToHttpStatus, sendGrpcError } from './grpc-status';
import { parseOperation } from './operation';
import { parseLimit, parseOffset } from './paging';
import { requireRequestId } from './request-id';
import { toISO } from './timestamp';

const DEFAULT_RUNS_PAGE = 10;
// State clamps ListRuns page_size to this ceiling.
const MAX_RUNS_PAGE = 200;

// createSchedulesRouter serves the schedules of the live release, their graphs
// and completed runs, and starts a run of a schedule, all from RunService.
export function createSchedulesRouter(client: GrpcClient) {
  const router = Router();

  // GET /api/schedules — every schedule with its cron and its last run
  router.get('/', (_req, res) => {
    client.listSchedules({}, (err: any, response: any) => {
      if (err) return res.status(500).json({ error: err.message });
      const schedules = (response.schedules || []).map((s: any) => ({
        schedule_name: s.schedule_name,
        cron_expression: s.cron_expression,
        description: s.description,
        timezone: s.timezone,
        has_active_run: s.has_active_run,
        last_run_at: toISO(s.last_run_at),
        last_run_status: s.last_run_status,
        last_run_id: s.last_run_id || null,
      }));
      res.json({ schedules });
    });
  });

  // GET /api/schedules/:name/graph — the live release's nodes of the schedule,
  // their one-hop upstreams in other schedules and the edges among them
  router.get('/:name/graph', (req, res) => {
    client.getScheduleGraph({ schedule_name: req.params.name }, (err: any, graphResp: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      const nodes = (graphResp.nodes || []).map((n: any) => ({
        node_id: `${n.service_name}.${n.schema_name}.${n.table_name}`,
        node_type: n.node_type,
        schedule_name: n.schedule_name,
      }));
      res.json({
        nodes,
        edges: executionOrderEdges(graphResp.edges),
        promotion_seq: Number(graphResp.promotion_seq ?? 0),
      });
    });
  });

  // GET /api/schedules/:name/runs?limit=&offset= — one page of a schedule's completed runs
  // (newest first), with the schedule's total completed run count.
  router.get('/:name/runs', (req, res) => {
    const request = {
      schedule_name: req.params.name,
      page_size: parseLimit(req.query.limit, { def: DEFAULT_RUNS_PAGE, max: MAX_RUNS_PAGE }),
      page_offset: parseOffset(req.query.offset),
    };
    client.listRuns(request, (err: any, resp: any) => {
      if (err) return res.status(500).json({ error: err.message });
      const runs = (resp.runs || []).map((r: any) => ({
        run_id: r.run_id,
        schedule_name: r.schedule_name,
        status: r.status,
        created_at: r.created_at || null,
        completed_at: r.completed_at || null,
        failure_reason: r.failure_reason || null,
      }));
      res.json({ runs, total_count: Number(resp.total_count ?? 0) });
    });
  });

  // POST /api/schedules/:name/trigger  {request_id, operation?} — starts a run
  // of the whole schedule. Cancelling is done by run id (POST /api/runs/:id/cancel).
  router.post('/:name/trigger', (req, res) => {
    const requestId = requireRequestId(req, res);
    if (requestId === null) return;
    const operation = parseOperation(req.body?.operation);
    if (operation === null) {
      return res.status(400).json({ error: 'operation must be one of "", run, test, build' });
    }
    client.startRun(
      { request_id: requestId, schedule_name: req.params.name, operation },
      userMetadata(req),
      (err: any, response: any) => {
        if (err) return sendGrpcError(res, err);
        res.json({ run_id: response.run_id });
      }
    );
  });

  return router;
}
