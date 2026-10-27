import { Router } from 'express';
import { GrpcClient, userMetadata } from '../clients/grpc-client';
import { GrpcGraphClient } from '../clients/grpc-graph-client';
import { grpcToHttpStatus, sendGrpcError } from './grpc-status';
import { parseOperation } from './operation';
import { parseLimit, parseOffset } from './paging';

const DEFAULT_RUNS_PAGE = 10;
// State clamps ListRuns page_size to this ceiling.
const MAX_RUNS_PAGE = 200;

interface ProtoTimestamp {
  seconds: string;
  nanos: number;
}

function toISO(ts: ProtoTimestamp | null | undefined): string | null {
  if (!ts) return null;
  const ms = Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000);
  return new Date(ms).toISOString();
}

// Graph edges arrive in dependency direction: `from` is the dependent node,
// `to` is the node it reads. Every client view (dependency depth, swimlane
// columns, arrowheads, the selected node's upstream/downstream sets, the
// topology layout) reads an edge as "`from` runs before `to`", so the edge is
// reversed exactly once here, at the boundary, into execution order.
function executionOrderEdges(edges: any[] | undefined) {
  return (edges || []).map((e: any) => ({
    from_node_id: e.to_node_id,
    to_node_id: e.from_node_id,
  }));
}

export function createSchedulesRouter(stateClient: GrpcClient, graphClient: GrpcGraphClient) {
  const router = Router();

  // GET /api/schedules — list all known schedules from state service
  router.get('/', async (_req, res) => {
    try {
      const stateResp = await new Promise<any>((resolve, reject) => {
        stateClient.listAllSchedules({}, (err: any, response: any) =>
          err ? reject(err) : resolve(response)
        );
      });

      const schedules = (stateResp.schedules || []).map((s: any) => ({
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
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
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
      const edges = executionOrderEdges(graphResp.edges);
      const promotion_seq = Number(graphResp.promotion_seq ?? 0);
      res.json({ nodes, edges, promotion_seq });
    });
  });

  // GET /api/schedules/:name/runs?limit=&offset= — one page of a schedule's runs (newest first)
  // from the state service, with the schedule's total run count.
  router.get('/:name/runs', (req, res) => {
    const request = {
      schedule_name: req.params.name,
      page_size: parseLimit(req.query.limit, { def: DEFAULT_RUNS_PAGE, max: MAX_RUNS_PAGE }),
      page_offset: parseOffset(req.query.offset),
    };
    stateClient.listRuns(request, (err: any, resp: any) => {
      if (err) return res.status(500).json({ error: err.message });
      const runs = (resp.runs || []).map((r: any) => ({
        run_id: r.run_id,
        schedule_name: r.schedule_name,
        terminal_status: r.terminal_status,
        created_at: r.created_at || null,
        completed_at: r.completed_at || null,
        failure_reason: r.failure_reason || null,
      }));
      res.json({ runs, total_count: Number(resp.total_count ?? 0) });
    });
  });

  // POST /api/schedules/:name/trigger — trigger a full DAG run
  router.post('/:name/trigger', (req, res) => {
    const operation = parseOperation(req.body?.operation);
    if (operation === null) {
      return res.status(400).json({ error: 'operation must be one of "", run, test, build' });
    }
    stateClient.triggerSchedule(
      { schedule_name: req.params.name, operation },
      userMetadata(req),
      (err: any, response: any) => {
        if (err) return sendGrpcError(res, err);
        res.json({ schedule_id: response.schedule_id });
      }
    );
  });

  // POST /api/schedules/:name/cancel — cancel an active DAG run.
  // The cancelling user is the authenticated caller, forwarded as gRPC metadata
  // (userMetadata); the client no longer supplies a cancelled_by identity.
  router.post('/:name/cancel', (req, res) => {
    const { cancellation_reason } = req.body ?? {};
    if (!cancellation_reason) {
      return res.status(400).json({ error: 'cancellation_reason is required' });
    }
    stateClient.cancelSchedule(
      {
        schedule_name:       req.params.name,
        cancellation_reason,
      },
      userMetadata(req),
      (err: any, response: any) => {
        if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        res.json({ schedule_id: response.schedule_id });
      },
    );
  });

  return router;
}

export function createRunsRouter(stateClient: GrpcClient) {
  const router = Router();

  // GET /api/runs/:run_id/graph — a run's tasks and dependencies from its own
  // release, with task statuses, from the state service
  router.get('/:run_id/graph', (req, res) => {
    stateClient.getRunGraph({ run_id: req.params.run_id }, (err: any, resp: any) => {
      if (err) return res.status(500).json({ error: err.message });
      const nodes = (resp.nodes || []).map((n: any) => ({
        node_id: `${n.service_name}.${n.schema_name}.${n.table_name}`,
        node_type: n.node_type,
        schedule_name: n.schedule_name,
        status: typeof n.status === 'string' ? n.status.toLowerCase() : null,
        wait_reason: n.wait_reason || null,
        waiting_on_run_id: n.waiting_on_run_id || null,
        waiting_since: n.waiting_since || null,
      }));
      const edges = executionOrderEdges(resp.edges);
      res.json({
        nodes,
        edges,
        run_promotion_seq: Number(resp.run_promotion_seq ?? 0),
        latest_promotion_seq: Number(resp.latest_promotion_seq ?? 0),
      });
    });
  });

  return router;
}
