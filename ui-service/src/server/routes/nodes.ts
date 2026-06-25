import { Router } from 'express';
import * as grpc from '@grpc/grpc-js';
import { GrpcClient, userMetadata } from '../grpc-client';

function grpcToHttpStatus(code: number): number {
  switch (code) {
    case grpc.status.INVALID_ARGUMENT:
      return 400;
    case grpc.status.NOT_FOUND:
      return 404;
    case grpc.status.FAILED_PRECONDITION:
      return 409;
    default:
      return 500;
  }
}

export function createNodesRouter(stateClient: GrpcClient) {
  const router = Router();

  // GET /api/nodes?search=&service=&limit=&offset= — node catalog (paged)
  router.get('/', (req, res) => {
    const nullIfNeg = (n: any): number | null => {
      const v = Number(n ?? -1);
      return v < 0 ? null : v;
    };
    const intOrDefault = (raw: any, def: number, min: number): number => {
      const n = parseInt(String(raw), 10);
      return Number.isNaN(n) || n < min ? def : n;
    };
    const q = {
      search: typeof req.query.search === 'string' ? req.query.search : '',
      service_name: typeof req.query.service === 'string' ? req.query.service : '',
      limit: intOrDefault(req.query.limit, 50, 1),
      offset: intOrDefault(req.query.offset, 0, 0),
    };
    stateClient.listNodes(q, (err: any, response: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      res.json({
        total_count: Number(response.total_count ?? 0),
        nodes: (response.nodes || []).map((r: any) => ({
          service_name:     r.service_name,
          schema_name:      r.schema_name,
          table_name:       r.table_name,
          run_count:        Number(r.run_count ?? 0),
          success_rate_pct: nullIfNeg(r.success_rate_pct),
          avg_duration_sec: nullIfNeg(r.avg_duration_sec),
          p95_duration_sec: nullIfNeg(r.p95_duration_sec),
          flaky_rate_pct:   Number(r.flaky_rate_pct ?? 0),
          last_status:      r.last_status || null,
          last_run_at:      r.last_run_at || null,
        })),
      });
    });
  });

  // GET /api/nodes/names?service= — distinct node table names for search autocomplete
  router.get('/names', (req, res) => {
    const service = typeof req.query.service === 'string' ? req.query.service : '';
    stateClient.listNodeNames({ service_name: service }, (err: any, response: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      res.json({ names: response.table_names || [] });
    });
  });

  // GET /api/nodes/:service/:schema/:table/runs — last 50 raw runs on this node
  router.get('/:service/:schema/:table/runs', (req, res) => {
    stateClient.listNodeRuns(
      {
        service_name: req.params.service,
        schema_name:  req.params.schema,
        table_name:   req.params.table,
        limit:        50,
      },
      (err: any, response: any) => {
        if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        const runs = (response.runs || []).map((r: any) => ({
          run_id:           r.run_id,
          schedule_name:    r.schedule_name,
          kind:             r.kind,
          terminal_status:  (r.terminal_status || '').toLowerCase(),
          task_id:          r.task_id,
          task_status:      (r.task_status || '').toLowerCase(),
          retry_count:      Number(r.retry_count ?? 0),
          image_tag:        r.image_tag,
          manifest_version: r.manifest_version,
          created_at:       r.created_at   || null,
          started_at:       r.started_at   || null,
          completed_at:     r.completed_at || null,
          error_message:    r.error_message || null,
          log_s3_key:       r.log_s3_key   || null,
        }));
        res.json({ runs });
      },
    );
  });

  // POST /api/nodes/:service/:schema/:table/run
  // Body: {} → latest mode; { source_run_id } → snapshot_of_run mode.
  router.post('/:service/:schema/:table/run', (req, res) => {
    const sourceRunID: string = (req.body?.source_run_id ?? '').trim();
    const metadataSource = sourceRunID === '' ? 'latest' : 'snapshot_of_run';
    stateClient.triggerSingleNodeRun(
      {
        service_name:    req.params.service,
        schema_name:     req.params.schema,
        table_name:      req.params.table,
        metadata_source: metadataSource,
        source_run_id:   sourceRunID,
      },
      userMetadata(req),
      (err: any, response: any) => {
        if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        res.json({ run_id: response.run_id, schedule_name: response.schedule_name });
      },
    );
  });

  return router;
}
