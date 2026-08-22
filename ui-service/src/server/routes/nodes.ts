import { Router } from 'express';
import { GrpcClient, userMetadata } from '../grpc-client';
import { GrpcGraphClient } from '../grpc-graph-client';
import { grpcToHttpStatus } from './grpc-status';
import { parseLimit, parseOffset } from './paging';
import { parseOperation, parseNodeOperation } from './operation';

// A python-csv version's raw_code is the node's normalized contract entry
// serialized as JSON (the node has no script); the CSV location is its
// reads.csv field. Anything unparseable or missing yields "".
function csvSourceUri(rawCode: unknown): string {
  if (typeof rawCode !== 'string' || rawCode === '') return '';
  try {
    const uri = JSON.parse(rawCode)?.reads?.csv;
    return typeof uri === 'string' ? uri : '';
  } catch {
    return '';
  }
}

export function createNodesRouter(stateClient: GrpcClient, graphClient: GrpcGraphClient) {
  const router = Router();

  // GET /api/nodes?search=&service=&limit=&offset= — node catalog (paged)
  router.get('/', (req, res) => {
    const nullIfNeg = (n: any): number | null => {
      const v = Number(n ?? -1);
      return v < 0 ? null : v;
    };
    const operation = parseNodeOperation(req.query.operation);
    if (operation === null) return res.status(400).json({ error: 'invalid operation' });
    const q = {
      search: typeof req.query.search === 'string' ? req.query.search : '',
      service_name: typeof req.query.service === 'string' ? req.query.service : '',
      operation,
      limit: parseLimit(req.query.limit, { def: 50, max: 500 }),
      offset: parseOffset(req.query.offset),
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
          operation:        r.operation ?? 'run',
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
    const operation = parseNodeOperation(req.query.operation);
    if (operation === null) return res.status(400).json({ error: 'invalid operation' });
    stateClient.listNodeRuns(
      {
        service_name: req.params.service,
        schema_name:  req.params.schema,
        table_name:   req.params.table,
        limit:        50,
        operation,
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
          run_results_uri:  r.run_results_uri || null,
          operation:        r.operation ?? 'run',
        }));
        res.json({ runs });
      },
    );
  });

  // POST /api/nodes/:service/:schema/:table/run
  // Body: {} → latest mode; { source_run_id } → snapshot_of_run mode.
  router.post('/:service/:schema/:table/run', (req, res) => {
    const operation = parseOperation(req.body?.operation);
    if (operation === null) {
      return res.status(400).json({ error: 'operation must be one of "", run, test, build' });
    }
    const sourceRunID: string = (req.body?.source_run_id ?? '').trim();
    const metadataSource = sourceRunID === '' ? 'latest' : 'snapshot_of_run';
    stateClient.triggerSingleNodeRun(
      {
        service_name:    req.params.service,
        schema_name:     req.params.schema,
        table_name:      req.params.table,
        metadata_source: metadataSource,
        source_run_id:   sourceRunID,
        operation,
      },
      userMetadata(req),
      (err: any, response: any) => {
        if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        res.json({ run_id: response.run_id, schedule_name: response.schedule_name });
      },
    );
  });

  // GET /:service/:schema/:table/meta — per-node topology metadata (test_count)
  // used by the UI to gate single-node "test" on a zero-test node. For a
  // python-csv node the response also carries source_uri: the CSV file the
  // node loads, recorded as the current version's raw_code (a csv node has no
  // script). The lookup is best-effort — the test gate must keep working even
  // when the version query fails — so errors degrade to an empty source_uri.
  router.get('/:service/:schema/:table/meta', (req, res) => {
    graphClient.getNode(
      {
        service_name: req.params.service,
        schema_name:  req.params.schema,
        table_name:   req.params.table,
      },
      (err: any, response: any) => {
        if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        const meta = {
          node_type:        response.node_type || '',
          test_count:       Number(response.test_count ?? 0),
          test_count_known: Boolean(response.test_count_known),
        };
        if (meta.node_type !== 'python-csv') return res.json(meta);

        graphClient.getNodeVersions(
          {
            // unique_id is canonically lowercase ("<schema>.<table>") while
            // the route params carry the declared spelling, which GetNode
            // accepts but the exact-match version lookup does not.
            unique_id:    `${req.params.schema}.${req.params.table}`.toLowerCase(),
            current_only: true,
            include_code: true,
          },
          (verr: any, vres: any) => {
            const sourceUri = verr ? '' : csvSourceUri(vres?.versions?.[0]?.raw_code);
            res.json({ ...meta, source_uri: sourceUri });
          },
        );
      },
    );
  });

  return router;
}
