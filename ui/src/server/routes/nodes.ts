import { Router } from 'express';
import * as grpc from '@grpc/grpc-js';
import { GrpcClient, userMetadata } from '../clients/grpc-client';
import { GrpcGraphClient } from '../clients/grpc-graph-client';
import { grpcToHttpStatus, sendGrpcError } from './grpc-status';
import { requireRequestId } from './request-id';
import { parseLimit, parseOffset } from './paging';
import { operationFromWire } from '../shared/operation';
import { parseNodeOperation, parseNodeRunOperation } from './operation';
import { type NodeType } from '../generated/vocabulary.gen';

const PYTHON_CSV: NodeType = 'python-csv';

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

// A contract version as the browser reads it. catalog sends its int64 fields as
// strings; same_as_version is null unless the version reverts to an earlier one.
function contractVersionDto(v: any) {
  const sameAs = Number(v.same_as_version ?? 0);
  return {
    unique_id:          v.unique_id,
    version:            Number(v.version ?? 0),
    contract_hash:      v.contract_hash,
    release_id:         v.release_id,
    promoted_at:        v.promoted_at || null,
    content_hash:       v.content_hash,
    is_current:         Boolean(v.is_current),
    same_as_version:    sameAs > 0 ? sameAs : null,
    continuo_contracts: v.continuo_contracts,
    ontology_version:   v.ontology_version,
  };
}

// A contract version is a whole number from 1 up that fits in a JS integer.
function parseContractVersion(raw: unknown): number | null {
  if (typeof raw !== 'string' || !/^[1-9][0-9]*$/.test(raw)) return null;
  const n = Number(raw);
  return Number.isSafeInteger(n) ? n : null;
}

export function createNodesRouter(runClient: GrpcClient, graphClient: GrpcGraphClient) {
  const router = Router();

  // GET /api/nodes?search=&project=&operation=&dormant=&limit=&offset= — every
  // node of the live release, paged, with the stats of its recent runs; a node
  // that never ran has none. dormant=1 lists only dormant nodes (the ones that
  // belong to no schedule).
  router.get('/', (req, res) => {
    const nullIfNeg = (n: any): number | null => {
      const v = Number(n ?? -1);
      return v < 0 ? null : v;
    };
    const operation = parseNodeOperation(req.query.operation);
    if (operation === null) return res.status(400).json({ error: 'invalid operation' });
    const q = {
      search: typeof req.query.search === 'string' ? req.query.search : '',
      project_name: typeof req.query.project === 'string' ? req.query.project : '',
      operation,
      limit: parseLimit(req.query.limit, { def: 50, max: 500 }),
      offset: parseOffset(req.query.offset),
      dormant_only: req.query.dormant === '1',
    };
    runClient.listNodes(q, (err: any, response: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      res.json({
        total_count: Number(response.total_count ?? 0),
        nodes: (response.nodes || []).map((r: any) => ({
          project_name:     r.project_name,
          schema_name:      r.schema_name,
          table_name:       r.table_name,
          task_count:       Number(r.task_count ?? 0),
          success_rate_pct: nullIfNeg(r.success_rate_pct),
          avg_duration_sec: nullIfNeg(r.avg_duration_sec),
          p95_duration_sec: nullIfNeg(r.p95_duration_sec),
          last_status:      r.last_status || null,
          last_run_at:      r.last_run_at || null,
          operation:        operationFromWire(r.operation),
          timeout_seconds:  Number(r.timeout_seconds ?? 0),
          timeout_declared: Boolean(r.timeout_declared),
          node_type:        r.node_type || '',
          dormant:          Boolean(r.dormant),
        })),
      });
    });
  });

  // GET /api/nodes/names?project= — the distinct table names of the nodes the
  // node list lists (the live release's nodes continuo builds), for the search
  // autocomplete
  router.get('/names', (req, res) => {
    const project = typeof req.query.project === 'string' ? req.query.project : '';
    runClient.listNodeNames({ project_name: project }, (err: any, response: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      res.json({ names: response.table_names || [] });
    });
  });

  // GET /api/nodes/projects — the distinct project names of the nodes the node
  // list lists, for the Nodes-tab and Remediation-tab project filters. Backed
  // by the ListNodeProjects run-controller read (a single DISTINCT query),
  // independent of the paginated node list.
  router.get('/projects', (_req, res) => {
    runClient.listNodeProjects({}, (err: any, response: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      res.json({ projects: response.project_names || [] });
    });
  });

  // GET /api/nodes/:project/:schema/:table/runs — last 50 raw runs on this node
  router.get('/:project/:schema/:table/runs', (req, res) => {
    const operation = parseNodeOperation(req.query.operation);
    if (operation === null) return res.status(400).json({ error: 'invalid operation' });
    runClient.listNodeTasks(
      {
        project_name: req.params.project,
        schema_name:  req.params.schema,
        table_name:   req.params.table,
        limit:        50,
        operation,
      },
      (err: any, response: any) => {
        if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        const tasks = (response.tasks || []).map((r: any) => ({
          run_id:           r.run_id,
          schedule_name:    r.schedule_name,
          kind:             r.kind,
          run_status:       r.run_status,
          task_id:          r.task_id,
          task_status:      r.task_status,
          attempt:          Number(r.attempt ?? 0),
          failure_class:    r.failure_class  || null,
          failure_reason:   r.failure_reason || null,
          image_tag:        r.image_tag,
          created_at:       r.created_at   || null,
          started_at:       r.started_at   || null,
          completed_at:     r.completed_at || null,
          error_message:    r.error_message || null,
          log_s3_key:       r.log_s3_key   || null,
          run_results_uri:  r.run_results_uri || null,
          operation:        operationFromWire(r.operation),
          blocking_objects: r.blocking_objects ?? [],
        }));
        const outage = response.outage
          ? {
              state:        response.outage.state,
              since:        response.outage.since || null,
              seed_node_id: response.outage.seed_node_id,
              run_id:       response.outage.run_id,
            }
          : null;
        res.json({ tasks, outage });
      },
    );
  });

  // POST /api/nodes/:project/:schema/:table/run
  // Body: { request_id } → latest mode; { request_id, source_run_id } →
  // snapshot_of_run mode. request_id is the browser-minted UUID run-controller uses to
  // start one run for a repeated action. acknowledged_dependents (an array of
  // "<schema>.<name>") names the warehouse objects an operator accepts a seed's
  // full refresh dropping or leaving stale.
  router.post('/:project/:schema/:table/run', (req, res) => {
    const requestId = requireRequestId(req, res);
    if (requestId === null) return;
    const operation = parseNodeRunOperation(req.body?.operation);
    if (operation === null) {
      return res.status(400).json({ error: 'operation must be one of "", run, test, build, full_refresh' });
    }
    const acknowledged = req.body?.acknowledged_dependents;
    if (acknowledged !== undefined && !(Array.isArray(acknowledged) && acknowledged.every((o) => typeof o === 'string'))) {
      return res.status(400).json({ error: 'acknowledged_dependents must be an array of strings' });
    }
    const sourceRunID: string = (req.body?.source_run_id ?? '').trim();
    const metadataSource = sourceRunID === '' ? 'latest' : 'snapshot_of_run';
    runClient.triggerSingleNodeRun(
      {
        request_id:      requestId,
        project_name:    req.params.project,
        schema_name:     req.params.schema,
        table_name:      req.params.table,
        metadata_source: metadataSource,
        source_run_id:   sourceRunID,
        operation,
        ...(acknowledged === undefined ? {} : { acknowledged_dependents: acknowledged }),
      },
      userMetadata(req),
      (err: any, response: any) => {
        if (err) return sendGrpcError(res, err);
        res.json({ run_id: response.run_id, schedule_name: response.schedule_name });
      },
    );
  });

  // GET /:project/:schema/:table/meta — per-node topology metadata (node_type,
  // test_count) used by the UI to gate single-node operations: "test" on a
  // zero-test node, and "full refresh" on a node type that cannot be rebuilt.
  // The lookup includes nodes no longer active in the topology (reported with
  // inactive: true), because a full refresh from one of their old snapshots
  // is still valid. For a python-csv node the response also carries
  // source_uri: the CSV file the node loads, recorded as the current version's
  // raw_code (a csv node has no script). The lookup is best-effort — the test
  // gate must keep working even when the version query fails — so errors
  // degrade to an empty source_uri.
  router.get('/:project/:schema/:table/meta', (req, res) => {
    graphClient.getNode(
      {
        project_name:     req.params.project,
        schema_name:      req.params.schema,
        table_name:       req.params.table,
        include_inactive: true,
      },
      (err: any, response: any) => {
        if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        const meta = {
          node_type:        response.node_type || '',
          test_count:       Number(response.test_count ?? 0),
          test_count_known: Boolean(response.test_count_known),
          inactive:         Boolean(response.inactive),
        };
        if (meta.node_type !== PYTHON_CSV) return res.json(meta);

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

  // GET /:project/:schema/:table/contract — the contract the node has now (with
  // its published YAML) and its version history, newest first. A node with no
  // current contract answers contract: null: a node that never had one has an
  // empty history, a retired node keeps the versions it had. The node is
  // addressed by its unique id, "<schema>.<table>" in lower case, as catalog
  // stores it.
  router.get('/:project/:schema/:table/contract', (req, res) => {
    const uniqueId = `${req.params.schema}.${req.params.table}`.toLowerCase();
    graphClient.getNodeContract({ unique_id: uniqueId, include_document: true }, (err: any, current: any) => {
      if (err && err.code !== grpc.status.NOT_FOUND) {
        return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      }
      graphClient.listNodeContractVersions({ unique_id: uniqueId, limit: 0 }, (lerr: any, listed: any) => {
        if (lerr) return res.status(grpcToHttpStatus(lerr.code)).json({ error: lerr.message });
        const contract = err || !current?.contract
          ? null
          : { ...contractVersionDto(current.contract), document_yaml: current.contract.document_yaml ?? '' };
        res.json({ contract, versions: (listed?.versions || []).map(contractVersionDto) });
      });
    });
  });

  // GET /:project/:schema/:table/contract/diff?from=<n>&to=<n> — the contract
  // fields that differ between two versions of the node, one row per changed
  // path; two equal versions have none. before_json and after_json are the
  // value at the path as JSON text, empty where the version lacks the path.
  router.get('/:project/:schema/:table/contract/diff', (req, res) => {
    const from = parseContractVersion(req.query.from);
    const to = parseContractVersion(req.query.to);
    if (from === null || to === null) {
      return res.status(400).json({ error: 'from and to must be whole numbers from 1 up' });
    }
    graphClient.getNodeContractDiff(
      {
        unique_id:    `${req.params.schema}.${req.params.table}`.toLowerCase(),
        from_version: from,
        to_version:   to,
      },
      (err: any, response: any) => {
        if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
        res.json({
          changes: (response?.changes || []).map((c: any) => ({
            path:        c.path,
            before_json: c.before_json,
            after_json:  c.after_json,
          })),
        });
      },
    );
  });

  return router;
}
