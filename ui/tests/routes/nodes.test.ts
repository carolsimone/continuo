import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import { createNodesRouter } from '../../src/server/routes/nodes';

const mockListNodeTasks = vi.fn();
const mockStartSingleNodeRun = vi.fn();
const mockListNodes = vi.fn();
const mockListNodeNames = vi.fn();
const mockListNodeProjects = vi.fn();
const mockGetNode = vi.fn();
const mockGetNodeVersions = vi.fn();
const mockGetNodeDataContract = vi.fn();
const mockListNodeDataContractVersions = vi.fn();
const mockGetNodeDataContractDiff = vi.fn();

const mockRunClient = {
  listNodeTasks: mockListNodeTasks,
  startSingleNodeRun: mockStartSingleNodeRun,
  listNodes: mockListNodes,
  listNodeNames: mockListNodeNames,
  listNodeProjects: mockListNodeProjects,
};

const REQUEST_ID = '5d21c0de-0000-4000-8000-000000000001';

const mockGraphClient = {
  getNode: mockGetNode,
  getNodeVersions: mockGetNodeVersions,
  getNodeDataContract: mockGetNodeDataContract,
  listNodeDataContractVersions: mockListNodeDataContractVersions,
  getNodeDataContractDiff: mockGetNodeDataContractDiff,
};

function makeApp() {
  const app = express();
  app.use(express.json());
  app.use('/api/nodes', createNodesRouter(mockRunClient as any, mockGraphClient as any));
  return app;
}

describe('nodes router', () => {
  beforeEach(() => {
    mockListNodeTasks.mockReset();
    mockStartSingleNodeRun.mockReset();
    mockListNodes.mockReset();
    mockListNodeNames.mockReset();
    mockListNodeProjects.mockReset();
    mockGetNode.mockReset();
    mockGetNodeVersions.mockReset();
    mockGetNodeDataContract.mockReset();
    mockListNodeDataContractVersions.mockReset();
    mockGetNodeDataContractDiff.mockReset();
  });

  it('GET /:svc/:schema/:table/runs returns the node\'s tasks under tasks', async () => {
    mockListNodeTasks.mockImplementation((_req, cb) =>
      cb(null, {
        tasks: [
          {
            run_id: 'r1', schedule_name: 'daily', kind: 'cron',
            run_status: 'succeeded', task_id: 't1',
            task_status: 'failed', attempt: 2,
            failure_class: 'user_code', failure_reason: 'compile_error',
            image_tag: 'v1',
            created_at: '2026-05-10T10:00:00Z',
            started_at: '2026-05-10T10:00:05Z',
            completed_at: '2026-05-10T10:01:00Z',
            error_message: '', log_s3_key: '', operation: 'run',
            run_results_uri: 'run-results/task-executions/svc/schema/tbl/e1.json',
          },
        ],
      }),
    );

    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs');
    expect(res.status).toBe(200);
    expect(res.body).not.toHaveProperty('runs');
    expect(res.body.tasks).toHaveLength(1);
    expect(res.body.tasks[0].run_id).toBe('r1');
    expect(res.body.tasks[0].kind).toBe('cron');
    expect(res.body.tasks[0].run_status).toBe('succeeded');
    expect(res.body.tasks[0].task_status).toBe('failed');
    expect(res.body.tasks[0]).not.toHaveProperty('terminal_status');
    // The structured result key must survive the DTO reconstruction: this route
    // rebuilds each run field by field, so a new run-controller field is dropped unless
    // it is mapped explicitly.
    expect(res.body.tasks[0].run_results_uri).toBe('run-results/task-executions/svc/schema/tbl/e1.json');
    expect(res.body.tasks[0].attempt).toBe(2);
    expect(Object.keys(res.body.tasks[0]).filter(k => k.startsWith('retry'))).toEqual([]);
    expect(res.body.tasks[0].failure_class).toBe('user_code');
    expect(res.body.tasks[0].failure_reason).toBe('compile_error');
    expect(res.body.tasks[0].operation).toBe('run');
    expect(mockListNodeTasks).toHaveBeenCalledWith(
      { project_name: 'svc', schema_name: 'schema', table_name: 'tbl', limit: 50, operation: 'run' },
      expect.any(Function),
    );
  });

  it('GET /:svc/:schema/:table/runs rejects an unknown operation with 400', async () => {
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs?operation=bogus');
    expect(res.status).toBe(400);
    expect(res.body.error).toBe('invalid operation');
    expect(mockListNodeTasks).not.toHaveBeenCalled();
  });

  it('GET /:svc/:schema/:table/runs forwards operation=test to the run-controller client', async () => {
    mockListNodeTasks.mockImplementation((_req, cb) => cb(null, { tasks: [] }));
    await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs?operation=test');
    expect(mockListNodeTasks).toHaveBeenCalledWith(
      { project_name: 'svc', schema_name: 'schema', table_name: 'tbl', limit: 50, operation: 'test' },
      expect.any(Function),
    );
  });

  it.each([
    ['an empty wire value', '', 'run'],
    ['full_refresh', 'full_refresh', 'full_refresh'],
    ['an unknown value from a newer backend', 'vacuum', 'vacuum'],
  ])('GET /:svc/:schema/:table/runs maps %s', async (_label, wire, want) => {
    mockListNodeTasks.mockImplementation((_req, cb) =>
      cb(null, { tasks: [{ run_id: 'r3', schedule_name: 'x', kind: 'cron', operation: wire }] }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs');
    expect(res.body.tasks[0].operation).toBe(want);
  });

  it('GET /:svc/:schema/:table/runs defaults a run missing operation to "run"', async () => {
    mockListNodeTasks.mockImplementation((_req, cb) =>
      cb(null, { tasks: [{ run_id: 'r2', schedule_name: 'x', kind: 'cron' }] }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs');
    expect(res.body.tasks[0].operation).toBe('run');
  });

  it('POST /:svc/:schema/:table/run (latest mode) calls StartSingleNodeRun', async () => {
    mockStartSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb(null, { run_id: 'new-r', schedule_name: 'single-node-run-abc12345' }),
    );

    const res = await request(makeApp())
      .post('/api/nodes/svc/schema/tbl/run')
      .send({ request_id: REQUEST_ID });

    expect(res.status).toBe(200);
    expect(res.body.run_id).toBe('new-r');
    expect(mockStartSingleNodeRun).toHaveBeenCalledWith(
      {
        request_id: REQUEST_ID, project_name: 'svc', schema_name: 'schema', table_name: 'tbl',
        metadata_source: 'latest', source_run_id: '', operation: '',
      },
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('POST /:svc/:schema/:table/run with source_run_id uses release_of_run', async () => {
    mockStartSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb(null, { run_id: 'new-r', schedule_name: 'single-node-run-abc12345' }),
    );

    await request(makeApp())
      .post('/api/nodes/svc/schema/tbl/run')
      .send({ request_id: REQUEST_ID, source_run_id: 'src-run-id' });

    expect(mockStartSingleNodeRun).toHaveBeenCalledWith(
      expect.objectContaining({
        request_id: REQUEST_ID,
        metadata_source: 'release_of_run',
        source_run_id: 'src-run-id',
      }),
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('GET / returns mapped catalog with total_count and -1 -> null', async () => {
    mockListNodes.mockImplementation((_req, cb) =>
      cb(null, {
        total_count: 2,
        nodes: [
          {
            project_name: 'svc', schema_name: 'an', table_name: 'fct',
            task_count: 48, success_rate_pct: -1, avg_duration_sec: -1,
            p95_duration_sec: 21, flaky_rate_pct: 4,
            last_status: 'succeeded', last_run_at: '2026-06-08T10:00:00Z',
            operation: 'run', timeout_seconds: 28800, timeout_declared: true,
            node_type: 'dbt-model', dormant: true,
          },
          {
            project_name: 'svc', schema_name: 'an', table_name: 'dim',
            task_count: 10, success_rate_pct: 90, avg_duration_sec: 5,
            p95_duration_sec: 9, flaky_rate_pct: 0,
            last_status: '', last_run_at: '', operation: 'test',
          },
        ],
      }),
    );

    const res = await request(makeApp()).get('/api/nodes?search=f&project=svc&limit=25&offset=0');
    expect(res.status).toBe(200);
    expect(res.body.total_count).toBe(2);
    expect(res.body.nodes).toHaveLength(2);
    expect(res.body.nodes[0].task_count).toBe(48);
    expect(res.body.nodes[0]).not.toHaveProperty('run_count');
    expect(res.body.nodes[0].success_rate_pct).toBeNull(); // -1 -> null
    expect(res.body.nodes[0].avg_duration_sec).toBeNull();
    expect(res.body.nodes[0].p95_duration_sec).toBe(21);
    expect(res.body.nodes[0].flaky_rate_pct).toBeUndefined(); // route no longer forwards it
    expect(res.body.nodes[0].operation).toBe('run');
    expect(res.body.nodes[0].timeout_seconds).toBe(28800);
    expect(res.body.nodes[0].timeout_declared).toBe(true);
    expect(res.body.nodes[1].timeout_seconds).toBe(0);     // unset -> 0
    expect(res.body.nodes[1].timeout_declared).toBe(false); // unset -> false (install default)
    expect(res.body.nodes[1].last_status).toBeNull();      // '' -> null
    expect(res.body.nodes[1].last_run_at).toBeNull();
    expect(res.body.nodes[1].operation).toBe('test');
    expect(res.body.nodes[0].node_type).toBe('dbt-model');
    expect(res.body.nodes[0].dormant).toBe(true);
    expect(res.body.nodes[1].node_type).toBe('');           // unset -> ''
    expect(res.body.nodes[1].dormant).toBe(false);          // unset -> false
    expect(mockListNodes).toHaveBeenCalledWith(
      { search: 'f', project_name: 'svc', operation: 'run', limit: 25, offset: 0, dormant_only: false },
      expect.any(Function),
    );
  });

  it('GET / sends dormant_only for ?dormant=1 and for no other value', async () => {
    mockListNodes.mockImplementation((_req, cb) => cb(null, { total_count: 0, nodes: [] }));
    await request(makeApp()).get('/api/nodes?dormant=1');
    await request(makeApp()).get('/api/nodes?dormant=0');
    await request(makeApp()).get('/api/nodes?dormant=true');
    expect(mockListNodes.mock.calls.map(([req]) => req.dormant_only)).toEqual([true, false, false]);
  });

  it('GET / lists a node that never ran with no stats, no status and no last run', async () => {
    mockListNodes.mockImplementation((_req, cb) =>
      cb(null, {
        total_count: 1,
        nodes: [{
          project_name: 'svc', schema_name: 'an', table_name: 'stg_backfill',
          task_count: 0, success_rate_pct: -1, avg_duration_sec: -1, p95_duration_sec: -1, flaky_rate_pct: 0,
          last_status: '', last_run_at: '', operation: 'run', timeout_seconds: 3600,
          node_type: 'dbt-model', dormant: true,
        }],
      }),
    );
    const res = await request(makeApp()).get('/api/nodes?dormant=1');
    expect(res.body.nodes[0]).toMatchObject({
      task_count: 0, success_rate_pct: null, avg_duration_sec: null, p95_duration_sec: null,
      last_status: null, last_run_at: null, node_type: 'dbt-model', dormant: true,
    });
  });

  it('GET / defaults missing/invalid query params', async () => {
    mockListNodes.mockImplementation((_req, cb) => cb(null, { total_count: 0, nodes: [] }));
    const res = await request(makeApp()).get('/api/nodes');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ total_count: 0, nodes: [] });
    expect(mockListNodes).toHaveBeenCalledWith(
      { search: '', project_name: '', operation: 'run', limit: 50, offset: 0, dormant_only: false },
      expect.any(Function),
    );
  });

  it.each([
    ['an empty wire value', '', 'run'],
    ['an absent wire value', undefined, 'run'],
    ['test', 'test', 'test'],
    ['an unknown value from a newer backend', 'vacuum', 'vacuum'],
  ])('GET / maps %s to the catalog operation', async (_label, wire, want) => {
    mockListNodes.mockImplementation((_req, cb) =>
      cb(null, { total_count: 1, nodes: [{ project_name: 'svc', schema_name: 'an', table_name: 'fct', operation: wire }] }),
    );
    const res = await request(makeApp()).get('/api/nodes');
    expect(res.body.nodes[0].operation).toBe(want);
  });

  it('GET / rejects an unknown operation with 400', async () => {
    const res = await request(makeApp()).get('/api/nodes?operation=bogus');
    expect(res.status).toBe(400);
    expect(res.body.error).toBe('invalid operation');
    expect(mockListNodes).not.toHaveBeenCalled();
  });

  it('GET / forwards operation=test to the run-controller client', async () => {
    mockListNodes.mockImplementation((_req, cb) => cb(null, { total_count: 0, nodes: [] }));
    await request(makeApp()).get('/api/nodes?operation=test');
    expect(mockListNodes).toHaveBeenCalledWith(
      { search: '', project_name: '', operation: 'test', limit: 50, offset: 0, dormant_only: false },
      expect.any(Function),
    );
  });

  it('GET / defaults a node missing operation to "run"', async () => {
    mockListNodes.mockImplementation((_req, cb) =>
      cb(null, { total_count: 1, nodes: [{ project_name: 'svc', schema_name: 'an', table_name: 'fct' }] }),
    );
    const res = await request(makeApp()).get('/api/nodes');
    expect(res.body.nodes[0].operation).toBe('run');
  });

  it('GET / maps a gRPC error to its HTTP status', async () => {
    mockListNodes.mockImplementation((_req, cb) =>
      cb({ code: grpc.status.INVALID_ARGUMENT, message: 'bad' }),
    );
    const res = await request(makeApp()).get('/api/nodes?limit=abc');
    expect(res.status).toBe(400);
    expect(res.body.error).toContain('bad');
  });

  it('maps gRPC NOT_FOUND to HTTP 404', async () => {
    mockStartSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb({ code: grpc.status.NOT_FOUND, message: 'source run missing' }),
    );

    const res = await request(makeApp())
      .post('/api/nodes/svc/schema/tbl/run')
      .send({ request_id: REQUEST_ID, source_run_id: 'src-run-id' });

    expect(res.status).toBe(404);
    expect(res.body.error).toContain('source run missing');
  });

  it('GET /names returns distinct node names', async () => {
    mockListNodeNames.mockImplementation((_req, cb) => cb(null, { table_names: ['customers', 'orders'] }));
    const res = await request(makeApp()).get('/api/nodes/names?project=svc');
    expect(res.status).toBe(200);
    expect(res.body.names).toEqual(['customers', 'orders']);
    expect(mockListNodeNames).toHaveBeenCalledWith({ project_name: 'svc' }, expect.any(Function));
  });

  it('GET /projects returns the distinct project list', async () => {
    mockListNodeProjects.mockImplementation((_req, cb) => cb(null, { project_names: ['billing', 'ledger'] }));
    const res = await request(makeApp()).get('/api/nodes/projects');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ projects: ['billing', 'ledger'] });
    expect(mockListNodeProjects).toHaveBeenCalledWith({}, expect.any(Function));
  });

  it('GET /projects defaults a missing project list to an empty array', async () => {
    mockListNodeProjects.mockImplementation((_req, cb) => cb(null, {}));
    const res = await request(makeApp()).get('/api/nodes/projects');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ projects: [] });
  });

  it('GET /projects maps a gRPC error to its HTTP status', async () => {
    mockListNodeProjects.mockImplementation((_req, cb) =>
      cb({ code: grpc.status.UNAVAILABLE, message: 'run-controller down' }),
    );
    const res = await request(makeApp()).get('/api/nodes/projects');
    expect(res.status).toBe(500);
    expect(res.body.error).toContain('run-controller down');
  });

  it('GET /:svc/:schema/:table/meta maps node metadata', async () => {
    mockGetNode.mockImplementation((_req, cb) =>
      cb(null, { node_type: 'dbt-model', test_count: 0, test_count_known: true }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ node_type: 'dbt-model', test_count: 0, test_count_known: true, inactive: false });
    expect(mockGetNode).toHaveBeenCalledWith(
      { project_name: 'svc', schema_name: 'schema', table_name: 'tbl', include_inactive: true },
      expect.any(Function),
    );
  });

  it('GET /meta reports a node a later release deactivated as inactive', async () => {
    // include_inactive keeps a deactivated node's type visible, so the page
    // can still offer a full refresh from one of its older releases.
    mockGetNode.mockImplementation((_req, cb) =>
      cb(null, { node_type: 'dbt-seed', test_count: 0, test_count_known: true, inactive: true }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ node_type: 'dbt-seed', test_count: 0, test_count_known: true, inactive: true });
  });

  it('GET /meta reports inactive=false when the response carries inactive=false', async () => {
    mockGetNode.mockImplementation((_req, cb) =>
      cb(null, { node_type: 'dbt-model', test_count: 1, test_count_known: true, inactive: false }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(res.status).toBe(200);
    expect(res.body.inactive).toBe(false);
  });

  it('GET /meta does not look up versions for a non-csv node', async () => {
    mockGetNode.mockImplementation((_req, cb) =>
      cb(null, { node_type: 'dbt-model', test_count: 2, test_count_known: true }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(res.status).toBe(200);
    expect(res.body).not.toHaveProperty('source_uri');
    expect(mockGetNodeVersions).not.toHaveBeenCalled();
  });

  it('GET /meta resolves a python-csv node source URI from its current version', async () => {
    // A python-csv version's raw_code is the normalized contract entry as
    // JSON (the node has no script); the CSV location is its reads.csv field.
    mockGetNode.mockImplementation((_req, cb) =>
      cb(null, { node_type: 'python-csv', test_count: 0, test_count_known: true }),
    );
    mockGetNodeVersions.mockImplementation((_req, cb) =>
      cb(null, {
        versions: [{
          raw_code: JSON.stringify({
            schema: 'schema', table: 'tbl', kind: 'python-csv',
            extra_columns: 'raise',
            reads: { csv: 's3://bucket/vendor/orders.csv' },
            output_columns: [],
          }),
        }],
      }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(res.status).toBe(200);
    expect(res.body.node_type).toBe('python-csv');
    expect(res.body.source_uri).toBe('s3://bucket/vendor/orders.csv');
    expect(mockGetNodeVersions).toHaveBeenCalledWith(
      { unique_id: 'schema.tbl', current_only: true, include_code: true },
      expect.any(Function),
    );
  });

  it('GET /meta lowercases the version lookup key for a node declared with uppercase spelling', async () => {
    // unique_id is canonically lowercase ("<schema>.<table>") while GetNode
    // works on the declared spelling; a preserved-case key would NOT_FOUND.
    mockGetNode.mockImplementation((_req, cb) =>
      cb(null, { node_type: 'python-csv', test_count: 0, test_count_known: true }),
    );
    mockGetNodeVersions.mockImplementation((_req, cb) =>
      cb(null, { versions: [{ raw_code: JSON.stringify({ reads: { csv: 'https://x/y.csv' } }) }] }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/Analytics/Vendor_Feed/meta');
    expect(res.status).toBe(200);
    expect(res.body.source_uri).toBe('https://x/y.csv');
    expect(mockGetNodeVersions).toHaveBeenCalledWith(
      { unique_id: 'analytics.vendor_feed', current_only: true, include_code: true },
      expect.any(Function),
    );
  });

  it('GET /meta returns an empty source_uri when raw_code is not contract JSON or lacks reads.csv', async () => {
    mockGetNode.mockImplementation((_req, cb) =>
      cb(null, { node_type: 'python-csv', test_count: 0, test_count_known: true }),
    );
    mockGetNodeVersions.mockImplementation((_req, cb) =>
      cb(null, { versions: [{ raw_code: 'not json at all' }] }),
    );
    const bad = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(bad.status).toBe(200);
    expect(bad.body.source_uri).toBe('');

    mockGetNodeVersions.mockImplementation((_req, cb) =>
      cb(null, { versions: [{ raw_code: JSON.stringify({ reads: {} }) }] }),
    );
    const missing = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(missing.status).toBe(200);
    expect(missing.body.source_uri).toBe('');
  });

  it('GET /meta degrades to an empty source_uri when the version lookup fails', async () => {
    // The meta endpoint's primary job is gating the test button; a broken
    // version lookup must not take the whole node page down with it.
    mockGetNode.mockImplementation((_req, cb) =>
      cb(null, { node_type: 'python-csv', test_count: 0, test_count_known: true }),
    );
    mockGetNodeVersions.mockImplementation((_req, cb) =>
      cb({ code: grpc.status.UNAVAILABLE, message: 'down' }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(res.status).toBe(200);
    expect(res.body.source_uri).toBe('');
  });

  it('GET /meta returns an empty source_uri for a csv node with no current version', async () => {
    mockGetNode.mockImplementation((_req, cb) =>
      cb(null, { node_type: 'python-csv', test_count: 0, test_count_known: true }),
    );
    mockGetNodeVersions.mockImplementation((_req, cb) => cb(null, { versions: [] }));
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(res.status).toBe(200);
    expect(res.body.source_uri).toBe('');
  });

  it('GET /meta maps gRPC NOT_FOUND to 404', async () => {
    mockGetNode.mockImplementation((_req, cb) =>
      cb({ code: grpc.status.NOT_FOUND, message: 'node missing' }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(res.status).toBe(404);
    expect(res.body.error).toContain('node missing');
  });

  it('POST /run forwards operation=build', async () => {
    mockStartSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb(null, { run_id: 'r', schedule_name: 'single-node-run-x' }),
    );
    await request(makeApp()).post('/api/nodes/svc/schema/tbl/run').send({ request_id: REQUEST_ID, operation: 'build' });
    expect(mockStartSingleNodeRun).toHaveBeenCalledWith(
      expect.objectContaining({ operation: 'build', metadata_source: 'latest' }),
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('POST /run forwards operation=full_refresh', async () => {
    mockStartSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb(null, { run_id: 'r', schedule_name: 'single-node-run-x' }),
    );
    await request(makeApp()).post('/api/nodes/svc/schema/tbl/run').send({ request_id: REQUEST_ID, operation: 'full_refresh' });
    expect(mockStartSingleNodeRun).toHaveBeenCalledWith(
      expect.objectContaining({ operation: 'full_refresh', metadata_source: 'latest' }),
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('POST /run rejects a bad operation with 400', async () => {
    const res = await request(makeApp()).post('/api/nodes/svc/schema/tbl/run').send({ request_id: REQUEST_ID, operation: 'nope' });
    expect(res.status).toBe(400);
    expect(mockStartSingleNodeRun).not.toHaveBeenCalled();
  });

  it.each([
    ['no body', undefined],
    ['an empty body', {}],
    ['an empty request_id', { request_id: '' }],
    ['a request_id that is not a UUID', { request_id: 'nope', operation: 'build' }],
  ])('POST /run refuses a missing or malformed request_id with 400: %s', async (_label, body) => {
    const req = request(makeApp()).post('/api/nodes/svc/schema/tbl/run');
    const res = await (body === undefined ? req : req.send(body));
    expect(res.status).toBe(400);
    expect(res.body).toEqual({ error: 'request_id must be a UUID' });
    expect(mockStartSingleNodeRun).not.toHaveBeenCalled();
  });

  it('POST /run maps ALREADY_EXISTS (request_id reused with different parameters) to 409', async () => {
    mockStartSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb({ code: grpc.status.ALREADY_EXISTS, message: 'request_id reused with different parameters' }),
    );
    const res = await request(makeApp()).post('/api/nodes/svc/schema/tbl/run').send({ request_id: REQUEST_ID });
    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/request_id reused/);
  });

  it('POST /run forwards the acknowledged objects of a full refresh', async () => {
    mockStartSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb(null, { run_id: 'r', schedule_name: 'single-node-run-x' }),
    );
    const res = await request(makeApp()).post('/api/nodes/svc/schema/tbl/run').send({
      request_id: REQUEST_ID, operation: 'full_refresh', acknowledged_dependents: ['e2e_bi.seed_reader'],
    });
    expect(res.status).toBe(200);
    expect(mockStartSingleNodeRun).toHaveBeenCalledWith(
      expect.objectContaining({ operation: 'full_refresh', acknowledged_dependents: ['e2e_bi.seed_reader'] }),
      expect.any(Object),
      expect.any(Function),
    );
  });

  it.each([
    ['a string', 'e2e_bi.seed_reader'],
    ['an object', { a: 1 }],
    ['an array holding a non-string', ['e2e_bi.seed_reader', 3]],
  ])('POST /run rejects acknowledged_dependents that is %s with 400', async (_label, value) => {
    const res = await request(makeApp()).post('/api/nodes/svc/schema/tbl/run').send({
      request_id: REQUEST_ID, operation: 'full_refresh', acknowledged_dependents: value,
    });
    expect(res.status).toBe(400);
    expect(mockStartSingleNodeRun).not.toHaveBeenCalled();
  });

  it('GET /:svc/:schema/:table/runs returns the node outage and each run blocking objects', async () => {
    mockListNodeTasks.mockImplementation((_req, cb) => cb(null, {
      tasks: [
        { run_id: 'r1', schedule_name: 'x', kind: 'promote_seed', blocking_objects: ['e2e_bi.seed_reader'] },
        { run_id: 'r2', schedule_name: 'x', kind: 'cron', blocking_objects: [] },
        { run_id: 'r3', schedule_name: 'x', kind: 'cron' },
      ],
      outage: { state: 'missing', since: '2026-11-24T09:16:02Z', seed_node_id: 'analytics.seed_users', run_id: 'r1' },
    }));
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs');
    expect(res.body.outage).toEqual({
      state: 'missing', since: '2026-11-24T09:16:02Z', seed_node_id: 'analytics.seed_users', run_id: 'r1',
    });
    expect(res.body.tasks.map((t: any) => t.blocking_objects)).toEqual([['e2e_bi.seed_reader'], [], []]);
  });

  it('GET /:svc/:schema/:table/runs gives outage null when the node has none', async () => {
    mockListNodeTasks.mockImplementation((_req, cb) => cb(null, { tasks: [], outage: null }));
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs');
    expect(res.body.outage).toBeNull();
  });

  // A catalog row as the proto loader hands it over: int64 fields are strings
  // and an unset field holds its default ('' or '0').
  const contractRow = (over: Record<string, unknown> = {}) => ({
    unique_id: 'analytics.py_daily_kpis',
    version: '3',
    contract_hash: 'sha256:aaa',
    document_yaml: '',
    release_id: 'rel-77aa0bc-230',
    promoted_at: '2026-11-27T16:40:12Z',
    content_hash: 'sha256:c3',
    is_current: true,
    same_as_version: '1',
    continuo_contracts: '0.2.0',
    ontology_version: '',
    ...over,
  });

  describe('GET /:svc/:schema/:table/data-contract', () => {
    it('returns the current contract with its YAML and the version history', async () => {
      mockGetNodeDataContract.mockImplementation((_req, cb) =>
        cb(null, { contract: contractRow({ document_yaml: 'id: analytics.py_daily_kpis\n' }) }));
      mockListNodeDataContractVersions.mockImplementation((_req, cb) => cb(null, {
        versions: [
          contractRow(),
          contractRow({ version: '2', is_current: false, same_as_version: '0', release_id: 'rel-5d6e7f8-219' }),
          contractRow({ version: '1', is_current: false, same_as_version: '0', release_id: 'rel-4e5f6a7-212' }),
        ],
      }));

      const res = await request(makeApp()).get('/api/nodes/py-service/analytics/py_daily_kpis/data-contract');

      expect(res.status).toBe(200);
      expect(res.body.contract).toEqual({
        unique_id: 'analytics.py_daily_kpis',
        version: 3,
        contract_hash: 'sha256:aaa',
        document_yaml: 'id: analytics.py_daily_kpis\n',
        release_id: 'rel-77aa0bc-230',
        promoted_at: '2026-11-27T16:40:12Z',
        content_hash: 'sha256:c3',
        is_current: true,
        same_as_version: 1,
        continuo_contracts: '0.2.0',
        ontology_version: '',
      });
      expect(res.body.versions.map((v: any) => [v.version, v.is_current, v.same_as_version])).toEqual([
        [3, true, 1],
        [2, false, null],
        [1, false, null],
      ]);
      expect(res.body.versions[0]).not.toHaveProperty('document_yaml');
      expect(mockGetNodeDataContract).toHaveBeenCalledWith(
        { unique_id: 'analytics.py_daily_kpis', include_document: true },
        expect.any(Function),
      );
      expect(mockListNodeDataContractVersions).toHaveBeenCalledWith(
        { unique_id: 'analytics.py_daily_kpis', limit: 0 },
        expect.any(Function),
      );
    });

    it('addresses the node by its lower-case unique id, whatever the spelling of the route', async () => {
      mockGetNodeDataContract.mockImplementation((_req, cb) => cb({ code: grpc.status.NOT_FOUND, message: 'none' }));
      mockListNodeDataContractVersions.mockImplementation((_req, cb) => cb(null, { versions: [] }));

      await request(makeApp()).get('/api/nodes/py-service/Analytics/Py_Daily_KPIs/data-contract');

      expect(mockGetNodeDataContract.mock.calls[0][0].unique_id).toBe('analytics.py_daily_kpis');
      expect(mockListNodeDataContractVersions.mock.calls[0][0].unique_id).toBe('analytics.py_daily_kpis');
    });

    it('answers a node with no contract with null and an empty history', async () => {
      mockGetNodeDataContract.mockImplementation((_req, cb) =>
        cb({ code: grpc.status.NOT_FOUND, message: 'node has no data contract' }));
      mockListNodeDataContractVersions.mockImplementation((_req, cb) => cb(null, { versions: [] }));

      const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/data-contract');

      expect(res.status).toBe(200);
      expect(res.body).toEqual({ contract: null, versions: [] });
    });

    it('keeps the history of a node that has no current contract', async () => {
      mockGetNodeDataContract.mockImplementation((_req, cb) =>
        cb({ code: grpc.status.NOT_FOUND, message: 'node is retired' }));
      mockListNodeDataContractVersions.mockImplementation((_req, cb) => cb(null, {
        versions: [contractRow({ version: '2', is_current: false, same_as_version: '0' })],
      }));

      const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/data-contract');

      expect(res.status).toBe(200);
      expect(res.body.contract).toBeNull();
      expect(res.body.versions).toHaveLength(1);
      expect(res.body.versions[0].version).toBe(2);
    });

    it('passes the status of any other catalog failure on and reads no history', async () => {
      mockGetNodeDataContract.mockImplementation((_req, cb) =>
        cb({ code: grpc.status.UNAVAILABLE, message: 'catalog down' }));

      const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/data-contract');

      expect(res.status).toBe(500);
      expect(res.body.error).toBe('catalog down');
      expect(mockListNodeDataContractVersions).not.toHaveBeenCalled();
    });

    it('passes on the status of a failed history read', async () => {
      mockGetNodeDataContract.mockImplementation((_req, cb) => cb(null, { contract: contractRow() }));
      mockListNodeDataContractVersions.mockImplementation((_req, cb) =>
        cb({ code: grpc.status.UNAVAILABLE, message: 'history down' }));

      const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/data-contract');

      expect(res.status).toBe(500);
      expect(res.body.error).toBe('history down');
    });
  });

  describe('GET /:svc/:schema/:table/data-contract/diff', () => {
    it('returns the fields two versions differ at', async () => {
      mockGetNodeDataContractDiff.mockImplementation((_req, cb) => cb(null, {
        changes: [{ path: 'team.name', before_json: '"service-py"', after_json: '"analytics-eng"' }],
      }));

      const res = await request(makeApp()).get('/api/nodes/py-service/analytics/py_daily_kpis/data-contract/diff?from=1&to=2');

      expect(res.status).toBe(200);
      expect(res.body).toEqual({
        changes: [{ path: 'team.name', before_json: '"service-py"', after_json: '"analytics-eng"' }],
      });
      expect(mockGetNodeDataContractDiff).toHaveBeenCalledWith(
        { unique_id: 'analytics.py_daily_kpis', from_version: 1, to_version: 2 },
        expect.any(Function),
      );
    });

    it('returns an empty list for two equal versions', async () => {
      mockGetNodeDataContractDiff.mockImplementation((_req, cb) => cb(null, {}));
      const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/data-contract/diff?from=1&to=3');
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ changes: [] });
    });

    it.each([
      ['a missing from', 'to=2'],
      ['a missing to', 'from=1'],
      ['a word', 'from=a&to=2'],
      ['a fraction', 'from=1&to=2.5'],
      ['an empty value', 'from=&to=2'],
      ['a signed number', 'from=-1&to=2'],
      ['a version below 1', 'from=0&to=2'],
      ['a number past what a version can be', 'from=1&to=99999999999999999999'],
    ])('rejects %s with 400 and asks catalog nothing', async (_label, query) => {
      const res = await request(makeApp()).get(`/api/nodes/svc/schema/tbl/data-contract/diff?${query}`);
      expect(res.status).toBe(400);
      expect(res.body.error).toMatch(/whole number/);
      expect(mockGetNodeDataContractDiff).not.toHaveBeenCalled();
    });

    it('passes on a version catalog does not have as 404', async () => {
      mockGetNodeDataContractDiff.mockImplementation((_req, cb) =>
        cb({ code: grpc.status.NOT_FOUND, message: 'no version 9' }));
      const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/data-contract/diff?from=1&to=9');
      expect(res.status).toBe(404);
      expect(res.body.error).toBe('no version 9');
    });
  });
});
