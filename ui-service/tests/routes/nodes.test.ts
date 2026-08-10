import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import { createNodesRouter } from '../../src/server/routes/nodes';

const mockListNodeRuns = vi.fn();
const mockTriggerSingleNodeRun = vi.fn();
const mockListNodes = vi.fn();
const mockListNodeNames = vi.fn();
const mockGetNode = vi.fn();

const mockStateClient = {
  listNodeRuns: mockListNodeRuns,
  triggerSingleNodeRun: mockTriggerSingleNodeRun,
  listNodes: mockListNodes,
  listNodeNames: mockListNodeNames,
};

const mockGraphClient = { getNode: mockGetNode };

function makeApp() {
  const app = express();
  app.use(express.json());
  app.use('/api/nodes', createNodesRouter(mockStateClient as any, mockGraphClient as any));
  return app;
}

describe('nodes router', () => {
  beforeEach(() => {
    mockListNodeRuns.mockReset();
    mockTriggerSingleNodeRun.mockReset();
    mockListNodes.mockReset();
    mockListNodeNames.mockReset();
    mockGetNode.mockReset();
  });

  it('GET /:svc/:schema/:table/runs returns parsed rows', async () => {
    mockListNodeRuns.mockImplementation((_req, cb) =>
      cb(null, {
        runs: [
          {
            run_id: 'r1', schedule_name: 'daily', kind: 'cron',
            terminal_status: 'succeeded', task_id: 't1',
            task_status: 'succeeded', retry_count: 0,
            image_tag: 'v1', manifest_version: 'm1',
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
    expect(res.body.runs).toHaveLength(1);
    expect(res.body.runs[0].run_id).toBe('r1');
    expect(res.body.runs[0].kind).toBe('cron');
    // The structured result key must survive the DTO reconstruction: this route
    // rebuilds each run field by field, so a new state field is dropped unless
    // it is mapped explicitly.
    expect(res.body.runs[0].run_results_uri).toBe('run-results/task-executions/svc/schema/tbl/e1.json');
    expect(res.body.runs[0].retry_count).toBe(0);
    expect(res.body.runs[0].operation).toBe('run');
    expect(mockListNodeRuns).toHaveBeenCalledWith(
      { service_name: 'svc', schema_name: 'schema', table_name: 'tbl', limit: 50, operation: 'run' },
      expect.any(Function),
    );
  });

  it('GET /:svc/:schema/:table/runs rejects an unknown operation with 400', async () => {
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs?operation=bogus');
    expect(res.status).toBe(400);
    expect(res.body.error).toBe('invalid operation');
    expect(mockListNodeRuns).not.toHaveBeenCalled();
  });

  it('GET /:svc/:schema/:table/runs forwards operation=test to the state client', async () => {
    mockListNodeRuns.mockImplementation((_req, cb) => cb(null, { runs: [] }));
    await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs?operation=test');
    expect(mockListNodeRuns).toHaveBeenCalledWith(
      { service_name: 'svc', schema_name: 'schema', table_name: 'tbl', limit: 50, operation: 'test' },
      expect.any(Function),
    );
  });

  it('GET /:svc/:schema/:table/runs defaults a run missing operation to "run"', async () => {
    mockListNodeRuns.mockImplementation((_req, cb) =>
      cb(null, { runs: [{ run_id: 'r2', schedule_name: 'x', kind: 'cron' }] }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs');
    expect(res.body.runs[0].operation).toBe('run');
  });

  it('POST /:svc/:schema/:table/run (latest mode) calls TriggerSingleNodeRun', async () => {
    mockTriggerSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb(null, { run_id: 'new-r', schedule_name: 'single-node-run-abc12345' }),
    );

    const res = await request(makeApp())
      .post('/api/nodes/svc/schema/tbl/run')
      .send({});

    expect(res.status).toBe(200);
    expect(res.body.run_id).toBe('new-r');
    expect(mockTriggerSingleNodeRun).toHaveBeenCalledWith(
      {
        service_name: 'svc', schema_name: 'schema', table_name: 'tbl',
        metadata_source: 'latest', source_run_id: '', operation: '',
      },
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('POST /:svc/:schema/:table/run with source_run_id uses snapshot_of_run', async () => {
    mockTriggerSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb(null, { run_id: 'new-r', schedule_name: 'single-node-run-abc12345' }),
    );

    await request(makeApp())
      .post('/api/nodes/svc/schema/tbl/run')
      .send({ source_run_id: 'src-run-id' });

    expect(mockTriggerSingleNodeRun).toHaveBeenCalledWith(
      expect.objectContaining({
        metadata_source: 'snapshot_of_run',
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
            service_name: 'svc', schema_name: 'an', table_name: 'fct',
            run_count: 48, success_rate_pct: -1, avg_duration_sec: -1,
            p95_duration_sec: 21, flaky_rate_pct: 4,
            last_status: 'succeeded', last_run_at: '2026-06-08T10:00:00Z',
            operation: 'run',
          },
          {
            service_name: 'svc', schema_name: 'an', table_name: 'dim',
            run_count: 10, success_rate_pct: 90, avg_duration_sec: 5,
            p95_duration_sec: 9, flaky_rate_pct: 0,
            last_status: '', last_run_at: '', operation: 'test',
          },
        ],
      }),
    );

    const res = await request(makeApp()).get('/api/nodes?search=f&service=svc&limit=25&offset=0');
    expect(res.status).toBe(200);
    expect(res.body.total_count).toBe(2);
    expect(res.body.nodes).toHaveLength(2);
    expect(res.body.nodes[0].success_rate_pct).toBeNull(); // -1 -> null
    expect(res.body.nodes[0].avg_duration_sec).toBeNull();
    expect(res.body.nodes[0].p95_duration_sec).toBe(21);
    expect(res.body.nodes[0].flaky_rate_pct).toBe(4);
    expect(res.body.nodes[0].operation).toBe('run');
    expect(res.body.nodes[1].last_status).toBeNull();      // '' -> null
    expect(res.body.nodes[1].last_run_at).toBeNull();
    expect(res.body.nodes[1].operation).toBe('test');
    expect(mockListNodes).toHaveBeenCalledWith(
      { search: 'f', service_name: 'svc', operation: 'run', limit: 25, offset: 0 },
      expect.any(Function),
    );
  });

  it('GET / defaults missing/invalid query params', async () => {
    mockListNodes.mockImplementation((_req, cb) => cb(null, { total_count: 0, nodes: [] }));
    const res = await request(makeApp()).get('/api/nodes');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ total_count: 0, nodes: [] });
    expect(mockListNodes).toHaveBeenCalledWith(
      { search: '', service_name: '', operation: 'run', limit: 50, offset: 0 },
      expect.any(Function),
    );
  });

  it('GET / rejects an unknown operation with 400', async () => {
    const res = await request(makeApp()).get('/api/nodes?operation=bogus');
    expect(res.status).toBe(400);
    expect(res.body.error).toBe('invalid operation');
    expect(mockListNodes).not.toHaveBeenCalled();
  });

  it('GET / forwards operation=test to the state client', async () => {
    mockListNodes.mockImplementation((_req, cb) => cb(null, { total_count: 0, nodes: [] }));
    await request(makeApp()).get('/api/nodes?operation=test');
    expect(mockListNodes).toHaveBeenCalledWith(
      { search: '', service_name: '', operation: 'test', limit: 50, offset: 0 },
      expect.any(Function),
    );
  });

  it('GET / defaults a node missing operation to "run"', async () => {
    mockListNodes.mockImplementation((_req, cb) =>
      cb(null, { total_count: 1, nodes: [{ service_name: 'svc', schema_name: 'an', table_name: 'fct' }] }),
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
    mockTriggerSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb({ code: grpc.status.NOT_FOUND, message: 'source run missing' }),
    );

    const res = await request(makeApp())
      .post('/api/nodes/svc/schema/tbl/run')
      .send({ source_run_id: 'src-run-id' });

    expect(res.status).toBe(404);
    expect(res.body.error).toContain('source run missing');
  });

  it('GET /names returns distinct node names', async () => {
    mockListNodeNames.mockImplementation((_req, cb) => cb(null, { table_names: ['customers', 'orders'] }));
    const res = await request(makeApp()).get('/api/nodes/names?service=svc');
    expect(res.status).toBe(200);
    expect(res.body.names).toEqual(['customers', 'orders']);
    expect(mockListNodeNames).toHaveBeenCalledWith({ service_name: 'svc' }, expect.any(Function));
  });

  it('GET /:svc/:schema/:table/meta maps node metadata', async () => {
    mockGetNode.mockImplementation((_req, cb) =>
      cb(null, { node_type: 'dbt-model', test_count: 0, test_count_known: true }),
    );
    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/meta');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ node_type: 'dbt-model', test_count: 0, test_count_known: true });
    expect(mockGetNode).toHaveBeenCalledWith(
      { service_name: 'svc', schema_name: 'schema', table_name: 'tbl' },
      expect.any(Function),
    );
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
    mockTriggerSingleNodeRun.mockImplementation((_req, _md, cb) =>
      cb(null, { run_id: 'r', schedule_name: 'single-node-run-x' }),
    );
    await request(makeApp()).post('/api/nodes/svc/schema/tbl/run').send({ operation: 'build' });
    expect(mockTriggerSingleNodeRun).toHaveBeenCalledWith(
      expect.objectContaining({ operation: 'build', metadata_source: 'latest' }),
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('POST /run rejects a bad operation with 400', async () => {
    const res = await request(makeApp()).post('/api/nodes/svc/schema/tbl/run').send({ operation: 'nope' });
    expect(res.status).toBe(400);
    expect(mockTriggerSingleNodeRun).not.toHaveBeenCalled();
  });
});
