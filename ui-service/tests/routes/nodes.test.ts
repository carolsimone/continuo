import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import { createNodesRouter } from '../../src/server/routes/nodes';

const mockListNodeRuns = vi.fn();
const mockTriggerSingleNodeRun = vi.fn();

const mockStateClient = {
  listNodeRuns: mockListNodeRuns,
  triggerSingleNodeRun: mockTriggerSingleNodeRun,
};

function makeApp() {
  const app = express();
  app.use(express.json());
  app.use('/api/nodes', createNodesRouter(mockStateClient as any));
  return app;
}

describe('nodes router', () => {
  beforeEach(() => {
    mockListNodeRuns.mockReset();
    mockTriggerSingleNodeRun.mockReset();
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
            error_message: '', log_s3_key: '',
          },
        ],
      }),
    );

    const res = await request(makeApp()).get('/api/nodes/svc/schema/tbl/runs');
    expect(res.status).toBe(200);
    expect(res.body.runs).toHaveLength(1);
    expect(res.body.runs[0].run_id).toBe('r1');
    expect(res.body.runs[0].kind).toBe('cron');
    expect(res.body.runs[0].retry_count).toBe(0);
    expect(mockListNodeRuns).toHaveBeenCalledWith(
      { service_name: 'svc', schema_name: 'schema', table_name: 'tbl', limit: 50 },
      expect.any(Function),
    );
  });

  it('POST /:svc/:schema/:table/run (latest mode) calls TriggerSingleNodeRun', async () => {
    mockTriggerSingleNodeRun.mockImplementation((_req, cb) =>
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
        metadata_source: 'latest', source_run_id: '',
      },
      expect.any(Function),
    );
  });

  it('POST /:svc/:schema/:table/run with source_run_id uses snapshot_of_run', async () => {
    mockTriggerSingleNodeRun.mockImplementation((_req, cb) =>
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
      expect.any(Function),
    );
  });

  it('maps gRPC NOT_FOUND to HTTP 404', async () => {
    mockTriggerSingleNodeRun.mockImplementation((_req, cb) =>
      cb({ code: grpc.status.NOT_FOUND, message: 'source run missing' }),
    );

    const res = await request(makeApp())
      .post('/api/nodes/svc/schema/tbl/run')
      .send({ source_run_id: 'src-run-id' });

    expect(res.status).toBe(404);
    expect(res.body.error).toContain('source run missing');
  });
});
