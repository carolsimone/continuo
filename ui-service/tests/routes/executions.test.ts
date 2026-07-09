import request from 'supertest';
import { describe, it, expect, vi, beforeAll } from 'vitest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import { createExecutionsRouter } from '../../src/server/routes/executions';

const mockStateClient = {
  getScheduler: vi.fn(),
  listSchedulers: vi.fn(),
  listTasks: vi.fn(),
  listTaskExecutions: vi.fn(),
};

const app = express();
app.use('/api/schedulers', createExecutionsRouter(mockStateClient as any));

describe('GET /api/schedulers/:id/executions', () => {
  beforeAll(() => {
    mockStateClient.listTaskExecutions.mockImplementation((_req: any, cb: any) => {
      cb(null, {
        task_executions: [
          {
            id: 'exec-1',
            task_id: 'task-1',
            error_message: 'boom',
            execution_time_seconds: 1.5,
            started_at: null,
            completed_at: null,
            log_s3_key: 'logs/task-executions/svc/sc/tbl/exec-1.log',
          },
        ],
        total_count: 1,
      });
    });
  });

  it('returns executions', async () => {
    const res = await request(app).get('/api/schedulers/abc-123/executions');
    expect(res.status).toBe(200);
    expect(res.body.executions).toHaveLength(1);
    expect(res.body.executions[0].task_id).toBe('task-1');
    expect(res.body.executions[0].error_message).toBe('boom');
    expect(res.body.executions[0].log_s3_key).toBe('logs/task-executions/svc/sc/tbl/exec-1.log');
  });

  it('returns 500 on gRPC error', async () => {
    mockStateClient.listTaskExecutions.mockImplementationOnce((_req: any, cb: any) => {
      cb(new Error('grpc error'), null);
    });
    const res = await request(app).get('/api/schedulers/bad-id/executions');
    expect(res.status).toBe(500);
  });

  it('defaults to page_size 200 and page_offset 0 when no query params', async () => {
    mockStateClient.listTaskExecutions.mockImplementationOnce((_req: any, cb: any) => {
      cb(null, { task_executions: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/executions');

    expect(mockStateClient.listTaskExecutions).toHaveBeenCalledWith(
      expect.objectContaining({ schedule_id: 'abc-123', page_size: 200, page_offset: 0 }),
      expect.any(Function)
    );
  });

  it('forwards limit and offset query params', async () => {
    mockStateClient.listTaskExecutions.mockImplementationOnce((_req: any, cb: any) => {
      cb(null, { task_executions: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/executions?limit=50&offset=200');

    expect(mockStateClient.listTaskExecutions).toHaveBeenCalledWith(
      expect.objectContaining({ page_size: 50, page_offset: 200 }),
      expect.any(Function)
    );
  });

  it('clamps limit to the state-side max of 200', async () => {
    mockStateClient.listTaskExecutions.mockImplementationOnce((_req: any, cb: any) => {
      cb(null, { task_executions: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/executions?limit=500');

    expect(mockStateClient.listTaskExecutions).toHaveBeenCalledWith(
      expect.objectContaining({ page_size: 200 }),
      expect.any(Function)
    );
  });

  it('returns total_count in the body', async () => {
    mockStateClient.listTaskExecutions.mockImplementationOnce((_req: any, cb: any) => {
      cb(null, { task_executions: [], total_count: 412 });
    });

    const res = await request(app).get('/api/schedulers/abc-123/executions');
    expect(res.status).toBe(200);
    expect(res.body.total_count).toBe(412);
  });

  it('returns 400 for INVALID_ARGUMENT', async () => {
    const err = Object.assign(new Error('schedule_id is required'), {
      code: grpc.status.INVALID_ARGUMENT,
    });
    mockStateClient.listTaskExecutions.mockImplementationOnce((_req: any, cb: any) => cb(err, null));

    const res = await request(app).get('/api/schedulers/bad/executions');
    expect(res.status).toBe(400);
  });

  it('returns 404 for NOT_FOUND', async () => {
    const err = Object.assign(new Error('no such schedule'), {
      code: grpc.status.NOT_FOUND,
    });
    mockStateClient.listTaskExecutions.mockImplementationOnce((_req: any, cb: any) => cb(err, null));

    const res = await request(app).get('/api/schedulers/abc-123/executions');
    expect(res.status).toBe(404);
  });
});
