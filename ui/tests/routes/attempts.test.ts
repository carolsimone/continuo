import request from 'supertest';
import { describe, it, expect, vi, beforeAll } from 'vitest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import { createAttemptsRouter } from '../../src/server/routes/attempts';

const mockStateClient = {
  getScheduler: vi.fn(),
  listTasks: vi.fn(),
  listTaskAttempts: vi.fn(),
};

const app = express();
app.use('/api/schedulers', createAttemptsRouter(mockStateClient as any));

describe('GET /api/schedulers/:id/attempts', () => {
  beforeAll(() => {
    mockStateClient.listTaskAttempts.mockImplementation((_req: any, cb: any) => {
      cb(null, {
        attempts: [
          {
            task_id: 'task-1',
            attempt: 1,
            job_name: 'core-sales-orders-1',
            status: 'failed',
            failure_class: 'user_code',
            failure_reason: 'compile_error',
            exit_code: 1,
            dispatched_at: { seconds: '1767261600', nanos: 0 },
            queued_at: { seconds: '1767261601', nanos: 0 },
            started_at: { seconds: '1767261605', nanos: 500_000_000 },
            finished_at: { seconds: '1767261625', nanos: 0 },
            execution_seconds: 19.5,
            error_message: 'boom',
            log_s3_key: 'logs/task-attempts/svc/sc/tbl/task-1-1.log',
            run_results_uri: 'run-results/task-attempts/svc/sc/tbl/task-1-1.json',
          },
          {
            task_id: 'task-2',
            attempt: 0,
            job_name: 'core-sales-items-0',
            status: 'dispatched',
            failure_class: '',
            failure_reason: '',
            exit_code: 0,
            dispatched_at: { seconds: '1767261600', nanos: 0 },
            queued_at: null,
            started_at: null,
            finished_at: null,
            execution_seconds: 0,
            error_message: '',
            log_s3_key: '',
            run_results_uri: '',
          },
        ],
        total_count: 2,
      });
    });
  });

  it('returns attempts', async () => {
    const res = await request(app).get('/api/schedulers/abc-123/attempts');
    expect(res.status).toBe(200);
    expect(res.body.attempts).toEqual([
      {
        task_id: 'task-1',
        attempt: 1,
        job_name: 'core-sales-orders-1',
        status: 'failed',
        failure_class: 'user_code',
        failure_reason: 'compile_error',
        dispatched_at: '2026-01-01T10:00:00.000Z',
        queued_at: '2026-01-01T10:00:01.000Z',
        started_at: '2026-01-01T10:00:05.500Z',
        finished_at: '2026-01-01T10:00:25.000Z',
        execution_seconds: 19.5,
        error_message: 'boom',
        log_s3_key: 'logs/task-attempts/svc/sc/tbl/task-1-1.log',
        run_results_uri: 'run-results/task-attempts/svc/sc/tbl/task-1-1.json',
      },
      {
        // Empty strings and a zero run time are state's "not set": they cross
        // the route as null, so the client never renders a blank cell.
        task_id: 'task-2',
        attempt: 0,
        job_name: 'core-sales-items-0',
        status: 'dispatched',
        failure_class: null,
        failure_reason: null,
        dispatched_at: '2026-01-01T10:00:00.000Z',
        queued_at: null,
        started_at: null,
        finished_at: null,
        execution_seconds: null,
        error_message: null,
        log_s3_key: null,
        run_results_uri: null,
      },
    ]);
  });

  it('returns 500 on gRPC error', async () => {
    mockStateClient.listTaskAttempts.mockImplementationOnce((_req: any, cb: any) => {
      cb(new Error('grpc error'), null);
    });
    const res = await request(app).get('/api/schedulers/bad-id/attempts');
    expect(res.status).toBe(500);
  });

  it('defaults to page_size 200 and page_offset 0', async () => {
    mockStateClient.listTaskAttempts.mockImplementationOnce((_req: any, cb: any) => {
      cb(null, { attempts: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/attempts');

    expect(mockStateClient.listTaskAttempts).toHaveBeenLastCalledWith(
      { run_id: 'abc-123', task_id: '', page_size: 200, page_offset: 0 },
      expect.any(Function)
    );
  });

  it('forwards limit, offset and task_id', async () => {
    mockStateClient.listTaskAttempts.mockImplementationOnce((_req: any, cb: any) => {
      cb(null, { attempts: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/attempts?limit=50&offset=200&task_id=task-7');

    expect(mockStateClient.listTaskAttempts).toHaveBeenLastCalledWith(
      { run_id: 'abc-123', task_id: 'task-7', page_size: 50, page_offset: 200 },
      expect.any(Function)
    );
  });

  it('clamps limit to the state-side max of 200', async () => {
    mockStateClient.listTaskAttempts.mockImplementationOnce((_req: any, cb: any) => {
      cb(null, { attempts: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/attempts?limit=500');

    expect(mockStateClient.listTaskAttempts).toHaveBeenLastCalledWith(
      expect.objectContaining({ page_size: 200 }),
      expect.any(Function)
    );
  });

  it('returns total_count in the body', async () => {
    mockStateClient.listTaskAttempts.mockImplementationOnce((_req: any, cb: any) => {
      cb(null, { attempts: [], total_count: 412 });
    });

    const res = await request(app).get('/api/schedulers/abc-123/attempts');
    expect(res.status).toBe(200);
    expect(res.body.total_count).toBe(412);
  });

  it('returns 400 for INVALID_ARGUMENT', async () => {
    const err = Object.assign(new Error('run_id is required'), {
      code: grpc.status.INVALID_ARGUMENT,
    });
    mockStateClient.listTaskAttempts.mockImplementationOnce((_req: any, cb: any) => cb(err, null));

    const res = await request(app).get('/api/schedulers/bad/attempts');
    expect(res.status).toBe(400);
  });

  it('returns 404 for NOT_FOUND', async () => {
    const err = Object.assign(new Error('no such run'), {
      code: grpc.status.NOT_FOUND,
    });
    mockStateClient.listTaskAttempts.mockImplementationOnce((_req: any, cb: any) => cb(err, null));

    const res = await request(app).get('/api/schedulers/abc-123/attempts');
    expect(res.status).toBe(404);
  });
});
