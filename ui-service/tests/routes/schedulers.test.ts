import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import { createSchedulersRouter } from '../../src/server/routes/schedulers';

const mockListTasks = vi.fn();
const mockGetScheduler = vi.fn();
const mockTriggerRerun = vi.fn();

const mockClient = {
  listTasks: mockListTasks,
  getScheduler: mockGetScheduler,
  triggerRerun: mockTriggerRerun,
};

const app = express();
app.use(express.json());
app.use('/api/schedulers', createSchedulersRouter(mockClient as any));

describe('GET /api/schedulers/:id/tasks', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('returns tasks with normalised status', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, {
        tasks: [
          {
            task_id: 'task-456',
            schedule_id: 'abc-123',
            service_name: 'dbt',
            schema_name: 'analytics',
            table_name: 'users',
            job_name: 'dbt-analytics-users',
            status: 'TASK_STATUS_SUCCEEDED',
            retry_count: 0,
            max_retries: 3,
            created_at: { seconds: '1740268805', nanos: 0 },
          },
        ],
        total_count: 1,
      });
    });

    const res = await request(app).get('/api/schedulers/abc-123/tasks');

    expect(res.status).toBe(200);
    expect(res.body.tasks).toHaveLength(1);
    const t = res.body.tasks[0];
    expect(t.task_id).toBe('task-456');
    expect(t.status).toBe('succeeded');
    expect(t.retry_count).toBe(0);
    expect(t.max_retries).toBe(3);
    expect(t.created_at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it('passes schedule_id to gRPC call', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/tasks');

    expect(mockListTasks).toHaveBeenCalledWith(
      expect.objectContaining({ schedule_id: 'abc-123' }),
      expect.any(Function)
    );
  });

  it('returns 500 when gRPC fails', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(new Error('not found'), null);
    });

    const res = await request(app).get('/api/schedulers/bad-id/tasks');
    expect(res.status).toBe(500);
  });
});

describe('POST /api/schedulers/:id/rerun', () => {
  const VALID_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';

  beforeEach(() => { vi.clearAllMocks(); });

  it('returns 200 on success', async () => {
    mockTriggerRerun.mockImplementation((_req: any, cb: any) => cb(null));
    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rerun`)
      .send({ schema: 'analytics', table_name: 'users', service_name: 'dbt' });
    expect(res.status).toBe(200);
  });

  it('returns 400 for INVALID_ARGUMENT', async () => {
    const err = Object.assign(new Error('invalid schedule_id format'), {
      code: grpc.status.INVALID_ARGUMENT,
    });
    mockTriggerRerun.mockImplementation((_req: any, cb: any) => cb(err));
    const res = await request(app)
      .post(`/api/schedulers/not-a-uuid/rerun`)
      .send({});
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/invalid schedule_id format/);
  });

  it('returns 404 for NOT_FOUND', async () => {
    const err = Object.assign(new Error('schedule not found'), {
      code: grpc.status.NOT_FOUND,
    });
    mockTriggerRerun.mockImplementation((_req: any, cb: any) => cb(err));
    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rerun`)
      .send({ schema: 'analytics', table_name: 'users', service_name: 'dbt' });
    expect(res.status).toBe(404);
    expect(res.body.error).toMatch(/schedule not found/);
  });

  it('returns 409 for FAILED_PRECONDITION (running tasks)', async () => {
    const err = Object.assign(new Error('schedule has running tasks'), {
      code: grpc.status.FAILED_PRECONDITION,
    });
    mockTriggerRerun.mockImplementation((_req: any, cb: any) => cb(err));
    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rerun`)
      .send({ schema: 'analytics', table_name: 'users', service_name: 'dbt' });
    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/running tasks/);
  });

  it('returns 409 for FAILED_PRECONDITION (task not FAILED)', async () => {
    const err = Object.assign(new Error('target task is not in FAILED state'), {
      code: grpc.status.FAILED_PRECONDITION,
    });
    mockTriggerRerun.mockImplementation((_req: any, cb: any) => cb(err));
    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rerun`)
      .send({ schema: 'analytics', table_name: 'users', service_name: 'dbt' });
    expect(res.status).toBe(409);
  });

  it('returns 500 for INTERNAL', async () => {
    const err = Object.assign(new Error('internal error'), {
      code: grpc.status.INTERNAL,
    });
    mockTriggerRerun.mockImplementation((_req: any, cb: any) => cb(err));
    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rerun`)
      .send({ schema: 'analytics', table_name: 'users', service_name: 'dbt' });
    expect(res.status).toBe(500);
  });

  it('returns 500 for unknown gRPC code', async () => {
    const err = Object.assign(new Error('something weird'), { code: 999 });
    mockTriggerRerun.mockImplementation((_req: any, cb: any) => cb(err));
    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rerun`)
      .send({});
    expect(res.status).toBe(500);
  });
});
