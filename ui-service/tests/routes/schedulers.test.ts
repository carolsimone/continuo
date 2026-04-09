import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';
import express from 'express';
import { createSchedulersRouter } from '../../src/server/routes/schedulers';

const mockListTasks = vi.fn();

const mockClient = {
  listTasks: mockListTasks,
};

const app = express();
app.use('/api/schedulers', createSchedulersRouter(mockClient as any));

describe('GET /api/schedulers/:id/tasks', () => {
  beforeEach(() => vi.clearAllMocks());

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
