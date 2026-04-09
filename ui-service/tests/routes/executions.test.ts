import request from 'supertest';
import { describe, it, expect, vi, beforeAll } from 'vitest';
import express from 'express';
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
});
