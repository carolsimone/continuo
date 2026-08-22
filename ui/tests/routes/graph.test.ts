import request from 'supertest';
import { describe, it, expect, vi, beforeAll } from 'vitest';
import express from 'express';
import { createSchedulesRouter } from '../../src/server/routes/schedules';

const mockStateClient = {
  listSchedulers: vi.fn(),
  listAllSchedules: vi.fn(),
  listTasks: vi.fn(),
  getScheduler: vi.fn(),
  listTaskExecutions: vi.fn(),
};

const mockGraphClient = {
  getScheduleGraph: vi.fn(),
};

const app = express();
app.use('/api/schedules', createSchedulesRouter(mockStateClient as any, mockGraphClient as any));

describe('GET /api/schedules/:name/graph', () => {
  beforeAll(() => {
    mockGraphClient.getScheduleGraph.mockImplementation((_req: any, cb: any) => {
      cb(null, {
        nodes: [
          { service_name: 'svc', schema_name: 'sales', table_name: 'orders', node_type: 'dbt-model', schedule_name: 'daily' },
        ],
        edges: [],
      });
    });
  });

  it('returns nodes and edges', async () => {
    const res = await request(app).get('/api/schedules/daily/graph');
    expect(res.status).toBe(200);
    expect(res.body.nodes).toHaveLength(1);
    expect(res.body.nodes[0].node_id).toBe('svc.sales.orders');
    expect(res.body.edges).toEqual([]);
  });

  it('returns 500 when getScheduleGraph fails', async () => {
    mockGraphClient.getScheduleGraph.mockImplementationOnce((_req: any, cb: any) => {
      cb(new Error('grpc error'), null);
    });
    const res = await request(app).get('/api/schedules/bad-name/graph');
    expect(res.status).toBe(500);
  });
});
