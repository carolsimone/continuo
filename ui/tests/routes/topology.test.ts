import request from 'supertest';
import { describe, it, expect, vi } from 'vitest';
import express from 'express';
import { createTopologyRouter } from '../../src/server/routes/topology';

const mockListScheduleTopologies = vi.fn();

const app = express();
app.use('/api/topology', createTopologyRouter({ listScheduleTopologies: mockListScheduleTopologies } as any));

describe('routes/topology: GET /schedules reads RunService', () => {
  it('returns one tile per schedule of the live release', async () => {
    mockListScheduleTopologies.mockImplementation((req: any, cb: any) => {
      expect(req).toEqual({});
      cb(null, {
        schedules: [
          { schedule_name: 'daily', node_count: 12, last_updated_at: { seconds: '1792836300', nanos: 0 } },
          { schedule_name: 'hourly', node_count: 0, last_updated_at: null },
        ],
        dormant_node_count: '3', // gRPC-JS returns int64 as a string
      });
    });

    const res = await request(app).get('/api/topology/schedules');

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      schedules: [
        { schedule_name: 'daily', node_count: 12, last_updated_at: '2026-10-24T10:05:00.000Z' },
        { schedule_name: 'hourly', node_count: 0, last_updated_at: null },
      ],
      dormant_node_count: 3,
    });
  });

  it('counts no dormant node when the answer carries no count', async () => {
    mockListScheduleTopologies.mockImplementation((_req: any, cb: any) => cb(null, { schedules: [] }));
    const res = await request(app).get('/api/topology/schedules');
    expect(res.body).toEqual({ schedules: [], dormant_node_count: 0 });
  });

  it('answers 500 when run-controller fails', async () => {
    mockListScheduleTopologies.mockImplementation((_req: any, cb: any) => cb(new Error('run-controller down')));
    const res = await request(app).get('/api/topology/schedules');
    expect(res.status).toBe(500);
    expect(res.body.error).toMatch(/run-controller down/);
  });
});
