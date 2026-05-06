// File: ui-service/tests/routes/runs.test.ts
import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';
import express from 'express';
import { createRunsRouter } from '../../src/server/routes/schedules';

const mockGetRunGraph = vi.fn();
const mockGraphClient = {
  getRunGraph: mockGetRunGraph,
} as any;

const app = express();
app.use(express.json());
app.use('/api/runs', createRunsRouter(mockGraphClient));

describe('GET /api/runs/:run_id/graph', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('passes through run_topology_generation and latest_topology_generation as numbers', async () => {
    mockGetRunGraph.mockImplementation((_req: any, cb: any) =>
      cb(null, {
        nodes: [{ service_name: 's', schema_name: 'sch', table_name: 't', node_type: 'model' }],
        edges: [],
        run_topology_generation: '5',     // gRPC-JS returns int64 as string
        latest_topology_generation: '7',
      })
    );

    const res = await request(app).get('/api/runs/run-1/graph');

    expect(res.status).toBe(200);
    expect(res.body.run_topology_generation).toBe(5);
    expect(res.body.latest_topology_generation).toBe(7);
    expect(typeof res.body.run_topology_generation).toBe('number');
  });

  it('defaults missing generation fields to 0', async () => {
    mockGetRunGraph.mockImplementation((_req: any, cb: any) =>
      cb(null, { nodes: [], edges: [] }) // no generation fields at all
    );

    const res = await request(app).get('/api/runs/run-1/graph');

    expect(res.status).toBe(200);
    expect(res.body.run_topology_generation).toBe(0);
    expect(res.body.latest_topology_generation).toBe(0);
  });
});
