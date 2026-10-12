// File: ui/tests/routes/runs.test.ts
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

  it('passes through run_promotion_seq and latest_promotion_seq as numbers', async () => {
    mockGetRunGraph.mockImplementation((_req: any, cb: any) =>
      cb(null, {
        nodes: [{ service_name: 's', schema_name: 'sch', table_name: 't', node_type: 'model' }],
        edges: [],
        run_promotion_seq: '5',     // gRPC-JS returns int64 as string
        latest_promotion_seq: '7',
      })
    );

    const res = await request(app).get('/api/runs/run-1/graph');

    expect(res.status).toBe(200);
    expect(res.body.run_promotion_seq).toBe(5);
    expect(res.body.latest_promotion_seq).toBe(7);
    expect(typeof res.body.run_promotion_seq).toBe('number');
    expect(res.body).not.toHaveProperty('run_topology_generation');
    expect(res.body).not.toHaveProperty('latest_topology_generation');
  });

  it('defaults missing promotion seq fields to 0', async () => {
    mockGetRunGraph.mockImplementation((_req: any, cb: any) =>
      cb(null, { nodes: [], edges: [] }) // no promotion seq fields at all
    );

    const res = await request(app).get('/api/runs/run-1/graph');

    expect(res.status).toBe(200);
    expect(res.body.run_promotion_seq).toBe(0);
    expect(res.body.latest_promotion_seq).toBe(0);
  });

  it('emits edges upstream -> consumer, reversing the DEPENDS_ON edge from the orchestrator', async () => {
    mockGetRunGraph.mockImplementation((_req: any, cb: any) =>
      cb(null, {
        nodes: [],
        // orders DEPENDS_ON seed_orders
        edges: [{ from_node_id: 'svc.sales.orders', to_node_id: 'svc.raw.seed_orders' }],
      })
    );

    const res = await request(app).get('/api/runs/run-1/graph');

    expect(res.status).toBe(200);
    expect(res.body.edges).toEqual([{ from_node_id: 'svc.raw.seed_orders', to_node_id: 'svc.sales.orders' }]);
  });
});
