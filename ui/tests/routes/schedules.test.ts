import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import { createSchedulesRouter } from '../../src/server/routes/schedules';
import { USER_ID_METADATA_KEY } from '../../src/server/clients/grpc-client';

const mockStartRun = vi.fn();
const mockListSchedules = vi.fn();
const mockListRuns = vi.fn();
const mockGetScheduleGraph = vi.fn();

const mockStateClient = {
  startRun: mockStartRun,
  listSchedules: mockListSchedules,
  listRuns: mockListRuns,
  getScheduleGraph: mockGetScheduleGraph,
};

const REQUEST_ID = '5d21c0de-0000-4000-8000-000000000001';

const app = express();
app.use(express.json());
app.use('/api/schedules', createSchedulesRouter(mockStateClient as any));

describe('GET /api/schedules', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('returns the state schedule catalog without drift fields', async () => {
    mockListSchedules.mockImplementation((_req: any, cb: any) =>
      cb(null, {
        schedules: [
          { schedule_name: 'daily', cron_expression: '0 0 * * *', description: '', timezone: 'UTC',
            has_active_run: true, last_run_at: null, last_run_status: 'running', last_run_id: 'run-1' },
          { schedule_name: 'hourly', cron_expression: '0 * * * *', description: '', timezone: 'UTC',
            has_active_run: false, last_run_at: null, last_run_status: 'succeeded', last_run_id: 'run-0' },
        ],
      })
    );

    const res = await request(app).get('/api/schedules');

    expect(res.status).toBe(200);
    expect(res.body.schedules).toHaveLength(2);

    const daily = res.body.schedules.find((s: any) => s.schedule_name === 'daily');
    expect(daily).toMatchObject({
      schedule_name: 'daily',
      cron_expression: '0 0 * * *',
      has_active_run: true,
      last_run_id: 'run-1',
      last_run_status: 'running',
    });
    expect(daily).not.toHaveProperty('is_running');
    expect(daily).not.toHaveProperty('active_run_promotion_seq');
    expect(daily).not.toHaveProperty('active_run_id');

    expect(res.body).not.toHaveProperty('latest_promotion_seq');
  });

  it('returns 500 if state call fails', async () => {
    mockListSchedules.mockImplementation((_req: any, cb: any) =>
      cb(new Error('state down'))
    );

    const res = await request(app).get('/api/schedules');

    expect(res.status).toBe(500);
    expect(res.body.error).toMatch(/state down/);
  });

  it('maps the gRPC status of a refused read', async () => {
    mockListSchedules.mockImplementation((_req: any, cb: any) =>
      cb(Object.assign(new Error('bad request'), { code: grpc.status.INVALID_ARGUMENT }))
    );

    const res = await request(app).get('/api/schedules');

    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/bad request/);
  });
});

describe('GET /api/schedules/:name/runs', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('lists completed runs from the state service', async () => {
    mockListRuns.mockImplementation((req: any, cb: any) => {
      expect(req).toEqual({ schedule_name: 'daily', page_size: 10, page_offset: 0 });
      cb(null, { runs: [
        { run_id: 'r1', schedule_name: 'daily', status: 'succeeded',
          created_at: '2026-10-14T23:00:00Z', completed_at: '2026-10-14T23:03:00Z', failure_reason: '' },
        { run_id: 'r2', schedule_name: 'daily', status: 'failed',
          created_at: '2026-10-13T23:00:00Z', completed_at: '2026-10-13T23:09:00Z', failure_reason: 'stalled' },
      ], total_count: 2 });
    });

    const res = await request(app).get('/api/schedules/daily/runs');

    expect(res.status).toBe(200);
    expect(res.body.runs).toEqual([
      { run_id: 'r1', schedule_name: 'daily', status: 'succeeded',
        created_at: '2026-10-14T23:00:00Z', completed_at: '2026-10-14T23:03:00Z', failure_reason: null },
      { run_id: 'r2', schedule_name: 'daily', status: 'failed',
        created_at: '2026-10-13T23:00:00Z', completed_at: '2026-10-13T23:09:00Z', failure_reason: 'stalled' },
    ]);
    expect(res.body.total_count).toBe(2);
  });

  it('answers 400 when state refuses the request as INVALID_ARGUMENT', async () => {
    mockListRuns.mockImplementation((_req: any, cb: any) =>
      cb(Object.assign(new Error('schedule_name is required'), { code: grpc.status.INVALID_ARGUMENT }))
    );

    const res = await request(app).get('/api/schedules/daily/runs');

    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/schedule_name is required/);
  });

  it('forwards limit and offset as page_size and page_offset and returns the total', async () => {
    mockListRuns.mockImplementation((_req: any, cb: any) => cb(null, { runs: [], total_count: 50 }));

    const res = await request(app).get('/api/schedules/daily/runs?limit=25&offset=50');

    expect(mockListRuns).toHaveBeenCalledWith(
      { schedule_name: 'daily', page_size: 25, page_offset: 50 },
      expect.any(Function),
    );
    expect(res.body.total_count).toBe(50);
  });

  it('clamps limit to 200 and falls back to the defaults on malformed values', async () => {
    mockListRuns.mockImplementation((_req: any, cb: any) => cb(null, { runs: [] }));

    await request(app).get('/api/schedules/daily/runs?limit=9999&offset=-3');
    await request(app).get('/api/schedules/daily/runs?limit=abc&offset=xyz');

    expect(mockListRuns).toHaveBeenNthCalledWith(
      1, { schedule_name: 'daily', page_size: 200, page_offset: 0 }, expect.any(Function));
    expect(mockListRuns).toHaveBeenNthCalledWith(
      2, { schedule_name: 'daily', page_size: 10, page_offset: 0 }, expect.any(Function));
  });

  it('reports a zero total when state sends none', async () => {
    mockListRuns.mockImplementation((_req: any, cb: any) => cb(null, { runs: [] }));

    const res = await request(app).get('/api/schedules/daily/runs');

    expect(res.body).toEqual({ runs: [], total_count: 0 });
  });
});

describe('routes/schedules: POST /:name/trigger', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('returns 200 with run_id on success and forwards request_id', async () => {
    mockStartRun.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(null, { run_id: 'aaa-bbb-ccc' })
    );

    const res = await request(app).post('/api/schedules/hourly/trigger').send({ request_id: REQUEST_ID });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ run_id: 'aaa-bbb-ccc' });
    expect(mockStartRun).toHaveBeenCalledWith(
      { request_id: REQUEST_ID, schedule_name: 'hourly', operation: '' },
      expect.any(Object),
      expect.any(Function)
    );
  });

  it('forwards the authenticated user id as gRPC metadata', async () => {
    let seenUserId: string | undefined;
    mockStartRun.mockImplementation((_req: any, md: any, cb: any) => {
      seenUserId = md.get(USER_ID_METADATA_KEY)[0];
      cb(null, { run_id: 'r1' });
    });

    // Mount a tiny app that injects an authenticated user before the router,
    // mirroring how the auth middleware attaches req.user in production.
    const authedApp = express();
    authedApp.use(express.json());
    authedApp.use((req, _res, next) => {
      req.user = { userId: 'okta|alice', email: 'a@x', name: 'A', role: 'operator' };
      next();
    });
    authedApp.use('/api/schedules', createSchedulesRouter(mockStateClient as any));

    const res = await request(authedApp).post('/api/schedules/hourly/trigger').send({ request_id: REQUEST_ID });
    expect(res.status).toBe(200);
    expect(seenUserId).toBe('okta|alice');
  });

  it('records the system sentinel when no user is authenticated', async () => {
    let seenUserId: string | undefined;
    mockStartRun.mockImplementation((_req: any, md: any, cb: any) => {
      seenUserId = md.get(USER_ID_METADATA_KEY)[0];
      cb(null, { run_id: 'r1' });
    });

    const res = await request(app).post('/api/schedules/hourly/trigger').send({ request_id: REQUEST_ID });
    expect(res.status).toBe(200);
    expect(seenUserId).toBe('system');
  });

  it('returns 404 when schedule not in catalog', async () => {
    const err = Object.assign(new Error('schedule "nope" not found in catalog'), {
      code: grpc.status.NOT_FOUND,
    });
    mockStartRun.mockImplementation((_req: any, _md: any, cb: any) => cb(err));

    const res = await request(app).post('/api/schedules/nope/trigger').send({ request_id: REQUEST_ID });

    expect(res.status).toBe(404);
    expect(res.body.error).toMatch(/not found/);
  });

  it('returns 409 when schedule already has an active run', async () => {
    const err = Object.assign(new Error('schedule "hourly" already has an active run'), {
      code: grpc.status.FAILED_PRECONDITION,
    });
    mockStartRun.mockImplementation((_req: any, _md: any, cb: any) => cb(err));

    const res = await request(app).post('/api/schedules/hourly/trigger').send({ request_id: REQUEST_ID });

    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/active run/);
  });

  it('returns 500 on unexpected gRPC error', async () => {
    const err = Object.assign(new Error('internal'), { code: grpc.status.INTERNAL });
    mockStartRun.mockImplementation((_req: any, _md: any, cb: any) => cb(err));

    const res = await request(app).post('/api/schedules/hourly/trigger').send({ request_id: REQUEST_ID });

    expect(res.status).toBe(500);
  });

  it('forwards operation=test', async () => {
    mockStartRun.mockImplementation((_req: any, _md: any, cb: any) => cb(null, { run_id: 's1' }));
    const res = await request(app).post('/api/schedules/daily/trigger').send({ request_id: REQUEST_ID, operation: 'test' });
    expect(res.status).toBe(200);
    expect(mockStartRun).toHaveBeenCalledWith(
      { request_id: REQUEST_ID, schedule_name: 'daily', operation: 'test' },
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('rejects a bad operation with 400 and no gRPC call', async () => {
    const res = await request(app).post('/api/schedules/daily/trigger').send({ request_id: REQUEST_ID, operation: 'drop' });
    expect(res.status).toBe(400);
    expect(mockStartRun).not.toHaveBeenCalled();
  });

  it('rejects full_refresh with 400 and no gRPC call', async () => {
    const res = await request(app).post('/api/schedules/daily/trigger').send({ request_id: REQUEST_ID, operation: 'full_refresh' });
    expect(res.status).toBe(400);
    expect(mockStartRun).not.toHaveBeenCalled();
  });

  it.each([
    ['no body', undefined],
    ['an empty body', {}],
    ['an empty request_id', { request_id: '' }],
    ['a request_id that is not a UUID', { request_id: 'nope', operation: 'test' }],
  ])('refuses a missing or malformed request_id with 400: %s', async (_label, body) => {
    const req = request(app).post('/api/schedules/daily/trigger');
    const res = await (body === undefined ? req : req.send(body));
    expect(res.status).toBe(400);
    expect(res.body).toEqual({ error: 'request_id must be a UUID' });
    expect(mockStartRun).not.toHaveBeenCalled();
  });

  it('returns 409 when request_id is reused with different parameters', async () => {
    const err = Object.assign(new Error('request_id reused with different parameters'), {
      code: grpc.status.ALREADY_EXISTS,
    });
    mockStartRun.mockImplementation((_req: any, _md: any, cb: any) => cb(err));

    const res = await request(app).post('/api/schedules/daily/trigger').send({ request_id: REQUEST_ID });

    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/request_id reused/);
  });
});

// The schedule graph is read from RunService, which serves the live release
// from Postgres.
describe('routes/schedules: GET /:name/graph reads RunService', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('reads RunService.GetScheduleGraph and returns nodes and edges', async () => {
    mockGetScheduleGraph.mockImplementation((req: any, cb: any) => {
      expect(req).toEqual({ schedule_name: 'daily' });
      cb(null, {
        nodes: [
          { service_name: 'svc', schema_name: 'sales', table_name: 'orders', node_type: 'dbt-model', schedule_name: 'daily' },
        ],
        edges: [],
        promotion_seq: '42', // gRPC-JS returns int64 as string
      });
    });
    const res = await request(app).get('/api/schedules/daily/graph');
    expect(res.status).toBe(200);
    expect(res.body.nodes).toEqual([
      { node_id: 'svc.sales.orders', node_type: 'dbt-model', schedule_name: 'daily' },
    ]);
    expect(res.body.edges).toEqual([]);
    expect(res.body.promotion_seq).toBe(42);
    expect(typeof res.body.promotion_seq).toBe('number');
    expect(res.body).not.toHaveProperty('topology_generation');
  });

  it('defaults a missing promotion seq to 0', async () => {
    mockGetScheduleGraph.mockImplementation((_req: any, cb: any) => cb(null, { nodes: [], edges: [] }));
    const res = await request(app).get('/api/schedules/daily/graph');
    expect(res.body.promotion_seq).toBe(0);
  });

  // State returns each edge from the dependent node to the node it reads. The
  // ui draws and orders graphs in execution order, where `from` runs first, so
  // the route flips every edge exactly once here, at the boundary.
  it('emits edges upstream -> consumer, reversing the dependent -> dependency edge state sends', async () => {
    mockGetScheduleGraph.mockImplementation((_req: any, cb: any) => {
      cb(null, {
        nodes: [
          { service_name: 'svc', schema_name: 'raw', table_name: 'seed_orders', node_type: 'dbt-seed', schedule_name: 'daily' },
          { service_name: 'svc', schema_name: 'sales', table_name: 'orders', node_type: 'dbt-model', schedule_name: 'daily' },
        ],
        // orders depends on seed_orders
        edges: [{ from_node_id: 'svc.sales.orders', to_node_id: 'svc.raw.seed_orders' }],
      });
    });
    const res = await request(app).get('/api/schedules/daily/graph');
    expect(res.status).toBe(200);
    expect(res.body.edges).toEqual([{ from_node_id: 'svc.raw.seed_orders', to_node_id: 'svc.sales.orders' }]);
  });

  it('maps a gRPC status to its HTTP status', async () => {
    mockGetScheduleGraph
      .mockImplementationOnce((_req: any, cb: any) => cb(Object.assign(new Error('schedule_name is required'), { code: grpc.status.INVALID_ARGUMENT })))
      .mockImplementationOnce((_req: any, cb: any) => cb(new Error('grpc error')));
    expect((await request(app).get('/api/schedules/%20/graph')).status).toBe(400);
    expect((await request(app).get('/api/schedules/bad-name/graph')).status).toBe(500);
  });
});
