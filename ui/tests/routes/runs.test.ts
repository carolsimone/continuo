import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import { createRunsRouter, RELEASE_MOVED_MESSAGE } from '../../src/server/routes/runs';
import { MAINTENANCE_CODE, MAINTENANCE_TRAILER_KEY } from '../../src/server/maintenance';
import { USER_ID_METADATA_KEY } from '../../src/server/clients/grpc-client';
import { RUN_STATUSES, TASK_STATUSES } from '../../src/server/generated/vocabulary.gen';

const mockGetRun = vi.fn();
const mockListTasks = vi.fn();
const mockListRunAttempts = vi.fn();
const mockGetRunGraph = vi.fn();
const mockTriggerRerun = vi.fn();
const mockTriggerRebase = vi.fn();
const mockPreviewRebase = vi.fn();
const mockCancelRun = vi.fn();

const mockClient = {
  getRun: mockGetRun,
  listTasks: mockListTasks,
  listRunAttempts: mockListRunAttempts,
  getRunGraph: mockGetRunGraph,
  triggerRerun: mockTriggerRerun,
  triggerRebase: mockTriggerRebase,
  previewRebase: mockPreviewRebase,
  cancelRun: mockCancelRun,
};

const app = express();
app.use(express.json());
app.use('/api/runs', createRunsRouter(mockClient as any));

const VALID_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
const REQUEST_ID = '5d21c0de-0000-4000-8000-000000000001';

// grpcError builds a failed call the way @grpc/grpc-js reports one: the
// message carries the code's prefix, details the server's own text.
function grpcError(code: number, details: string, trailer?: Record<string, string>) {
  const metadata = new grpc.Metadata();
  for (const [k, v] of Object.entries(trailer ?? {})) metadata.set(k, v);
  return Object.assign(new Error(`${code} ${grpc.status[code]}: ${details}`), { code, details, metadata });
}

// authedApp mounts the router behind a middleware that attaches an
// authenticated user, as the auth middleware does in production.
function authedApp() {
  const a = express();
  a.use(express.json());
  a.use((req, _res, next) => {
    req.user = { userId: 'okta|alice', email: 'a@x', name: 'A', role: 'operator' };
    next();
  });
  a.use('/api/runs', createRunsRouter(mockClient as any));
  return a;
}

describe('routes/runs: GET /api/runs/:id', () => {
  beforeEach(() => vi.clearAllMocks());

  it('answers the run under run_id, with its statuses as run-controller sends them', async () => {
    mockGetRun.mockImplementation((req: any, cb: any) => {
      expect(req).toEqual({ run_id: 'run-3' });
      cb(null, { run: {
        run_id: 'run-3', schedule_name: 'daily', kind: 'cron', operation: 'run', status: 'cancelling',
        failure_reason: '', release_id: 'rel-7', promotion_seq: '7', initiated_by: '',
        created_at: { seconds: '1792836300', nanos: 0 }, started_at: { seconds: '1792836301', nanos: 0 },
        completed_at: null, last_progress_at: { seconds: '1792836360', nanos: 0 },
        cancelled_at: { seconds: '1792836400', nanos: 0 }, cancelled_by: 'okta|alice', cancellation_reason: 'manual',
      } });
    });
    const res = await request(app).get('/api/runs/run-3');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      run_id: 'run-3', schedule_name: 'daily', kind: 'cron', operation: 'run', status: 'cancelling',
      failure_reason: null, release_id: 'rel-7', promotion_seq: 7, initiated_by: null,
      created_at: '2026-10-24T10:05:00.000Z', started_at: '2026-10-24T10:05:01.000Z',
      completed_at: null, last_progress_at: '2026-10-24T10:06:00.000Z',
      cancelled_at: '2026-10-24T10:06:40.000Z', cancelled_by: 'okta|alice', cancellation_reason: 'manual',
    });
    expect(res.body).not.toHaveProperty('schedule_id');
  });

  it("returns the run's failure reason, null when it has none", async () => {
    mockGetRun.mockImplementation((_req: any, cb: any) => cb(null, { run: {
      run_id: 'run-1', schedule_name: 'daily', status: 'failed', failure_reason: 'stalled',
    } }));
    const res = await request(app).get('/api/runs/run-1');
    expect(res.body.status).toBe('failed');
    expect(res.body.failure_reason).toBe('stalled');

    mockGetRun.mockImplementation((_req: any, cb: any) => cb(null, { run: {
      run_id: 'run-2', schedule_name: 'daily', status: 'succeeded', failure_reason: '',
    } }));
    const ok = await request(app).get('/api/runs/run-2');
    expect(ok.body.failure_reason).toBeNull();
  });

  it('answers 404 when the response holds no run', async () => {
    mockGetRun.mockImplementation((_req: any, cb: any) => cb(null, {}));
    const res = await request(app).get('/api/runs/run-9');
    expect(res.status).toBe(404);
  });

  it('maps a gRPC status to its HTTP status', async () => {
    mockGetRun
      .mockImplementationOnce((_req: any, cb: any) => cb(grpcError(grpc.status.NOT_FOUND, 'no such run')))
      .mockImplementationOnce((_req: any, cb: any) => cb(grpcError(grpc.status.INVALID_ARGUMENT, 'run_id is not a UUID')))
      .mockImplementationOnce((_req: any, cb: any) => cb(new Error('run-controller down')));
    expect((await request(app).get('/api/runs/a')).status).toBe(404);
    expect((await request(app).get('/api/runs/b')).status).toBe(400);
    expect((await request(app).get('/api/runs/c')).status).toBe(500);
  });
});

// Statuses cross the server as the lowercase strings of the contract's
// run_status and task_status vocabularies, with no translation.
describe('routes/runs: statuses cross unchanged', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it.each(RUN_STATUSES)('GET /:id reports the run_status %s as sent', async (status) => {
    mockGetRun.mockImplementation((_req: any, cb: any) => cb(null, { run: { run_id: 'r', status } }));
    expect((await request(app).get('/api/runs/r')).body.status).toBe(status);
  });

  it.each(TASK_STATUSES)('GET /:id/tasks and /:id/graph report the task_status %s as sent', async (status) => {
    mockListTasks.mockImplementation((_req: any, cb: any) => cb(null, { tasks: [{ task_id: 't', status }], total_count: 1 }));
    mockGetRunGraph.mockImplementation((_req: any, cb: any) => cb(null, { nodes: [{ service_name: 's', schema_name: 'a', table_name: 'b', status }], edges: [] }));
    expect((await request(app).get('/api/runs/r/tasks')).body.tasks[0].status).toBe(status);
    expect((await request(app).get('/api/runs/r/graph')).body.nodes[0].status).toBe(status);
  });
});

describe('routes/runs: POST /:id/rerun', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('POST /:id/rerun forwards request_id', async () => {
    mockTriggerRerun.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(null, { run_id: 'run-2', schedule_name: 'daily' }));
    const res = await request(app).post(`/api/runs/${VALID_ID}/rerun`).send({ request_id: REQUEST_ID });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ run_id: 'run-2', schedule_name: 'daily' });
    expect(mockTriggerRerun).toHaveBeenCalledWith(
      { request_id: REQUEST_ID, source_run_id: VALID_ID },
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('forwards the authenticated user id as gRPC metadata', async () => {
    let seenUserId: string | undefined;
    mockTriggerRerun.mockImplementation((_req: any, md: any, cb: any) => {
      seenUserId = md.get(USER_ID_METADATA_KEY)[0];
      cb(null, { run_id: 'run-2', schedule_name: 'daily' });
    });
    await request(authedApp()).post(`/api/runs/${VALID_ID}/rerun`).send({ request_id: REQUEST_ID });
    expect(seenUserId).toBe('okta|alice');
  });

  it.each([
    ['no body', undefined],
    ['an empty body', {}],
    ['a null request_id', { request_id: null }],
    ['an empty request_id', { request_id: '' }],
    ['a request_id that is not a UUID', { request_id: 'not-a-uuid' }],
    ['a numeric request_id', { request_id: 7 }],
  ])('POST /:id/rerun refuses a missing or malformed request_id with 400: %s', async (_label, body) => {
    const req = request(app).post(`/api/runs/${VALID_ID}/rerun`);
    const res = await (body === undefined ? req : req.send(body));
    expect(res.status).toBe(400);
    expect(res.body).toEqual({ error: 'request_id must be a UUID' });
    expect(mockTriggerRerun).not.toHaveBeenCalled();
  });

  it('returns 400 for INVALID_ARGUMENT', async () => {
    mockTriggerRerun.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.INVALID_ARGUMENT, 'invalid source_run_id')));
    const res = await request(app).post('/api/runs/not-a-uuid/rerun').send({ request_id: REQUEST_ID });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/invalid source_run_id/);
  });

  it('returns 404 for NOT_FOUND', async () => {
    mockTriggerRerun.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.NOT_FOUND, 'run not found')));
    const res = await request(app).post(`/api/runs/${VALID_ID}/rerun`).send({ request_id: REQUEST_ID });
    expect(res.status).toBe(404);
    expect(res.body.error).toMatch(/run not found/);
  });

  it('returns 409 for FAILED_PRECONDITION (the schedule has an active run)', async () => {
    mockTriggerRerun.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.FAILED_PRECONDITION, 'schedule has an active run')));
    const res = await request(app).post(`/api/runs/${VALID_ID}/rerun`).send({ request_id: REQUEST_ID });
    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/active run/);
  });

  it('returns 500 for INTERNAL and for an unknown gRPC code', async () => {
    mockTriggerRerun
      .mockImplementationOnce((_req: any, _md: any, cb: any) => cb(grpcError(grpc.status.INTERNAL, 'internal error')))
      .mockImplementationOnce((_req: any, _md: any, cb: any) => cb(Object.assign(new Error('weird'), { code: 999 })));
    expect((await request(app).post(`/api/runs/${VALID_ID}/rerun`).send({ request_id: REQUEST_ID })).status).toBe(500);
    expect((await request(app).post(`/api/runs/${VALID_ID}/rerun`).send({ request_id: REQUEST_ID })).status).toBe(500);
  });
});

describe('routes/runs: POST /:id/cancel', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('POST /:id/cancel cancels by run id and answers the run as the cancel left it', async () => {
    mockCancelRun.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(null, { run: { run_id: VALID_ID, schedule_name: 'daily', status: 'cancelling' } }));

    const res = await request(app).post(`/api/runs/${VALID_ID}/cancel`).send({ cancellation_reason: 'manual' });

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ run_id: VALID_ID, schedule_name: 'daily', status: 'cancelling' });
    expect(mockCancelRun).toHaveBeenCalledWith(
      { run_id: VALID_ID, cancellation_reason: 'manual' },
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('takes no request_id, because CancelRun has none', async () => {
    mockCancelRun.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(null, { run: { run_id: VALID_ID, status: 'cancelled' } }));
    const res = await request(app).post(`/api/runs/${VALID_ID}/cancel`)
      .send({ cancellation_reason: 'manual', request_id: REQUEST_ID });
    expect(res.status).toBe(200);
    expect(mockCancelRun.mock.calls[0][0]).toEqual({ run_id: VALID_ID, cancellation_reason: 'manual' });
  });

  it('forwards the authenticated user id as gRPC metadata', async () => {
    let seenUserId: string | undefined;
    mockCancelRun.mockImplementation((_req: any, md: any, cb: any) => {
      seenUserId = md.get(USER_ID_METADATA_KEY)[0];
      cb(null, { run: { run_id: VALID_ID, status: 'cancelling' } });
    });
    await request(authedApp()).post(`/api/runs/${VALID_ID}/cancel`).send({ cancellation_reason: 'manual' });
    expect(seenUserId).toBe('okta|alice');
  });

  it('returns 400 when cancellation_reason is missing, calling nothing', async () => {
    const res = await request(app).post(`/api/runs/${VALID_ID}/cancel`).send({});
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/cancellation_reason is required/);
    expect(mockCancelRun).not.toHaveBeenCalled();
  });

  it('maps NOT_FOUND to 404, FAILED_PRECONDITION (the run ended) to 409 and INTERNAL to 500', async () => {
    mockCancelRun
      .mockImplementationOnce((_req: any, _md: any, cb: any) => cb(grpcError(grpc.status.NOT_FOUND, 'run not found')))
      .mockImplementationOnce((_req: any, _md: any, cb: any) => cb(grpcError(grpc.status.FAILED_PRECONDITION, 'run already succeeded')))
      .mockImplementationOnce((_req: any, _md: any, cb: any) => cb(grpcError(grpc.status.INTERNAL, 'internal')));
    const body = { cancellation_reason: 'manual' };
    expect((await request(app).post(`/api/runs/${VALID_ID}/cancel`).send(body)).status).toBe(404);
    const ended = await request(app).post(`/api/runs/${VALID_ID}/cancel`).send(body);
    expect(ended.status).toBe(409);
    expect(ended.body.error).toMatch(/already succeeded/);
    expect((await request(app).post(`/api/runs/${VALID_ID}/cancel`).send(body)).status).toBe(500);
  });
});

describe('routes/runs: GET /:id/attempts', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('returns attempts', async () => {
    mockListRunAttempts.mockImplementation((_req: any, cb: any) => {
      cb(null, {
        attempts: [
          {
            task_id: 'task-1', attempt: 1, job_name: 'core-sales-orders-1', status: 'failed',
            failure_class: 'task', failure_reason: 'exit_code', exit_code: 1,
            dispatched_at: { seconds: '1767261600', nanos: 0 },
            queued_at: { seconds: '1767261601', nanos: 0 },
            started_at: { seconds: '1767261605', nanos: 500_000_000 },
            finished_at: { seconds: '1767261625', nanos: 0 },
            execution_seconds: 19.5, error_message: 'boom',
            log_s3_key: 'logs/task-attempts/svc/sc/tbl/task-1-1.log',
            run_results_uri: 'run-results/task-attempts/svc/sc/tbl/task-1-1.json',
          },
          {
            task_id: 'task-2', attempt: 0, job_name: 'core-sales-items-0', status: 'dispatched',
            failure_class: '', failure_reason: '', exit_code: 0,
            dispatched_at: { seconds: '1767261600', nanos: 0 },
            queued_at: null, started_at: null, finished_at: null,
            execution_seconds: 0, error_message: '', log_s3_key: '', run_results_uri: '',
          },
        ],
        total_count: 2,
      });
    });
    const res = await request(app).get('/api/runs/abc-123/attempts');
    expect(res.status).toBe(200);
    expect(res.body.total_count).toBe(2);
    expect(res.body.attempts).toEqual([
      {
        task_id: 'task-1', attempt: 1, job_name: 'core-sales-orders-1', status: 'failed',
        failure_class: 'task', failure_reason: 'exit_code',
        dispatched_at: '2026-01-01T10:00:00.000Z', queued_at: '2026-01-01T10:00:01.000Z',
        started_at: '2026-01-01T10:00:05.500Z', finished_at: '2026-01-01T10:00:25.000Z',
        execution_seconds: 19.5, error_message: 'boom',
        log_s3_key: 'logs/task-attempts/svc/sc/tbl/task-1-1.log',
        run_results_uri: 'run-results/task-attempts/svc/sc/tbl/task-1-1.json',
      },
      {
        // Empty strings and a zero run time are run-controller's "not set": they cross
        // the route as null, so the client never renders a blank cell.
        task_id: 'task-2', attempt: 0, job_name: 'core-sales-items-0', status: 'dispatched',
        failure_class: null, failure_reason: null,
        dispatched_at: '2026-01-01T10:00:00.000Z', queued_at: null, started_at: null, finished_at: null,
        execution_seconds: null, error_message: null, log_s3_key: null, run_results_uri: null,
      },
    ]);
  });

  it('defaults to page_size 200 and page_offset 0', async () => {
    mockListRunAttempts.mockImplementation((_req: any, cb: any) => cb(null, { attempts: [], total_count: 0 }));
    await request(app).get('/api/runs/abc-123/attempts');
    expect(mockListRunAttempts).toHaveBeenLastCalledWith(
      { run_id: 'abc-123', task_id: '', page_size: 200, page_offset: 0 },
      expect.any(Function),
    );
  });

  it('forwards limit, offset and task_id', async () => {
    mockListRunAttempts.mockImplementation((_req: any, cb: any) => cb(null, { attempts: [], total_count: 0 }));
    await request(app).get('/api/runs/abc-123/attempts?limit=50&offset=200&task_id=task-7');
    expect(mockListRunAttempts).toHaveBeenLastCalledWith(
      { run_id: 'abc-123', task_id: 'task-7', page_size: 50, page_offset: 200 },
      expect.any(Function),
    );
  });

  it("clamps limit to run-controller's max of 200", async () => {
    mockListRunAttempts.mockImplementation((_req: any, cb: any) => cb(null, { attempts: [], total_count: 0 }));
    await request(app).get('/api/runs/abc-123/attempts?limit=500');
    expect(mockListRunAttempts).toHaveBeenLastCalledWith(
      expect.objectContaining({ page_size: 200 }),
      expect.any(Function),
    );
  });

  it('returns total_count in the body', async () => {
    mockListRunAttempts.mockImplementation((_req: any, cb: any) => cb(null, { attempts: [], total_count: 412 }));
    const res = await request(app).get('/api/runs/abc-123/attempts');
    expect(res.status).toBe(200);
    expect(res.body.total_count).toBe(412);
  });

  it('maps a gRPC status to its HTTP status', async () => {
    mockListRunAttempts
      .mockImplementationOnce((_req: any, cb: any) => cb(grpcError(grpc.status.INVALID_ARGUMENT, 'run_id is not a UUID')))
      .mockImplementationOnce((_req: any, cb: any) => cb(grpcError(grpc.status.NOT_FOUND, 'no such run')))
      .mockImplementationOnce((_req: any, cb: any) => cb(new Error('grpc error')));
    expect((await request(app).get('/api/runs/bad/attempts')).status).toBe(400);
    expect((await request(app).get('/api/runs/abc-123/attempts')).status).toBe(404);
    expect((await request(app).get('/api/runs/abc-123/attempts')).status).toBe(500);
  });
});

describe('routes/runs: GET /:id/graph', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('passes through run_promotion_seq and latest_promotion_seq as numbers', async () => {
    mockGetRunGraph.mockImplementation((_req: any, cb: any) =>
      cb(null, {
        nodes: [{ service_name: 's', schema_name: 'sch', table_name: 't', node_type: 'dbt-model' }],
        edges: [],
        run_promotion_seq: '5',     // gRPC-JS returns int64 as string
        latest_promotion_seq: '7',
      })
    );

    const res = await request(app).get('/api/runs/run-1/graph');

    expect(res.status).toBe(200);
    expect(mockGetRunGraph).toHaveBeenCalledWith({ run_id: 'run-1' }, expect.any(Function));
    expect(res.body.run_promotion_seq).toBe(5);
    expect(res.body.latest_promotion_seq).toBe(7);
    expect(typeof res.body.run_promotion_seq).toBe('number');
    expect(res.body).not.toHaveProperty('run_topology_generation');
    expect(res.body).not.toHaveProperty('latest_topology_generation');
  });

  it('returns task statuses as the run-controller service sends them', async () => {
    mockGetRunGraph.mockImplementation((_req: any, cb: any) =>
      cb(null, {
        nodes: [{ service_name: 's', schema_name: 'sch', table_name: 't', node_type: 'dbt-model', status: 'skipped' }],
        edges: [],
        run_promotion_seq: '0',
        latest_promotion_seq: '3',
      })
    );

    const res = await request(app).get('/api/runs/run-1/graph');

    expect(res.status).toBe(200);
    expect(res.body.nodes[0].status).toBe('skipped');
    expect(res.body.run_promotion_seq).toBe(0);
  });

  it("carries a node's lease wait: its reason, the holder run and when it began", async () => {
    mockGetRunGraph.mockImplementation((_req: any, cb: any) =>
      cb(null, {
        nodes: [
          { service_name: 's', schema_name: 'sch', table_name: 'a', node_type: 'dbt-model', status: 'pending',
            wait_reason: 'node_lease', waiting_on_run_id: 'run-9', waiting_since: '2026-10-24T10:05:00Z' },
          { service_name: 's', schema_name: 'sch', table_name: 'b', node_type: 'dbt-model', status: 'pending',
            wait_reason: '', waiting_on_run_id: '', waiting_since: '' },
        ],
        edges: [],
      })
    );

    const res = await request(app).get('/api/runs/run-1/graph');

    expect(res.body.nodes[0]).toMatchObject({
      wait_reason: 'node_lease', waiting_on_run_id: 'run-9', waiting_since: '2026-10-24T10:05:00Z',
    });
    expect(res.body.nodes[1]).toMatchObject({ wait_reason: null, waiting_on_run_id: null, waiting_since: null });
  });

  it('defaults missing promotion seq fields to 0', async () => {
    mockGetRunGraph.mockImplementation((_req: any, cb: any) => cb(null, { nodes: [], edges: [] }));

    const res = await request(app).get('/api/runs/run-1/graph');

    expect(res.status).toBe(200);
    expect(res.body.run_promotion_seq).toBe(0);
    expect(res.body.latest_promotion_seq).toBe(0);
  });

  it('emits edges upstream -> consumer, reversing the dependent -> dependency edge run-controller sends', async () => {
    mockGetRunGraph.mockImplementation((_req: any, cb: any) =>
      cb(null, {
        nodes: [],
        // orders depends on seed_orders
        edges: [{ from_node_id: 'svc.sales.orders', to_node_id: 'svc.raw.seed_orders' }],
      })
    );

    const res = await request(app).get('/api/runs/run-1/graph');

    expect(res.status).toBe(200);
    expect(res.body.edges).toEqual([{ from_node_id: 'svc.raw.seed_orders', to_node_id: 'svc.sales.orders' }]);
  });

  it('maps a gRPC status to its HTTP status', async () => {
    mockGetRunGraph
      .mockImplementationOnce((_req: any, cb: any) => cb(grpcError(grpc.status.INVALID_ARGUMENT, 'run_id is not a UUID')))
      .mockImplementationOnce((_req: any, cb: any) => cb(new Error('run-controller down')));
    expect((await request(app).get('/api/runs/bad/graph')).status).toBe(400);
    expect((await request(app).get('/api/runs/run-1/graph')).status).toBe(500);
  });
});

describe('routes/runs: GET /:id/tasks', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('returns tasks with the status run-controller sends', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, {
        tasks: [
          {
            task_id: 'task-456',
            run_id: 'abc-123',
            service_name: 'dbt',
            schema_name: 'analytics',
            table_name: 'users',
            job_name: 'dbt-analytics-users',
            status: 'succeeded',
            attempt: 2,
            max_retries: 3,
            created_at: { seconds: '1740268805', nanos: 0 },
          },
        ],
        total_count: 1,
      });
    });

    const res = await request(app).get('/api/runs/abc-123/tasks');

    expect(res.status).toBe(200);
    expect(res.body.tasks).toHaveLength(1);
    const t = res.body.tasks[0];
    expect(t.task_id).toBe('task-456');
    expect(t.run_id).toBe('abc-123');
    expect(t).not.toHaveProperty('schedule_id');
    expect(t.status).toBe('succeeded');
    expect(t.attempt).toBe(2);
    expect(Object.keys(t).filter(k => k.startsWith('retry'))).toEqual([]);
    expect(t.max_retries).toBe(3);
    expect(t.created_at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it('reads a stopping task as stopping', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, {
        tasks: [{ task_id: 't1', run_id: 'abc-123', status: 'stopping', attempt: 0, max_retries: 0 }],
        total_count: 1,
      });
    });
    const res = await request(app).get('/api/runs/abc-123/tasks');
    expect(res.body.tasks[0].status).toBe('stopping');
  });

  it('carries a lease wait: its reason, the holder run and when it began', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, {
        tasks: [
          { task_id: 't1', run_id: 'abc-123', status: 'pending', attempt: 0, max_retries: 0,
            wait_reason: 'node_lease', waiting_on_run_id: 'run-9', waiting_since: { seconds: '1792836300', nanos: 0 } },
          { task_id: 't2', run_id: 'abc-123', status: 'pending', attempt: 0, max_retries: 0,
            wait_reason: '', waiting_on_run_id: '' },
        ],
        total_count: 2,
      });
    });
    const res = await request(app).get('/api/runs/abc-123/tasks');
    expect(res.body.tasks[0].wait_reason).toBe('node_lease');
    expect(res.body.tasks[0].waiting_on_run_id).toBe('run-9');
    expect(res.body.tasks[0].waiting_since).toBe('2026-10-24T10:05:00.000Z');
    expect(res.body.tasks[1].wait_reason).toBeNull();
    expect(res.body.tasks[1].waiting_on_run_id).toBeNull();
    expect(res.body.tasks[1].waiting_since).toBeNull();
  });

  it('passes run_id to gRPC call', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 0 });
    });

    await request(app).get('/api/runs/abc-123/tasks');

    expect(mockListTasks).toHaveBeenCalledWith(
      expect.objectContaining({ run_id: 'abc-123' }),
      expect.any(Function)
    );
  });

  it('returns 500 when gRPC fails', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(new Error('not found'), null);
    });

    const res = await request(app).get('/api/runs/bad-id/tasks');
    expect(res.status).toBe(500);
  });

  it('defaults to page_size 200 and page_offset 0 when no query params', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 0 });
    });

    await request(app).get('/api/runs/abc-123/tasks');

    expect(mockListTasks).toHaveBeenCalledWith(
      expect.objectContaining({ run_id: 'abc-123', page_size: 200, page_offset: 0 }),
      expect.any(Function)
    );
  });

  it('forwards limit and offset query params', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 0 });
    });

    await request(app).get('/api/runs/abc-123/tasks?limit=50&offset=100');

    expect(mockListTasks).toHaveBeenCalledWith(
      expect.objectContaining({ page_size: 50, page_offset: 100 }),
      expect.any(Function)
    );
  });

  it('clamps limit above the max of 500', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 0 });
    });

    await request(app).get('/api/runs/abc-123/tasks?limit=9999');

    expect(mockListTasks).toHaveBeenCalledWith(
      expect.objectContaining({ page_size: 500 }),
      expect.any(Function)
    );
  });

  it('falls back to the default for a malformed limit', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 0 });
    });

    await request(app).get('/api/runs/abc-123/tasks?limit=abc');

    expect(mockListTasks).toHaveBeenCalledWith(
      expect.objectContaining({ page_size: 200 }),
      expect.any(Function)
    );
  });

  it('returns total_count in the body', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 412 });
    });

    const res = await request(app).get('/api/runs/abc-123/tasks');
    expect(res.status).toBe(200);
    expect(res.body.total_count).toBe(412);
    expect(res.body.tasks).toEqual([]);
  });

  it('returns 400 for INVALID_ARGUMENT', async () => {
    const err = Object.assign(new Error('invalid run_id format'), {
      code: grpc.status.INVALID_ARGUMENT,
    });
    mockListTasks.mockImplementation((_req: any, cb: any) => cb(err, null));

    const res = await request(app).get('/api/runs/not-a-uuid/tasks');
    expect(res.status).toBe(400);
  });

  it('returns 404 for NOT_FOUND', async () => {
    const err = Object.assign(new Error('no such run'), {
      code: grpc.status.NOT_FOUND,
    });
    mockListTasks.mockImplementation((_req: any, cb: any) => cb(err, null));

    const res = await request(app).get('/api/runs/abc-123/tasks');
    expect(res.status).toBe(404);
  });
});

describe('routes/runs: POST /:id/rebase', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('forwards the toggle and the previewed release and returns 200', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) => cb(null, { run_id: 'run-2', schedule_name: 'daily' }));

    const res = await request(app)
      .post(`/api/runs/${VALID_ID}/rebase`)
      .send({ request_id: REQUEST_ID, include_changed_upstream: true, expected_release_id: 'rel-7' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ run_id: 'run-2', schedule_name: 'daily' });
    expect(mockTriggerRebase).toHaveBeenCalledWith(
      { request_id: REQUEST_ID, source_run_id: VALID_ID, include_changed_upstream: true, expected_release_id: 'rel-7' },
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('defaults an absent toggle to false and an absent release to no check', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) => cb(null, {}));

    const res = await request(app).post(`/api/runs/${VALID_ID}/rebase`).send({ request_id: REQUEST_ID });

    expect(res.status).toBe(200);
    expect(mockTriggerRebase.mock.calls.map(c => c[0])).toEqual([
      { request_id: REQUEST_ID, source_run_id: VALID_ID, include_changed_upstream: false, expected_release_id: '' },
    ]);
  });

  it.each([
    [{ include_changed_upstream: 'true' }],
    [{ include_changed_upstream: 1 }],
    [{ include_changed_upstream: null }],
    [{ expected_release_id: 7 }],
    [{ expected_release_id: null }],
    [[true]],
  ])('refuses the body %j with 400 before calling run-controller', async (body) => {
    const res = await request(app).post(`/api/runs/${VALID_ID}/rebase`)
      .send(Array.isArray(body) ? body : { request_id: REQUEST_ID, ...(body as object) });

    expect(res.status).toBe(400);
    expect(typeof res.body.error).toBe('string');
    expect(mockTriggerRebase).not.toHaveBeenCalled();
  });

  it('answers 409 with code release_moved when the live release is not the previewed one', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.FAILED_PRECONDITION, RELEASE_MOVED_MESSAGE)));

    const res = await request(app)
      .post(`/api/runs/${VALID_ID}/rebase`)
      .send({ request_id: REQUEST_ID, include_changed_upstream: true, expected_release_id: 'rel-6' });

    expect(res.status).toBe(409);
    expect(res.body).toEqual({ error: 'the live release changed; preview again', code: 'release_moved' });
  });

  it('keeps any other FAILED_PRECONDITION a plain 409 without a code', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.FAILED_PRECONDITION, 'schedule already has an active run')));

    const res = await request(app).post(`/api/runs/${VALID_ID}/rebase`).send({ request_id: REQUEST_ID });

    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/already has an active run/);
    expect(res.body.code).toBeUndefined();
  });

  it('reads the release-moved message only from a FAILED_PRECONDITION', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.INTERNAL, RELEASE_MOVED_MESSAGE)));

    const res = await request(app).post(`/api/runs/${VALID_ID}/rebase`).send({ request_id: REQUEST_ID });

    expect(res.status).toBe(500);
    expect(res.body.code).toBeUndefined();
  });

  it('answers a maintenance refusal with 503 maintenance', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.UNAVAILABLE, 'maintenance', { [MAINTENANCE_TRAILER_KEY]: 'true' })));

    const res = await request(app).post(`/api/runs/${VALID_ID}/rebase`).send({ request_id: REQUEST_ID });

    expect(res.status).toBe(503);
    expect(res.body.code).toBe(MAINTENANCE_CODE);
  });

  it('maps NOT_FOUND and INVALID_ARGUMENT as every other route does', async () => {
    mockTriggerRebase
      .mockImplementationOnce((_req: any, _md: any, cb: any) => cb(grpcError(grpc.status.NOT_FOUND, 'source run not found')))
      .mockImplementationOnce((_req: any, _md: any, cb: any) => cb(grpcError(grpc.status.INVALID_ARGUMENT, 'invalid source_run_id')));

    const missing = await request(app).post(`/api/runs/${VALID_ID}/rebase`).send({ request_id: REQUEST_ID });
    const malformed = await request(app).post('/api/runs/not-a-uuid/rebase').send({ request_id: REQUEST_ID });

    expect(missing.status).toBe(404);
    expect(malformed.status).toBe(400);
  });

  it.each([
    ['no body', undefined],
    ['an empty body', {}],
    ['an empty request_id', { request_id: '' }],
    ['a request_id that is not a UUID', { request_id: 'not-a-uuid', include_changed_upstream: true }],
  ])('refuses a missing or malformed request_id with 400: %s', async (_label, body) => {
    const req = request(app).post(`/api/runs/${VALID_ID}/rebase`);
    const res = await (body === undefined ? req : req.send(body));

    expect(res.status).toBe(400);
    expect(res.body).toEqual({ error: 'request_id must be a UUID' });
    expect(mockTriggerRebase).not.toHaveBeenCalled();
  });

  it('answers ALREADY_EXISTS (request_id reused with different parameters) with 409', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.ALREADY_EXISTS, 'request_id reused with different parameters')));

    const res = await request(app).post(`/api/runs/${VALID_ID}/rebase`).send({ request_id: REQUEST_ID });

    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/request_id reused/);
  });
});

describe('routes/runs: GET /:id/rebase-preview', () => {
  beforeEach(() => { vi.clearAllMocks(); });

  it('forwards the toggle and answers the preview in snake_case, the int64 as a number', async () => {
    mockPreviewRebase.mockImplementation((_req: any, cb: any) => cb(null, {
      release_id: 'rel-7', promotion_seq: '7', total_nodes: 2000, will_run: 41,
      failed_or_skipped: 3, new_nodes: 1, changed_upstream: 2, downstream: 35, inherited: 1959,
      changed_nodes: [
        { service_name: 'billing', schema_name: 'stg', table_name: 'invoices', downstream: 3, last_success_seconds: 0 },
        { service_name: 'core', schema_name: 'stg', table_name: 'orders', downstream: 18, last_success_seconds: 252.4 },
      ],
    }));

    const res = await request(app).get(`/api/runs/${VALID_ID}/rebase-preview?include_changed_upstream=true`);

    expect(res.status).toBe(200);
    expect(mockPreviewRebase).toHaveBeenCalledWith(
      { source_run_id: VALID_ID, include_changed_upstream: true },
      expect.any(Function),
    );
    expect(res.body).toEqual({
      release_id: 'rel-7', promotion_seq: 7, total_nodes: 2000, will_run: 41,
      failed_or_skipped: 3, new_nodes: 1, changed_upstream: 2, downstream: 35, inherited: 1959,
      changed_nodes: [
        { service_name: 'billing', schema_name: 'stg', table_name: 'invoices', downstream: 3, last_success_seconds: null },
        { service_name: 'core', schema_name: 'stg', table_name: 'orders', downstream: 18, last_success_seconds: 252.4 },
      ],
    });
  });

  it('answers an empty preview with zero counts and no changed nodes', async () => {
    mockPreviewRebase.mockImplementation((_req: any, cb: any) => cb(null, {
      release_id: 'rel-7', promotion_seq: '0', total_nodes: 0, will_run: 0,
      failed_or_skipped: 0, new_nodes: 0, changed_upstream: 0, downstream: 0, inherited: 0,
      changed_nodes: [],
    }));

    const res = await request(app).get(`/api/runs/${VALID_ID}/rebase-preview`);

    expect(res.status).toBe(200);
    expect(res.body.promotion_seq).toBe(0);
    expect(res.body.changed_nodes).toEqual([]);
  });

  it('reads include_changed_upstream=false and an absent toggle as false', async () => {
    mockPreviewRebase.mockImplementation((_req: any, cb: any) => cb(null, { changed_nodes: [] }));

    await request(app).get(`/api/runs/${VALID_ID}/rebase-preview?include_changed_upstream=false`);
    await request(app).get(`/api/runs/${VALID_ID}/rebase-preview`);

    expect(mockPreviewRebase.mock.calls.map(c => c[0])).toEqual([
      { source_run_id: VALID_ID, include_changed_upstream: false },
      { source_run_id: VALID_ID, include_changed_upstream: false },
    ]);
  });

  it.each(['maybe', 'TRUE', '1', '', 'true&include_changed_upstream=false'])(
    'refuses include_changed_upstream=%s with 400 before calling run-controller',
    async (value) => {
      const res = await request(app).get(`/api/runs/${VALID_ID}/rebase-preview?include_changed_upstream=${value}`);

      expect(res.status).toBe(400);
      expect(typeof res.body.error).toBe('string');
      expect(mockPreviewRebase).not.toHaveBeenCalled();
    },
  );

  it('maps NOT_FOUND and FAILED_PRECONDITION to 404 and 409 with the message', async () => {
    mockPreviewRebase
      .mockImplementationOnce((_req: any, cb: any) => cb(grpcError(grpc.status.NOT_FOUND, 'source run not found')))
      .mockImplementationOnce((_req: any, cb: any) => cb(grpcError(grpc.status.FAILED_PRECONDITION, 'a test run cannot be rebased')));

    const missing = await request(app).get(`/api/runs/${VALID_ID}/rebase-preview`);
    const refused = await request(app).get(`/api/runs/${VALID_ID}/rebase-preview`);

    expect(missing.status).toBe(404);
    expect(refused.status).toBe(409);
    expect(refused.body.error).toMatch(/test run/);
    expect(refused.body.code).toBeUndefined();
  });

  it('answers INTERNAL with 500', async () => {
    mockPreviewRebase.mockImplementation((_req: any, cb: any) => cb(grpcError(grpc.status.INTERNAL, 'boom')));

    const res = await request(app).get(`/api/runs/${VALID_ID}/rebase-preview`);

    expect(res.status).toBe(500);
  });
});
