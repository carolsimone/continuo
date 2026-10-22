import { describe, it, expect, vi, beforeEach } from 'vitest';
import request from 'supertest';
import express from 'express';
import * as grpc from '@grpc/grpc-js';
import { createSchedulersRouter, RELEASE_MOVED_MESSAGE } from '../../src/server/routes/schedulers';
import { MAINTENANCE_CODE, MAINTENANCE_TRAILER_KEY } from '../../src/server/maintenance';

const mockListTasks = vi.fn();
const mockGetScheduler = vi.fn();
const mockTriggerRerun = vi.fn();
const mockTriggerRebase = vi.fn();
const mockPreviewRebase = vi.fn();

const mockClient = {
  listTasks: mockListTasks,
  getScheduler: mockGetScheduler,
  triggerRerun: mockTriggerRerun,
  triggerRebase: mockTriggerRebase,
  previewRebase: mockPreviewRebase,
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
            attempt: 2,
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
    expect(t.attempt).toBe(2);
    expect(Object.keys(t).filter(k => k.startsWith('retry'))).toEqual([]);
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

  it('defaults to page_size 200 and page_offset 0 when no query params', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/tasks');

    expect(mockListTasks).toHaveBeenCalledWith(
      expect.objectContaining({ schedule_id: 'abc-123', page_size: 200, page_offset: 0 }),
      expect.any(Function)
    );
  });

  it('forwards limit and offset query params', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/tasks?limit=50&offset=100');

    expect(mockListTasks).toHaveBeenCalledWith(
      expect.objectContaining({ page_size: 50, page_offset: 100 }),
      expect.any(Function)
    );
  });

  it('clamps limit above the max of 500', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/tasks?limit=9999');

    expect(mockListTasks).toHaveBeenCalledWith(
      expect.objectContaining({ page_size: 500 }),
      expect.any(Function)
    );
  });

  it('falls back to the default for a malformed limit', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 0 });
    });

    await request(app).get('/api/schedulers/abc-123/tasks?limit=abc');

    expect(mockListTasks).toHaveBeenCalledWith(
      expect.objectContaining({ page_size: 200 }),
      expect.any(Function)
    );
  });

  it('returns total_count in the body', async () => {
    mockListTasks.mockImplementation((_req: any, callback: any) => {
      callback(null, { tasks: [], total_count: 412 });
    });

    const res = await request(app).get('/api/schedulers/abc-123/tasks');
    expect(res.status).toBe(200);
    expect(res.body.total_count).toBe(412);
    expect(res.body.tasks).toEqual([]);
  });

  it('returns 400 for INVALID_ARGUMENT', async () => {
    const err = Object.assign(new Error('invalid schedule_id format'), {
      code: grpc.status.INVALID_ARGUMENT,
    });
    mockListTasks.mockImplementation((_req: any, cb: any) => cb(err, null));

    const res = await request(app).get('/api/schedulers/not-a-uuid/tasks');
    expect(res.status).toBe(400);
  });

  it('returns 404 for NOT_FOUND', async () => {
    const err = Object.assign(new Error('no such schedule'), {
      code: grpc.status.NOT_FOUND,
    });
    mockListTasks.mockImplementation((_req: any, cb: any) => cb(err, null));

    const res = await request(app).get('/api/schedulers/abc-123/tasks');
    expect(res.status).toBe(404);
  });
});

describe('POST /api/schedulers/:id/rerun', () => {
  const VALID_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';

  beforeEach(() => { vi.clearAllMocks(); });

  it('returns 200 on success and forwards source_run_id from path', async () => {
    mockTriggerRerun.mockImplementation((req: any, _md: any, cb: any) => {
      expect(req).toEqual({ source_run_id: VALID_ID });
      cb(null);
    });
    const res = await request(app).post(`/api/schedulers/${VALID_ID}/rerun`).send({});
    expect(res.status).toBe(200);
    expect(mockTriggerRerun).toHaveBeenCalledWith(
      { source_run_id: VALID_ID },
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('returns 400 for INVALID_ARGUMENT', async () => {
    const err = Object.assign(new Error('invalid schedule_id format'), {
      code: grpc.status.INVALID_ARGUMENT,
    });
    mockTriggerRerun.mockImplementation((_req: any, _md: any, cb: any) => cb(err));
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
    mockTriggerRerun.mockImplementation((_req: any, _md: any, cb: any) => cb(err));
    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rerun`)
      .send({});
    expect(res.status).toBe(404);
    expect(res.body.error).toMatch(/schedule not found/);
  });

  it('returns 409 for FAILED_PRECONDITION (running tasks)', async () => {
    const err = Object.assign(new Error('schedule has running tasks'), {
      code: grpc.status.FAILED_PRECONDITION,
    });
    mockTriggerRerun.mockImplementation((_req: any, _md: any, cb: any) => cb(err));
    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rerun`)
      .send({});
    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/running tasks/);
  });

  it('returns 500 for INTERNAL', async () => {
    const err = Object.assign(new Error('internal error'), {
      code: grpc.status.INTERNAL,
    });
    mockTriggerRerun.mockImplementation((_req: any, _md: any, cb: any) => cb(err));
    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rerun`)
      .send({});
    expect(res.status).toBe(500);
  });

  it('returns 500 for unknown gRPC code', async () => {
    const err = Object.assign(new Error('something weird'), { code: 999 });
    mockTriggerRerun.mockImplementation((_req: any, _md: any, cb: any) => cb(err));
    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rerun`)
      .send({});
    expect(res.status).toBe(500);
  });
});

// grpcError builds a failed call the way @grpc/grpc-js reports one: the
// message carries the code's prefix, details the server's own text.
function grpcError(code: number, details: string, trailer?: Record<string, string>) {
  const metadata = new grpc.Metadata();
  for (const [k, v] of Object.entries(trailer ?? {})) metadata.set(k, v);
  return Object.assign(new Error(`${code} ${grpc.status[code]}: ${details}`), { code, details, metadata });
}

describe('POST /api/schedulers/:id/rebase', () => {
  const VALID_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';

  beforeEach(() => { vi.clearAllMocks(); });

  it('forwards the toggle and the previewed release and returns 200', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) => cb(null, {}));

    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rebase`)
      .send({ include_changed_upstream: true, expected_release_id: 'rel-7' });

    expect(res.status).toBe(200);
    expect(mockTriggerRebase).toHaveBeenCalledWith(
      { source_run_id: VALID_ID, include_changed_upstream: true, expected_release_id: 'rel-7' },
      expect.any(Object),
      expect.any(Function),
    );
  });

  it('defaults an absent toggle to false and an absent release to no check', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) => cb(null, {}));

    const withEmptyBody = await request(app).post(`/api/schedulers/${VALID_ID}/rebase`).send({});
    const withNoBody = await request(app).post(`/api/schedulers/${VALID_ID}/rebase`);

    expect(withEmptyBody.status).toBe(200);
    expect(withNoBody.status).toBe(200);
    expect(mockTriggerRebase.mock.calls.map(c => c[0])).toEqual([
      { source_run_id: VALID_ID, include_changed_upstream: false, expected_release_id: '' },
      { source_run_id: VALID_ID, include_changed_upstream: false, expected_release_id: '' },
    ]);
  });

  it.each([
    [{ include_changed_upstream: 'true' }],
    [{ include_changed_upstream: 1 }],
    [{ include_changed_upstream: null }],
    [{ expected_release_id: 7 }],
    [{ expected_release_id: null }],
    [[true]],
  ])('refuses the body %j with 400 before calling state', async (body) => {
    const res = await request(app).post(`/api/schedulers/${VALID_ID}/rebase`).send(body as object);

    expect(res.status).toBe(400);
    expect(typeof res.body.error).toBe('string');
    expect(mockTriggerRebase).not.toHaveBeenCalled();
  });

  it('answers 409 with code release_moved when the live release is not the previewed one', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.FAILED_PRECONDITION, RELEASE_MOVED_MESSAGE)));

    const res = await request(app)
      .post(`/api/schedulers/${VALID_ID}/rebase`)
      .send({ include_changed_upstream: true, expected_release_id: 'rel-6' });

    expect(res.status).toBe(409);
    expect(res.body).toEqual({ error: 'the live release changed; preview again', code: 'release_moved' });
  });

  it('keeps any other FAILED_PRECONDITION a plain 409 without a code', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.FAILED_PRECONDITION, 'schedule already has an active run')));

    const res = await request(app).post(`/api/schedulers/${VALID_ID}/rebase`).send({});

    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/already has an active run/);
    expect(res.body.code).toBeUndefined();
  });

  it('reads the release-moved message only from a FAILED_PRECONDITION', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.INTERNAL, RELEASE_MOVED_MESSAGE)));

    const res = await request(app).post(`/api/schedulers/${VALID_ID}/rebase`).send({});

    expect(res.status).toBe(500);
    expect(res.body.code).toBeUndefined();
  });

  it('answers a maintenance refusal with 503 maintenance', async () => {
    mockTriggerRebase.mockImplementation((_req: any, _md: any, cb: any) =>
      cb(grpcError(grpc.status.UNAVAILABLE, 'maintenance', { [MAINTENANCE_TRAILER_KEY]: 'true' })));

    const res = await request(app).post(`/api/schedulers/${VALID_ID}/rebase`).send({});

    expect(res.status).toBe(503);
    expect(res.body.code).toBe(MAINTENANCE_CODE);
  });

  it('maps NOT_FOUND and INVALID_ARGUMENT as every other route does', async () => {
    mockTriggerRebase
      .mockImplementationOnce((_req: any, _md: any, cb: any) => cb(grpcError(grpc.status.NOT_FOUND, 'source run not found')))
      .mockImplementationOnce((_req: any, _md: any, cb: any) => cb(grpcError(grpc.status.INVALID_ARGUMENT, 'invalid source_run_id')));

    const missing = await request(app).post(`/api/schedulers/${VALID_ID}/rebase`).send({});
    const malformed = await request(app).post('/api/schedulers/not-a-uuid/rebase').send({});

    expect(missing.status).toBe(404);
    expect(malformed.status).toBe(400);
  });
});

describe('GET /api/schedulers/:id/rebase-preview', () => {
  const VALID_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';

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

    const res = await request(app).get(`/api/schedulers/${VALID_ID}/rebase-preview?include_changed_upstream=true`);

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

    const res = await request(app).get(`/api/schedulers/${VALID_ID}/rebase-preview`);

    expect(res.status).toBe(200);
    expect(res.body.promotion_seq).toBe(0);
    expect(res.body.changed_nodes).toEqual([]);
  });

  it('reads include_changed_upstream=false and an absent toggle as false', async () => {
    mockPreviewRebase.mockImplementation((_req: any, cb: any) => cb(null, { changed_nodes: [] }));

    await request(app).get(`/api/schedulers/${VALID_ID}/rebase-preview?include_changed_upstream=false`);
    await request(app).get(`/api/schedulers/${VALID_ID}/rebase-preview`);

    expect(mockPreviewRebase.mock.calls.map(c => c[0])).toEqual([
      { source_run_id: VALID_ID, include_changed_upstream: false },
      { source_run_id: VALID_ID, include_changed_upstream: false },
    ]);
  });

  it.each(['maybe', 'TRUE', '1', '', 'true&include_changed_upstream=false'])(
    'refuses include_changed_upstream=%s with 400 before calling state',
    async (value) => {
      const res = await request(app).get(`/api/schedulers/${VALID_ID}/rebase-preview?include_changed_upstream=${value}`);

      expect(res.status).toBe(400);
      expect(typeof res.body.error).toBe('string');
      expect(mockPreviewRebase).not.toHaveBeenCalled();
    },
  );

  it('maps NOT_FOUND and FAILED_PRECONDITION to 404 and 409 with the message', async () => {
    mockPreviewRebase
      .mockImplementationOnce((_req: any, cb: any) => cb(grpcError(grpc.status.NOT_FOUND, 'source run not found')))
      .mockImplementationOnce((_req: any, cb: any) => cb(grpcError(grpc.status.FAILED_PRECONDITION, 'a test run cannot be rebased')));

    const missing = await request(app).get(`/api/schedulers/${VALID_ID}/rebase-preview`);
    const refused = await request(app).get(`/api/schedulers/${VALID_ID}/rebase-preview`);

    expect(missing.status).toBe(404);
    expect(refused.status).toBe(409);
    expect(refused.body.error).toMatch(/test run/);
    expect(refused.body.code).toBeUndefined();
  });

  it('answers INTERNAL with 500', async () => {
    mockPreviewRebase.mockImplementation((_req: any, cb: any) => cb(grpcError(grpc.status.INTERNAL, 'boom')));

    const res = await request(app).get(`/api/schedulers/${VALID_ID}/rebase-preview`);

    expect(res.status).toBe(500);
  });
});
