import { describe, it, expect } from 'vitest';
import * as protoLoader from '@grpc/proto-loader';
import * as grpc from '@grpc/grpc-js';
import path from 'path';
import express from 'express';
import request from 'supertest';
import { createRunsRouter } from '../../src/server/routes/runs';
import { createSchedulesRouter } from '../../src/server/routes/schedules';

const PROTO_DIR = path.join(process.cwd(), 'proto');
const PROTO_PATH = path.join(PROTO_DIR, 'run.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [PROTO_DIR],
});

const grpcObject = grpc.loadPackageDefinition(packageDefinition) as any;
const Messages = grpcObject.run.v1;

// requestRoundTrip drives the production RPC's request serializer/deserializer
// pair — the same code path @grpc/grpc-js uses to encode arguments to a unary
// call. Encoding then decoding the request reveals which fields cross the wire
// vs. get silently dropped by a stale proto.
function requestRoundTrip(rpcName: string, request: Record<string, unknown>): Record<string, unknown> {
  const svc = (packageDefinition as Record<string, any>)['run.v1.RunService'];
  const method = svc[rpcName];
  expect(method, `missing rpc method: ${rpcName}`).toBeTruthy();
  const buf: Buffer = method.requestSerialize(request);
  return method.requestDeserialize(buf) as Record<string, unknown>;
}

// runMethod returns the production serializer/deserializer pairs of one
// RunService RPC.
function runMethod(rpcName: string): Record<string, any> {
  const method = (packageDefinition as Record<string, any>)['run.v1.RunService'][rpcName];
  expect(method, `missing rpc method: ${rpcName}`).toBeTruthy();
  return method;
}

// decodeResponse runs the production response deserializer of one
// RunService RPC over raw wire bytes.
function decodeResponse(rpcName: string, bytes: number[]): Record<string, any> {
  const svc = (packageDefinition as Record<string, any>)['run.v1.RunService'];
  const method = svc[rpcName];
  expect(method, `missing rpc method: ${rpcName}`).toBeTruthy();
  return method.responseDeserialize(Buffer.from(bytes)) as Record<string, any>;
}

const REQUEST_ID = '5d21c0de-0000-4000-8000-000000000001';

describe('proto wire: run.proto', () => {
  it('run.proto loads RunService with every RPC the ui server calls', () => {
    const svc = (packageDefinition as Record<string, any>)['run.v1.RunService'];
    expect(svc).toBeTruthy();
    for (const rpc of [
      'StartRun', 'TriggerRerun', 'TriggerSingleNodeRun', 'TriggerRebase', 'PreviewRebase',
      'CancelRun', 'GetRun', 'ListRuns', 'ListTasks', 'ListRunAttempts', 'GetRunGraph',
      'ListNodeTasks', 'ListNodes', 'ListNodeNames', 'ListNodeServices',
      'ListSchedules', 'GetScheduleGraph', 'ListScheduleTopologies',
      'UpsertSchedule', 'DeleteSchedule', 'PreviewSchedule',
    ]) {
      expect(svc[rpc], `missing rpc method: ${rpc}`).toBeTruthy();
    }
  });

  it('TriggerRerun request round-trips request_id and source_run_id', () => {
    const result = requestRoundTrip('TriggerRerun', {
      request_id: '5d21c0de-0000-4000-8000-000000000001',
      source_run_id: 'run-abc-123',
    });
    expect(result).toEqual({ request_id: '5d21c0de-0000-4000-8000-000000000001', source_run_id: 'run-abc-123' });
    expect(Object.keys(result)).not.toContain('schedule_id');
  });

  it('StartRun request round-trips request_id, schedule_name and operation', () => {
    expect(requestRoundTrip('StartRun', {
      request_id: '5d21c0de-0000-4000-8000-000000000001', schedule_name: 'seed', operation: 'test',
    })).toEqual({ request_id: '5d21c0de-0000-4000-8000-000000000001', schedule_name: 'seed', operation: 'test' });
  });

  it('StartRun response decodes run_id at field 1', () => {
    expect(decodeResponse('StartRun', [0x0a, 2, 0x72, 0x31]).run_id).toBe('r1');
  });

  it('CancelRun request round-trips run_id, cancelled_by and cancellation_reason', () => {
    const result = requestRoundTrip('CancelRun', {
      run_id: 'run-abc-123', cancelled_by: 'tester', cancellation_reason: 'aborted by hand',
    });
    expect(result).toEqual({ run_id: 'run-abc-123', cancelled_by: 'tester', cancellation_reason: 'aborted by hand' });
    expect(Object.keys(result)).not.toContain('request_id');
  });

  it('TriggerSingleNodeRun request round-trips request_id and the node identity', () => {
    const sent = {
      request_id: '5d21c0de-0000-4000-8000-000000000001', service_name: 'svc', schema_name: 'sch',
      table_name: 'tbl', metadata_source: 'snapshot_of_run', source_run_id: 'run-1', operation: 'full_refresh',
    };
    expect(requestRoundTrip('TriggerSingleNodeRun', sent)).toEqual(sent);
  });

  it('GetRunGraph decodes fields 3 and 4 as run_promotion_seq and latest_promotion_seq', () => {
    const resp = decodeResponse('GetRunGraph', [0x18, 5, 0x20, 7]);
    expect(resp.run_promotion_seq).toBe('5');
    expect(resp.latest_promotion_seq).toBe('7');
  });

  it('GetScheduleGraph decodes field 3 as promotion_seq', () => {
    const resp = decodeResponse('GetScheduleGraph', [0x18, 42]);
    expect(resp.promotion_seq).toBe('42');
  });

  it('ListActiveRunDrifts decodes field 1 and ActiveRunDrift field 3 as promotion seqs', () => {
    const resp = decodeResponse('ListActiveRunDrifts', [0x08, 9, 0x12, 2, 0x18, 4]);
    expect(resp.latest_promotion_seq).toBe('9');
    expect(resp.active_runs).toHaveLength(1);
    expect(resp.active_runs[0].run_promotion_seq).toBe('4');
  });

  it('TriggerRebase request carries request_id as field 1, source_run_id as 2, include_changed_upstream as 3 and expected_release_id as 4', () => {
    const method = runMethod('TriggerRebase');
    const bytes = [...method.requestSerialize({
      request_id: 'q', source_run_id: 'r', include_changed_upstream: true, expected_release_id: 'x',
    })];
    expect(bytes).toEqual([0x0a, 1, 0x71, 0x12, 1, 0x72, 0x18, 1, 0x22, 1, 0x78]);
    const sent = {
      request_id: '5d21c0de-0000-4000-8000-000000000001', source_run_id: 'run-abc-123',
      include_changed_upstream: true, expected_release_id: 'rel-7',
    };
    expect(requestRoundTrip('TriggerRebase', sent)).toEqual(sent);
  });

  it('PreviewRebase request carries source_run_id as field 1 and include_changed_upstream as field 2', () => {
    const method = runMethod('PreviewRebase');
    expect([...method.requestSerialize({ source_run_id: 'r', include_changed_upstream: true })])
      .toEqual([0x0a, 1, 0x72, 0x10, 1]);
    expect(requestRoundTrip('PreviewRebase', { source_run_id: 'run-abc-123', include_changed_upstream: true }))
      .toEqual({ source_run_id: 'run-abc-123', include_changed_upstream: true });
  });

  it('PreviewRebase response decodes every field at its contract number', () => {
    const lastSuccess = Buffer.alloc(8);
    lastSuccess.writeDoubleLE(252.5);
    const changedNode = [
      0x0a, 1, 0x63, // service_name = "c"
      0x12, 1, 0x73, // schema_name = "s"
      0x1a, 1, 0x74, // table_name = "t"
      0x20, 18, // downstream = 18
      0x29, ...lastSuccess, // last_success_seconds = 252.5 (double)
    ];
    const resp = decodeResponse('PreviewRebase', [
      0x0a, 2, 0x72, 0x37, // release_id = "r7"
      0x10, 7, // promotion_seq = 7
      0x18, 9, // total_nodes
      0x20, 4, // will_run
      0x28, 1, // failed_or_skipped
      0x30, 1, // new_nodes
      0x38, 1, // changed_upstream
      0x40, 1, // downstream
      0x48, 5, // inherited
      0x52, changedNode.length, ...changedNode, // changed_nodes
    ]);
    expect(resp).toEqual({
      release_id: 'r7', promotion_seq: '7', total_nodes: 9, will_run: 4,
      failed_or_skipped: 1, new_nodes: 1, changed_upstream: 1, downstream: 1, inherited: 5,
      changed_nodes: [{ service_name: 'c', schema_name: 's', table_name: 't', downstream: 18, last_success_seconds: 252.5 }],
    });
  });

  it('the rebase routes send and read the field names the proto declares', async () => {
    // Each fake RPC passes its request and its response through the real
    // serializers, so a field name the route misspells is dropped on the wire
    // and the assertions below see its default instead of the value.
    const sentRebase: Record<string, unknown>[] = [];
    const sentPreview: Record<string, unknown>[] = [];
    const preview = runMethod('PreviewRebase');
    const rebase = runMethod('TriggerRebase');
    const client = {
      triggerRebase: (req: any, _md: unknown, cb: (err: unknown, res: unknown) => void) => {
        sentRebase.push(rebase.requestDeserialize(rebase.requestSerialize(req)));
        cb(null, rebase.responseDeserialize(rebase.responseSerialize({ run_id: 'run-2', schedule_name: 'daily' })));
      },
      previewRebase: (req: any, cb: (err: unknown, res: unknown) => void) => {
        sentPreview.push(preview.requestDeserialize(preview.requestSerialize(req)));
        cb(null, preview.responseDeserialize(preview.responseSerialize({
          release_id: 'rel-7', promotion_seq: 7, total_nodes: 2000, will_run: 41,
          failed_or_skipped: 3, new_nodes: 1, changed_upstream: 2, downstream: 35, inherited: 1959,
          changed_nodes: [{ service_name: 'core', schema_name: 'stg', table_name: 'orders', downstream: 18, last_success_seconds: 252.5 }],
        })));
      },
    };
    const app = express();
    app.use(express.json());
    app.use('/api/runs', createRunsRouter(client as any));

    const got = await request(app).get('/api/runs/run-1/rebase-preview?include_changed_upstream=true');
    const posted = await request(app).post('/api/runs/run-1/rebase')
      .send({ request_id: REQUEST_ID, include_changed_upstream: true, expected_release_id: 'rel-7' });

    expect(sentPreview).toEqual([{ source_run_id: 'run-1', include_changed_upstream: true }]);
    expect(got.body).toEqual({
      release_id: 'rel-7', promotion_seq: 7, total_nodes: 2000, will_run: 41,
      failed_or_skipped: 3, new_nodes: 1, changed_upstream: 2, downstream: 35, inherited: 1959,
      changed_nodes: [{ service_name: 'core', schema_name: 'stg', table_name: 'orders', downstream: 18, last_success_seconds: 252.5 }],
    });
    expect(posted.status).toBe(200);
    expect(posted.body).toEqual({ run_id: 'run-2', schedule_name: 'daily' });
    expect(sentRebase).toEqual([{
      request_id: REQUEST_ID, source_run_id: 'run-1', include_changed_upstream: true, expected_release_id: 'rel-7',
    }]);
  });

  it('the run and schedule routes send and read the field names the proto declares', async () => {
    // Every fake RPC runs its request and response through the real
    // serializers, so a misspelled field is dropped and an assertion sees the
    // default.
    const sent: Record<string, Record<string, unknown>> = {};
    const through = (rpc: string, req: any, res: Record<string, unknown>) => {
      const m = runMethod(rpc);
      sent[rpc] = m.requestDeserialize(m.requestSerialize(req));
      return m.responseDeserialize(m.responseSerialize(res));
    };
    const client = {
      getRun: (req: any, cb: any) => cb(null, through('GetRun', req, {
        run: { run_id: 'run-1', schedule_name: 'daily', status: 'running', promotion_seq: 7, created_at: { seconds: 1792836300, nanos: 0 } },
      })),
      triggerRerun: (req: any, _md: unknown, cb: any) => cb(null, through('TriggerRerun', req, { run_id: 'run-2', schedule_name: 'daily' })),
      cancelRun: (req: any, _md: unknown, cb: any) => cb(null, through('CancelRun', req, { run: { run_id: 'run-1', status: 'cancelling' } })),
      listTasks: (req: any, cb: any) => cb(null, through('ListTasks', req, {
        tasks: [{ task_id: 't1', run_id: 'run-1', status: 'pending', wait_reason: 'node_lease', waiting_on_run_id: 'run-9' }], total_count: 1,
      })),
      listRunAttempts: (req: any, cb: any) => cb(null, through('ListRunAttempts', req, { attempts: [{ task_id: 't1', attempt: 1, status: 'failed' }], total_count: 1 })),
      startRun: (req: any, _md: unknown, cb: any) => cb(null, through('StartRun', req, { run_id: 'run-3' })),
      listSchedules: (req: any, cb: any) => cb(null, through('ListSchedules', req, {
        schedules: [{ schedule_name: 'daily', has_active_run: true, last_run_status: 'running', last_run_id: 'run-1' }],
      })),
      getScheduleGraph: (req: any, cb: any) => cb(null, through('GetScheduleGraph', req, {
        nodes: [{ service_name: 's', schema_name: 'a', table_name: 'b', node_type: 'dbt-model', schedule_name: 'daily' }],
        edges: [{ from_node_id: 's.a.b', to_node_id: 's.a.c' }], promotion_seq: 4,
      })),
    };
    const app = express();
    app.use(express.json());
    app.use('/api/runs', createRunsRouter(client as any));
    app.use('/api/schedules', createSchedulesRouter(client as any));

    const run = await request(app).get('/api/runs/run-1');
    const rerun = await request(app).post('/api/runs/run-1/rerun').send({ request_id: REQUEST_ID });
    const cancel = await request(app).post('/api/runs/run-1/cancel').send({ cancellation_reason: 'manual' });
    const tasks = await request(app).get('/api/runs/run-1/tasks');
    const attempts = await request(app).get('/api/runs/run-1/attempts?task_id=t1');
    const trigger = await request(app).post('/api/schedules/daily/trigger').send({ request_id: REQUEST_ID, operation: 'test' });
    const schedules = await request(app).get('/api/schedules');
    const graph = await request(app).get('/api/schedules/daily/graph');

    expect(sent.GetRun).toEqual({ run_id: 'run-1' });
    expect(run.body).toMatchObject({ run_id: 'run-1', schedule_name: 'daily', status: 'running', promotion_seq: 7 });
    expect(sent.TriggerRerun).toEqual({ request_id: REQUEST_ID, source_run_id: 'run-1' });
    expect(rerun.body).toEqual({ run_id: 'run-2', schedule_name: 'daily' });
    expect(sent.CancelRun).toEqual({ run_id: 'run-1', cancelled_by: '', cancellation_reason: 'manual' });
    expect(cancel.body).toMatchObject({ run_id: 'run-1', status: 'cancelling' });
    expect(sent.ListTasks).toMatchObject({ run_id: 'run-1' });
    expect(tasks.body.tasks[0]).toMatchObject({ run_id: 'run-1', wait_reason: 'node_lease', waiting_on_run_id: 'run-9' });
    expect(sent.ListRunAttempts).toMatchObject({ run_id: 'run-1', task_id: 't1' });
    expect(attempts.body.attempts[0]).toMatchObject({ task_id: 't1', attempt: 1, status: 'failed' });
    expect(sent.StartRun).toEqual({ request_id: REQUEST_ID, schedule_name: 'daily', operation: 'test' });
    expect(trigger.body).toEqual({ run_id: 'run-3' });
    expect(schedules.body.schedules[0]).toMatchObject({ schedule_name: 'daily', has_active_run: true, last_run_id: 'run-1' });
    expect(sent.GetScheduleGraph).toEqual({ schedule_name: 'daily' });
    expect(graph.body.nodes[0]).toMatchObject({ node_id: 's.a.b', node_type: 'dbt-model', schedule_name: 'daily' });
    expect(graph.body.edges).toEqual([{ from_node_id: 's.a.c', to_node_id: 's.a.b' }]);
    expect(graph.body.promotion_seq).toBe(4);
  });

  it('UpsertSchedule request carries expected_version as an int64 at field 6, above 2^31 and at 0', () => {
    const method = runMethod('UpsertSchedule');
    const sent = {
      schedule_name: 'daily', cron: '0 1 * * *', timezone: 'Europe/Paris', description: 'Runs daily', paused: true,
    };
    // The wire form of the field: tag 0x30 (field 6, varint), then 2^31 as the
    // five-byte varint a 32-bit integer could not hold.
    expect([...method.requestSerialize({ schedule_name: 's', expected_version: 2 ** 31 })])
      .toEqual([0x0a, 1, 0x73, 0x30, 0x80, 0x80, 0x80, 0x80, 0x08]);
    // Zero, the default of a proto3 field, reads back as '0' whether it is on
    // the wire or left out of it.
    expect(method.requestDeserialize(Buffer.from([0x0a, 1, 0x73])).expected_version).toBe('0');
    expect(method.requestDeserialize(method.requestSerialize({ schedule_name: 's', expected_version: 0 })).expected_version)
      .toBe('0');

    // int64 is loaded as a string: the value comes back whole, not truncated.
    for (const version of [0, 3, 2 ** 31 - 1, 2 ** 31, 3_000_000_000, Number.MAX_SAFE_INTEGER]) {
      expect(requestRoundTrip('UpsertSchedule', { ...sent, expected_version: version }))
        .toEqual({ ...sent, expected_version: String(version) });
    }
    expect(requestRoundTrip('UpsertSchedule', sent)).toEqual({ ...sent, expected_version: '0' });
  });

  it('DeleteSchedule request round-trips schedule_name', () => {
    expect(requestRoundTrip('DeleteSchedule', { schedule_name: 'daily' })).toEqual({ schedule_name: 'daily' });
  });

  it('PreviewSchedule request carries cron as field 1 and timezone as field 2', () => {
    const method = runMethod('PreviewSchedule');
    expect([...method.requestSerialize({ cron: 'c', timezone: 'z' })]).toEqual([0x0a, 1, 0x63, 0x12, 1, 0x7a]);
    expect(requestRoundTrip('PreviewSchedule', { cron: '0 1 * * *', timezone: 'Europe/Paris' }))
      .toEqual({ cron: '0 1 * * *', timezone: 'Europe/Paris' });
  });

  it('the cron routes send and read the field names the proto declares', async () => {
    // Each fake RPC passes its request and its response through the real
    // serializers, so a misspelled field is dropped and an assertion sees the
    // default, and an int64 reaches the route as the string the wire gives.
    const sent: Record<string, Record<string, unknown>[]> = {};
    const through = (rpc: string, req: any, res: Record<string, unknown>) => {
      const m = runMethod(rpc);
      (sent[rpc] ??= []).push(m.requestDeserialize(m.requestSerialize(req)));
      return m.responseDeserialize(m.responseSerialize(res));
    };
    const at = (iso: string) => ({ seconds: Date.parse(iso) / 1000, nanos: 0 });
    const client = {
      previewSchedule: (req: any, cb: any) => cb(null, through('PreviewSchedule', req, {
        cron_words: 'every day at 01:00', next_fire_times: [at('2026-10-28T00:00:00Z')],
      })),
      upsertSchedule: (req: any, _md: unknown, cb: any) => cb(null, through('UpsertSchedule', req, {
        schedule: {
          schedule_name: 'daily', cron: '0 1 * * *', timezone: 'Europe/Paris', description: 'Runs daily', paused: false,
          next_fire_at: at('2026-10-28T00:00:00Z'), version: 3_000_000_001, updated_by: 'alice@example.com',
          updated_at: at('2026-10-27T12:02:00Z'),
        },
        next_fire_times: [at('2026-10-28T00:00:00Z')],
      })),
      deleteSchedule: (req: any, _md: unknown, cb: any) => cb(null, through('DeleteSchedule', req, {})),
    };
    const app = express();
    app.use(express.json());
    app.use('/api/schedules', createSchedulesRouter(client as any));

    const preview = await request(app).get('/api/schedules/cron-preview').query({ cron: '0 1 * * *', timezone: 'Europe/Paris' });
    const put = await request(app).put('/api/schedules/daily/cron').send({
      cron: '0 1 * * *', timezone: 'Europe/Paris', description: 'Runs daily', paused: true, expected_version: 3_000_000_000,
    });
    const zero = await request(app).put('/api/schedules/daily/cron').send({ cron: '@daily', timezone: 'UTC' });
    const del = await request(app).delete('/api/schedules/daily/cron');

    expect(sent.PreviewSchedule).toEqual([{ cron: '0 1 * * *', timezone: 'Europe/Paris' }]);
    expect(preview.body).toEqual({ cron_words: 'every day at 01:00', next_fire_times: ['2026-10-28T00:00:00.000Z'] });
    expect(sent.UpsertSchedule).toEqual([
      {
        schedule_name: 'daily', cron: '0 1 * * *', timezone: 'Europe/Paris', description: 'Runs daily', paused: true,
        expected_version: '3000000000',
      },
      { schedule_name: 'daily', cron: '@daily', timezone: 'UTC', description: '', paused: false, expected_version: '0' },
    ]);
    expect(put.status).toBe(200);
    expect(put.body.schedule).toEqual({
      schedule_name: 'daily', cron: '0 1 * * *', timezone: 'Europe/Paris', description: 'Runs daily', paused: false,
      next_fire_at: '2026-10-28T00:00:00.000Z', version: 3_000_000_001, updated_by: 'alice@example.com',
      updated_at: '2026-10-27T12:02:00.000Z',
    });
    expect(zero.status).toBe(200);
    expect(sent.DeleteSchedule).toEqual([{ schedule_name: 'daily' }]);
    expect(del.status).toBe(204);
  });

  it('ListRunAttempts request round-trips run_id, task_id and paging', () => {
    expect(requestRoundTrip('ListRunAttempts', {
      run_id: 'run-abc-123', task_id: 'task-7', page_size: 200, page_offset: 400,
    })).toEqual({ run_id: 'run-abc-123', task_id: 'task-7', page_size: 200, page_offset: 400 });
  });

  it("ListTasks response decodes a task's attempt from field 9", () => {
    const resp = decodeResponse('ListTasks', [0x0a, 0x02, 0x48, 0x02]);
    expect(resp.tasks).toHaveLength(1);
    expect(resp.tasks[0].attempt).toBe(2);
    expect(Object.keys(resp.tasks[0]).filter(k => k.startsWith('retry'))).toEqual([]);
  });

  it('ListNodeTasks response decodes attempt (field 7), failure_class (field 17) and failure_reason (field 18)', () => {
    const nodeTask = [
      0x38, 3, // attempt = 3
      0x8a, 0x01, 4, ...Buffer.from('user'), // failure_class = "user" (field 17, wire type 2)
      0x92, 0x01, 3, ...Buffer.from('oom'), // failure_reason = "oom" (field 18, wire type 2)
    ];
    const resp = decodeResponse('ListNodeTasks', [0x0a, nodeTask.length, ...nodeTask]);
    expect(resp.tasks).toHaveLength(1);
    expect(resp.tasks[0].attempt).toBe(3);
    expect(resp.tasks[0].failure_class).toBe('user');
    expect(resp.tasks[0].failure_reason).toBe('oom');
    expect(resp).not.toHaveProperty('runs');
  });

  it('ListRunAttempts response decodes every attempt', () => {
    const method = runMethod('ListRunAttempts');
    const attempt = {
      task_id: 'task-1', attempt: 2, job_name: 'core-sales-orders-2', status: 'failed',
      failure_class: 'user_code', failure_reason: 'compile_error', exit_code: 1,
      dispatched_at: { seconds: '100', nanos: 0 },
      queued_at: { seconds: '101', nanos: 0 },
      started_at: { seconds: '105', nanos: 0 },
      finished_at: { seconds: '125', nanos: 0 },
      execution_seconds: 19.5,
      error_message: 'boom',
      log_s3_key: 'logs/task-1-2.log',
      run_results_uri: 'run-results/task-1-2.json',
    };
    const second = { ...attempt, task_id: 'task-2', attempt: 0, status: 'succeeded' };
    const resp = method.responseDeserialize(method.responseSerialize({
      attempts: [attempt, second], total_count: 2,
    }));
    expect(resp.total_count).toBe(2);
    expect(resp.attempts).toEqual([attempt, second]);
  });

  it('RunService client constructor exists', () => {
    expect(Messages.RunService).toBeTypeOf('function');
  });
});
