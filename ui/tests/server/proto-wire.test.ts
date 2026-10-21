import { describe, it, expect } from 'vitest';
import * as protoLoader from '@grpc/proto-loader';
import * as grpc from '@grpc/grpc-js';
import path from 'path';
import express from 'express';
import request from 'supertest';
import { createSchedulersRouter } from '../../src/server/routes/schedulers';

const PROTO_DIR = path.join(process.cwd(), 'proto');
const PROTO_PATH = path.join(PROTO_DIR, 'state.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [PROTO_DIR],
});

const grpcObject = grpc.loadPackageDefinition(packageDefinition) as any;
const Messages = grpcObject.state.v1;

// requestRoundTrip drives the production RPC's request serializer/deserializer
// pair — the same code path @grpc/grpc-js uses to encode arguments to a unary
// call. Encoding then decoding the request reveals which fields cross the wire
// vs. get silently dropped by a stale proto.
function requestRoundTrip(rpcName: string, request: Record<string, unknown>): Record<string, unknown> {
  const svc = (packageDefinition as Record<string, any>)['state.v1.StateService'];
  const method = svc[rpcName];
  expect(method, `missing rpc method: ${rpcName}`).toBeTruthy();
  const buf: Buffer = method.requestSerialize(request);
  return method.requestDeserialize(buf) as Record<string, unknown>;
}

// stateMethod returns the production serializer/deserializer pairs of one
// StateService RPC.
function stateMethod(rpcName: string): Record<string, any> {
  const method = (packageDefinition as Record<string, any>)['state.v1.StateService'][rpcName];
  expect(method, `missing rpc method: ${rpcName}`).toBeTruthy();
  return method;
}

// decodeResponse runs the production response deserializer of one
// StateService RPC over raw wire bytes.
function decodeResponse(rpcName: string, bytes: number[]): Record<string, any> {
  const svc = (packageDefinition as Record<string, any>)['state.v1.StateService'];
  const method = svc[rpcName];
  expect(method, `missing rpc method: ${rpcName}`).toBeTruthy();
  return method.responseDeserialize(Buffer.from(bytes)) as Record<string, any>;
}

describe('proto wire: state.proto', () => {
  it('TriggerRerun request round-trips source_run_id', () => {
    const result = requestRoundTrip('TriggerRerun', {
      source_run_id: 'run-abc-123',
    });
    expect(result.source_run_id).toBe('run-abc-123');
    expect(Object.keys(result)).toContain('source_run_id');
    expect(Object.keys(result)).not.toContain('schedule_id');
    expect(Object.keys(result)).not.toContain('table_name');
  });

  it('TriggerSchedule request round-trips schedule_name', () => {
    const result = requestRoundTrip('TriggerSchedule', {
      schedule_name: 'seed',
    });
    expect(result.schedule_name).toBe('seed');
  });

  it('CancelSchedule request round-trips schedule_name + cancelled_by + cancellation_reason', () => {
    const result = requestRoundTrip('CancelSchedule', {
      schedule_name: 'e2e-schedule',
      cancelled_by: 'tester',
      cancellation_reason: 'aborted by hand',
    });
    expect(result.schedule_name).toBe('e2e-schedule');
    expect(result.cancelled_by).toBe('tester');
    expect(result.cancellation_reason).toBe('aborted by hand');
  });

  it('GetRunGraph decodes fields 3 and 4 as run_promotion_seq and latest_promotion_seq', () => {
    const resp = decodeResponse('GetRunGraph', [0x18, 5, 0x20, 7]);
    expect(resp.run_promotion_seq).toBe('5');
    expect(resp.latest_promotion_seq).toBe('7');
  });

  it('ListActiveRunDrifts decodes field 1 and ActiveRunDrift field 3 as promotion seqs', () => {
    const resp = decodeResponse('ListActiveRunDrifts', [0x08, 9, 0x12, 2, 0x18, 4]);
    expect(resp.latest_promotion_seq).toBe('9');
    expect(resp.active_runs).toHaveLength(1);
    expect(resp.active_runs[0].run_promotion_seq).toBe('4');
  });

  it('TriggerRebase request carries include_changed_upstream as field 2 and expected_release_id as field 3', () => {
    const method = stateMethod('TriggerRebase');
    const bytes = [...method.requestSerialize({
      source_run_id: 'r', include_changed_upstream: true, expected_release_id: 'x',
    })];
    expect(bytes).toEqual([0x0a, 1, 0x72, 0x10, 1, 0x1a, 1, 0x78]);
    expect(requestRoundTrip('TriggerRebase', {
      source_run_id: 'run-abc-123', include_changed_upstream: true, expected_release_id: 'rel-7',
    })).toEqual({ source_run_id: 'run-abc-123', include_changed_upstream: true, expected_release_id: 'rel-7' });
  });

  it('PreviewRebase request carries source_run_id as field 1 and include_changed_upstream as field 2', () => {
    const method = stateMethod('PreviewRebase');
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
    const preview = stateMethod('PreviewRebase');
    const rebase = stateMethod('TriggerRebase');
    const client = {
      triggerRebase: (req: any, _md: unknown, cb: (err: unknown, res: unknown) => void) => {
        sentRebase.push(rebase.requestDeserialize(rebase.requestSerialize(req)));
        cb(null, {});
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
    app.use('/api/schedulers', createSchedulersRouter(client as any));

    const got = await request(app).get('/api/schedulers/run-1/rebase-preview?include_changed_upstream=true');
    const posted = await request(app).post('/api/schedulers/run-1/rebase')
      .send({ include_changed_upstream: true, expected_release_id: 'rel-7' });

    expect(sentPreview).toEqual([{ source_run_id: 'run-1', include_changed_upstream: true }]);
    expect(got.body).toEqual({
      release_id: 'rel-7', promotion_seq: 7, total_nodes: 2000, will_run: 41,
      failed_or_skipped: 3, new_nodes: 1, changed_upstream: 2, downstream: 35, inherited: 1959,
      changed_nodes: [{ service_name: 'core', schema_name: 'stg', table_name: 'orders', downstream: 18, last_success_seconds: 252.5 }],
    });
    expect(posted.status).toBe(200);
    expect(sentRebase).toEqual([{ source_run_id: 'run-1', include_changed_upstream: true, expected_release_id: 'rel-7' }]);
  });

  it('ListTaskAttempts request round-trips run_id, task_id and paging', () => {
    expect(requestRoundTrip('ListTaskAttempts', {
      run_id: 'run-abc-123', task_id: 'task-7', page_size: 200, page_offset: 400,
    })).toEqual({ run_id: 'run-abc-123', task_id: 'task-7', page_size: 200, page_offset: 400 });
  });

  it("ListTasks response decodes a task's attempt from field 9", () => {
    const resp = decodeResponse('ListTasks', [0x0a, 0x02, 0x48, 0x02]);
    expect(resp.tasks).toHaveLength(1);
    expect(resp.tasks[0].attempt).toBe(2);
    expect(Object.keys(resp.tasks[0]).filter(k => k.startsWith('retry'))).toEqual([]);
  });

  it('ListNodeRuns response decodes attempt (field 7) and failure_class (field 18)', () => {
    const nodeRun = [
      0x38, 3, // attempt = 3
      0x92, 0x01, 4, ...Buffer.from('user'), // failure_class = "user" (field 18, wire type 2)
      0x9a, 0x01, 3, ...Buffer.from('oom'), // failure_reason = "oom" (field 19, wire type 2)
    ];
    const resp = decodeResponse('ListNodeRuns', [0x0a, nodeRun.length, ...nodeRun]);
    expect(resp.runs).toHaveLength(1);
    expect(resp.runs[0].attempt).toBe(3);
    expect(resp.runs[0].failure_class).toBe('user');
    expect(resp.runs[0].failure_reason).toBe('oom');
  });

  it('ListTaskAttempts response decodes every attempt', () => {
    const method = stateMethod('ListTaskAttempts');
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

  it('StateService client constructor exists', () => {
    expect(Messages.StateService).toBeTypeOf('function');
  });
});
