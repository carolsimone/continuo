import { describe, it, expect } from 'vitest';
import * as protoLoader from '@grpc/proto-loader';
import * as grpc from '@grpc/grpc-js';
import path from 'path';

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

  it('StateService client constructor exists', () => {
    expect(Messages.StateService).toBeTypeOf('function');
  });
});
