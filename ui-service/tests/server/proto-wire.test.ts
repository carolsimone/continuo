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

  it('StateService client constructor exists', () => {
    expect(Messages.StateService).toBeTypeOf('function');
  });
});
