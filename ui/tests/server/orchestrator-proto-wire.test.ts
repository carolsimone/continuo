import { describe, it, expect } from 'vitest';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

const PROTO_DIR = path.join(process.cwd(), 'proto');
const PROTO_PATH = path.join(PROTO_DIR, 'orchestrator/v1/orchestrator.proto');

// The loader options of src/server/grpc-graph-client.ts, so the decoded object
// has exactly the keys the ui server reads.
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [PROTO_DIR],
});

// decodeResponse runs the production response deserializer of one
// OrchestratorQuery RPC over raw wire bytes: what an orchestrator sends, whatever
// its own copy of the proto names the fields.
function decodeResponse(rpcName: string, bytes: number[]): Record<string, any> {
  const svc = (packageDefinition as Record<string, any>)['orchestrator.v1.OrchestratorQuery'];
  const method = svc[rpcName];
  expect(method, `missing rpc method: ${rpcName}`).toBeTruthy();
  return method.responseDeserialize(Buffer.from(bytes)) as Record<string, any>;
}

describe('proto wire: orchestrator.proto promotion seq', () => {
  it('GetScheduleGraph decodes field 3 as promotion_seq', () => {
    const resp = decodeResponse('GetScheduleGraph', [0x18, 42]);
    expect(resp.promotion_seq).toBe('42');
    expect(resp).not.toHaveProperty('topology_generation');
  });
});
