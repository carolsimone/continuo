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

describe('proto wire: orchestrator.proto promotion seqs', () => {
  it('GetScheduleGraph decodes field 3 as promotion_seq', () => {
    const resp = decodeResponse('GetScheduleGraph', [0x18, 42]);
    expect(resp.promotion_seq).toBe('42');
    expect(resp).not.toHaveProperty('topology_generation');
  });

  it('GetRunGraph decodes fields 3 and 4 as run_promotion_seq and latest_promotion_seq', () => {
    const resp = decodeResponse('GetRunGraph', [0x18, 5, 0x20, 7]);
    expect(resp.run_promotion_seq).toBe('5');
    expect(resp.latest_promotion_seq).toBe('7');
  });

  it('ListActiveRunDrifts decodes field 1 and ActiveRunDrift field 3 as promotion seqs', () => {
    // latest_promotion_seq = 9; active_runs = [{ run_promotion_seq: 4 }]
    const resp = decodeResponse('ListActiveRunDrifts', [0x08, 9, 0x12, 2, 0x18, 4]);
    expect(resp.latest_promotion_seq).toBe('9');
    expect(resp.active_runs).toHaveLength(1);
    expect(resp.active_runs[0].run_promotion_seq).toBe('4');
  });
});
