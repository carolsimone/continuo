import { describe, it, expect } from 'vitest';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';
import { createRemediationClient } from '../../src/server/clients/remediation-client';

describe('createRemediationClient', () => {
  it('constructs a client with all five promise-returning methods', () => {
    const client = createRemediationClient('localhost:50054');
    expect(typeof client.listFixes).toBe('function');
    expect(typeof client.getFix).toBe('function');
    expect(typeof client.beginPullRequest).toBe('function');
    expect(typeof client.recordPullRequest).toBe('function');
    expect(typeof client.failPullRequest).toBe('function');
  });
});

// The ui loads remediation.proto at runtime with the options the client uses,
// so the field set of its copy decides which fields of a fix reach the
// browser: a field the copy lacks is dropped on decode.
describe('proto wire: remediation.proto', () => {
  const PROTO_DIR = path.join(process.cwd(), 'proto');
  const packageDefinition = protoLoader.loadSync(path.join(PROTO_DIR, 'remediation/v1/remediation.proto'), {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [PROTO_DIR],
  });

  // getFixRoundTrip drives the production GetFix response
  // serializer/deserializer pair, the code path @grpc/grpc-js uses to decode
  // the server's reply.
  function getFixRoundTrip(fix: Record<string, unknown>): Record<string, any> {
    const method = (packageDefinition as Record<string, any>)['remediation.v1.RemediationFixes'].GetFix;
    expect(method, 'missing rpc method: GetFix').toBeTruthy();
    const buf: Buffer = method.responseSerialize(fix);
    return method.responseDeserialize(buf) as Record<string, any>;
  }

  it('GetFix carries the reason code of each node outcome beside its status and reason', () => {
    const result = getFixRoundTrip({
      id: 'p1',
      node_outcomes: {
        's.handed': { status: 'escalated', reason: 'no type matches', code: 'no_type' },
        's.fixed': { status: 'proposed', reason: '' },
      },
    });
    expect(result.node_outcomes['s.handed']).toEqual({ status: 'escalated', reason: 'no type matches', code: 'no_type' });
    expect(result.node_outcomes['s.fixed']).toEqual({ status: 'proposed', reason: '', code: '' });
  });

  it('GetFix carries who must approve each node', () => {
    const result = getFixRoundTrip({
      id: 'p1',
      approvers: [
        { node_id: 's.a', owner: 'finance', source: 'bundle' },
        { node_id: 's.b', owner: '', source: 'none' },
      ],
    });
    expect(result.approvers).toEqual([
      { node_id: 's.a', owner: 'finance', source: 'bundle' },
      { node_id: 's.b', owner: '', source: 'none' },
    ]);
  });

  it('BeginPullRequest carries the approvers of the claim nodes', () => {
    const method = (packageDefinition as Record<string, any>)['remediation.v1.RemediationFixes'].BeginPullRequest;
    expect(method, 'missing rpc method: BeginPullRequest').toBeTruthy();
    const buf: Buffer = method.responseSerialize({
      fix_id: 'p1',
      resolved_node_ids: ['s.a'],
      approvers: [{ node_id: 's.a', owner: 'finance', source: 'contract_file' }],
    });
    const result = method.responseDeserialize(buf) as Record<string, any>;
    expect(result.approvers).toEqual([{ node_id: 's.a', owner: 'finance', source: 'contract_file' }]);
  });
});
