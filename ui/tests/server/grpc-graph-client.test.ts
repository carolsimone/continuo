import { describe, it, expect } from 'vitest';
import { createGrpcGraphClient } from '../../src/server/clients/grpc-graph-client';

describe('createGrpcGraphClient', () => {
  it('loads the vendored catalog proto and exposes the node reads', () => {
    const client = createGrpcGraphClient('localhost:50052');
    expect(typeof client.getNode).toBe('function');
    expect(typeof client.getNodeVersions).toBe('function');
  });

  it('exposes the three data contract reads of the vendored catalog proto', () => {
    const client = createGrpcGraphClient('localhost:50052');
    expect(typeof client.getNodeDataContract).toBe('function');
    expect(typeof client.listNodeDataContractVersions).toBe('function');
    expect(typeof client.getNodeDataContractDiff).toBe('function');
  });
});
