import { describe, it, expect } from 'vitest';
import { createGrpcGraphClient } from '../../src/server/clients/grpc-graph-client';

describe('createGrpcGraphClient', () => {
  it('loads the vendored catalog proto and exposes the node reads', () => {
    const client = createGrpcGraphClient('localhost:50052');
    expect(typeof client.getNode).toBe('function');
    expect(typeof client.getNodeVersions).toBe('function');
  });

  it('exposes the three contract reads of the vendored catalog proto', () => {
    const client = createGrpcGraphClient('localhost:50052');
    expect(typeof client.getNodeContract).toBe('function');
    expect(typeof client.listNodeContractVersions).toBe('function');
    expect(typeof client.getNodeContractDiff).toBe('function');
  });
});
