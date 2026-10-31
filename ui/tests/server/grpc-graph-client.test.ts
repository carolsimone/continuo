import { describe, it, expect } from 'vitest';
import { createGrpcGraphClient } from '../../src/server/clients/grpc-graph-client';

describe('createGrpcGraphClient', () => {
  it('loads the vendored catalog proto and exposes the node reads', () => {
    const client = createGrpcGraphClient('localhost:50052');
    expect(typeof client.getNode).toBe('function');
    expect(typeof client.getNodeVersions).toBe('function');
  });
});
