import { describe, it, expect } from 'vitest';
import { createRemediationClient } from '../../src/server/remediation-client';

describe('createRemediationClient', () => {
  it('constructs a client with all five promise-returning methods', () => {
    const client = createRemediationClient('localhost:50054');
    expect(typeof client.listProposals).toBe('function');
    expect(typeof client.getProposal).toBe('function');
    expect(typeof client.beginPullRequest).toBe('function');
    expect(typeof client.recordPullRequest).toBe('function');
    expect(typeof client.failPullRequest).toBe('function');
  });
});
