import { describe, it, expect } from 'vitest';
import { resolveActiveGraph } from './detail-page-helpers';

describe('resolveActiveGraph mode=latest', () => {
  const scheduleGraph = { nodes: [{ node_id: 't', node_type: 'model', schedule_name: 's' }], edges: [] };
  const liveRunGraph  = { nodes: [{ node_id: 'r', node_type: 'model', schedule_name: 's', status: 'running' }], edges: [] };
  const selectedRunGraph = { nodes: [{ node_id: 'h', node_type: 'model', schedule_name: 's', status: 'succeeded' }], edges: [] };

  it('returns scheduleGraph regardless of run inputs when mode is latest', () => {
    const r = resolveActiveGraph({ mode: 'latest', scheduleGraph, liveRunGraph, selectedRunGraph, selectedRunId: 'abc' });
    expect(r).toBe(scheduleGraph);
  });

  it('falls back to existing behaviour when mode is run', () => {
    const r = resolveActiveGraph({ mode: 'run', scheduleGraph, liveRunGraph, selectedRunGraph: null, selectedRunId: null });
    expect(r?.nodes[0].node_id).toBe('r');
  });

  it('treats undefined mode as run (backwards compatible)', () => {
    const r = resolveActiveGraph({ scheduleGraph, liveRunGraph: null, selectedRunGraph: null, selectedRunId: null } as any);
    expect(r).toBe(scheduleGraph);
  });
});
