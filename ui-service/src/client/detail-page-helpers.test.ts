import { describe, it, expect } from 'vitest';
import { resolveActiveGraph, headerDriftLabel } from './detail-page-helpers';

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

describe('headerDriftLabel', () => {
  it('returns null in latest mode even when drift is non-null', () => {
    expect(headerDriftLabel({ mode: 'latest', driftLabel: 'source 1 gen behind latest' })).toBeNull();
  });

  it('returns the drift label in run mode', () => {
    expect(headerDriftLabel({ mode: 'run', driftLabel: 'source 1 gen behind latest' }))
      .toBe('source 1 gen behind latest');
  });

  it('returns null when drift is null regardless of mode', () => {
    expect(headerDriftLabel({ mode: 'run', driftLabel: null })).toBeNull();
    expect(headerDriftLabel({ mode: 'latest', driftLabel: null })).toBeNull();
  });

  it('treats undefined mode as run (backwards compatible)', () => {
    expect(headerDriftLabel({ driftLabel: 'source 2 gen behind latest' } as any))
      .toBe('source 2 gen behind latest');
  });
});
