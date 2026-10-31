import { describe, it, expect } from 'vitest';
import {
  resolveActiveGraph, headerDriftLabel, taskStatusTitle, runFailureLabel,
  graphBadgeLabel, graphBadgeClass, graphCardState, runTabState, driftLabelOf,
  formatStatusLabel, formatDate,
} from './detail-page-helpers';
import type { RunGraph, RunSummary, ScheduleGraph } from './types';

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
    expect(headerDriftLabel({ mode: 'latest', driftLabel: 'source 1 release behind latest' })).toBeNull();
  });

  it('returns the drift label in run mode', () => {
    expect(headerDriftLabel({ mode: 'run', driftLabel: 'source 1 release behind latest' }))
      .toBe('source 1 release behind latest');
  });

  it('returns null when drift is null regardless of mode', () => {
    expect(headerDriftLabel({ mode: 'run', driftLabel: null })).toBeNull();
    expect(headerDriftLabel({ mode: 'latest', driftLabel: null })).toBeNull();
  });

  it('treats undefined mode as run (backwards compatible)', () => {
    expect(headerDriftLabel({ driftLabel: 'source 2 releases behind latest' } as any))
      .toBe('source 2 releases behind latest');
  });
});

describe('taskStatusTitle', () => {
  it('says a queued task waits for a slot and titles no other status', () => {
    expect(taskStatusTitle('queued')).toBe('waiting for a slot');
    for (const s of ['dispatched', 'running', 'pending', 'failed']) expect(taskStatusTitle(s)).toBeUndefined();
  });
});

describe('runFailureLabel', () => {
  it('names why a run failed without a failed task, and nothing otherwise', () => {
    expect(runFailureLabel('stalled')).toBe('failed: stalled');
    expect(runFailureLabel('empty_selection')).toBe('failed: empty selection');
    expect(runFailureLabel(null)).toBeNull();
    expect(runFailureLabel('')).toBeNull();
  });
});

describe('graphBadgeLabel', () => {
  const run: RunSummary = {
    run_id: 'r', schedule_name: 's', status: 'failed', created_at: null, completed_at: null,
  } as RunSummary;
  const graph: ScheduleGraph = { nodes: [{ node_id: 'n', node_type: 'model', schedule_name: 's' }], edges: [] };
  const emptyGraph: ScheduleGraph = { nodes: [], edges: [] };

  it('is "catalog" in latest mode whatever else is true', () => {
    expect(graphBadgeLabel({ mode: 'latest', selectedRun: run, liveRunExists: true, activeGraph: null })).toBe('catalog');
  });

  it('is "snapshot" for a selected run, before a live run', () => {
    expect(graphBadgeLabel({ mode: 'run', selectedRun: run, liveRunExists: true, activeGraph: graph })).toBe('snapshot');
  });

  it('is "live" while a run is active', () => {
    expect(graphBadgeLabel({ mode: 'run', selectedRun: null, liveRunExists: true, activeGraph: null })).toBe('live');
  });

  it('is "catalog" for a non-empty graph with no live run', () => {
    expect(graphBadgeLabel({ mode: 'run', selectedRun: null, liveRunExists: false, activeGraph: graph })).toBe('catalog');
  });

  it('is "idle" with no graph or an empty one', () => {
    expect(graphBadgeLabel({ mode: 'run', selectedRun: null, liveRunExists: false, activeGraph: null })).toBe('idle');
    expect(graphBadgeLabel({ mode: 'run', selectedRun: null, liveRunExists: false, activeGraph: emptyGraph })).toBe('idle');
  });
});

describe('graphCardState', () => {
  const runGraph = { nodes: [], edges: [] } as unknown as RunGraph;

  it('is "loading" for a selected run whose graph has not arrived', () => {
    expect(graphCardState({ selectedRunId: 'r', runGraph: null, shouldRenderGraph: false, graphState: 'ready' })).toBe('loading');
    expect(graphCardState({ selectedRunId: 'r', runGraph: null, shouldRenderGraph: true, graphState: 'ready' })).toBe('loading');
  });

  it('is "ready" or "empty" for a selected run whose graph arrived', () => {
    expect(graphCardState({ selectedRunId: 'r', runGraph, shouldRenderGraph: true, graphState: 'error' })).toBe('ready');
    expect(graphCardState({ selectedRunId: 'r', runGraph, shouldRenderGraph: false, graphState: 'ready' })).toBe('empty');
  });

  it('is "ready" with no selected run when there is a graph to draw', () => {
    expect(graphCardState({ selectedRunId: null, runGraph: null, shouldRenderGraph: true, graphState: 'error' })).toBe('ready');
  });

  it('passes the schedule graph state through when nothing is drawn', () => {
    for (const graphState of ['loading', 'ready', 'empty', 'error'] as const) {
      expect(graphCardState({ selectedRunId: null, runGraph: null, shouldRenderGraph: false, graphState })).toBe(graphState);
    }
  });
});

describe('runTabState', () => {
  const runGraph = { nodes: [], edges: [] } as unknown as RunGraph;
  const base = { lastRunId: 'r' as string | null | undefined, runsCount: 0, taskCount: 1, selectedRunId: null as string | null, runGraph: null as RunGraph | null };

  it('is "no-runs" for a schedule that never ran and has no past runs or tasks', () => {
    expect(runTabState({ ...base, lastRunId: null, taskCount: 0 })).toBe('no-runs');
  });

  it('is not "no-runs" once a past run or a task exists', () => {
    expect(runTabState({ ...base, lastRunId: null, runsCount: 2, taskCount: 0 })).toBe('loading');
    expect(runTabState({ ...base, lastRunId: null, taskCount: 1 })).toBe('ready');
  });

  it('is "loading" while the last run is unresolved', () => {
    expect(runTabState({ ...base, lastRunId: undefined })).toBe('loading');
  });

  it('is "loading" while a selected run\'s graph has not arrived, even with tasks', () => {
    expect(runTabState({ ...base, selectedRunId: 'old', runGraph: null })).toBe('loading');
  });

  it('is "loading" while the tasks have not arrived', () => {
    expect(runTabState({ ...base, taskCount: 0 })).toBe('loading');
  });

  it('is "ready" with tasks and, for a selected run, its graph', () => {
    expect(runTabState(base)).toBe('ready');
    expect(runTabState({ ...base, selectedRunId: 'old', runGraph })).toBe('ready');
  });

  it('ignores the last run id when a run is selected', () => {
    expect(runTabState({ ...base, lastRunId: undefined, selectedRunId: 'old', runGraph })).toBe('ready');
  });
});

describe('driftLabelOf', () => {
  const graphAt = (run: number | null | undefined, latest: number) =>
    ({ nodes: [], edges: [], run_promotion_seq: run, latest_promotion_seq: latest }) as unknown as RunGraph;

  it('is null without a graph, for a fresh run and for a run without a promotion seq', () => {
    expect(driftLabelOf(null)).toBeNull();
    expect(driftLabelOf(graphAt(7, 7))).toBeNull();
    expect(driftLabelOf(graphAt(0, 7))).toBeNull();
    expect(driftLabelOf(graphAt(undefined, 7))).toBeNull();
  });

  it('words how many releases a stale run is behind', () => {
    expect(driftLabelOf(graphAt(6, 7))).toBe('source 1 release behind latest');
    expect(driftLabelOf(graphAt(4, 7))).toBe('source 3 releases behind latest');
  });
});

describe('graphBadgeClass', () => {
  it('colours a snapshot badge by the snapshot run\'s status', () => {
    expect(graphBadgeClass('snapshot', 'failed')).toBe('pill-sm pill-sm--failed');
    expect(graphBadgeClass('snapshot', 'succeeded')).toBe('pill-sm pill-sm--succeeded');
    expect(graphBadgeClass('snapshot', null)).toBe('pill-sm pill-sm--pending');
  });

  it('marks a live badge as current and every other badge as pending', () => {
    expect(graphBadgeClass('live', null)).toBe('pill-sm pill-sm--current');
    expect(graphBadgeClass('catalog', null)).toBe('pill-sm pill-sm--pending');
    expect(graphBadgeClass('idle', null)).toBe('pill-sm pill-sm--pending');
  });
});

describe('formatStatusLabel', () => {
  it('reads an empty status as pending and spaces underscores', () => {
    expect(formatStatusLabel(null)).toBe('pending');
    expect(formatStatusLabel('')).toBe('pending');
    expect(formatStatusLabel('never run')).toBe('never run');
    expect(formatStatusLabel('in_progress')).toBe('in progress');
  });
});

describe('formatDate', () => {
  it('shows a dash for no date, the text of an unparsable one, and a local date otherwise', () => {
    expect(formatDate(null)).toBe('—');
    expect(formatDate('not a date')).toBe('not a date');
    expect(formatDate('2026-09-01T10:00:00Z')).toBe(new Date('2026-09-01T10:00:00Z').toLocaleString());
  });
});
