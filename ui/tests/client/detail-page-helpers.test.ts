import { describe, expect, it } from 'vitest';
import {
  attemptClassReason,
  attemptPillClass,
  attemptWaitSeconds,
  formatAttemptSeconds,
  groupAttemptsByTask,
  headerDriftLabel,
  parseNodeId,
  resolveActiveGraph,
  resolveNodeStatus,
  taskPillClass,
  taskStatusTitle,
  toScheduleGraph,
} from '../../src/client/detail-page-helpers';
import { GraphNode, RunGraph, ScheduleGraph, Task, TaskAttempt } from '../../src/client/types';

describe('detail page graph helpers', () => {
  it('converts a run graph into a schedule graph payload', () => {
    const runGraph: RunGraph = {
      nodes: [
        {
          node_id: 'svc.analytics.orders',
          node_type: 'dbt-model',
          schedule_name: 'daily',
          status: 'running',
        },
      ],
      edges: [{ from_node_id: 'svc.raw.seed_orders', to_node_id: 'svc.analytics.orders' }],
    };

    expect(toScheduleGraph(runGraph)).toEqual({
      nodes: [
        {
          node_id: 'svc.analytics.orders',
          node_type: 'dbt-model',
          schedule_name: 'daily',
          status: 'running',
        },
      ],
      edges: [{ from_node_id: 'svc.raw.seed_orders', to_node_id: 'svc.analytics.orders' }],
    });
  });

  it('prefers the live run graph over an empty schedule graph for the current run', () => {
    const scheduleGraph: ScheduleGraph = { nodes: [], edges: [] };
    const liveRunGraph: RunGraph = {
      nodes: [
        {
          node_id: 'svc.analytics.orders',
          node_type: 'dbt-model',
          schedule_name: 'daily',
          status: 'running',
        },
      ],
      edges: [],
    };

    expect(
      resolveActiveGraph({
        scheduleGraph,
        liveRunGraph,
        selectedRunGraph: null,
        selectedRunId: null,
      }),
    ).toEqual({
      nodes: [
        {
          node_id: 'svc.analytics.orders',
          node_type: 'dbt-model',
          schedule_name: 'daily',
          status: 'running',
        },
      ],
      edges: [],
    });
  });

  it('uses the selected historical run graph when a past run is selected', () => {
    const selectedRunGraph: RunGraph = {
      nodes: [
        {
          node_id: 'svc.analytics.orders',
          node_type: 'dbt-model',
          schedule_name: 'daily',
          status: 'succeeded',
        },
      ],
      edges: [],
    };

    expect(
      resolveActiveGraph({
        scheduleGraph: null,
        liveRunGraph: null,
        selectedRunGraph,
        selectedRunId: 'run-123',
      }),
    ).toEqual({
      nodes: [
        {
          node_id: 'svc.analytics.orders',
          node_type: 'dbt-model',
          schedule_name: 'daily',
          status: 'succeeded',
        },
      ],
      edges: [],
    });
  });

  it('prefers live task status and falls back to graph node status', () => {
    const graphNode: GraphNode = {
      node_id: 'svc.analytics.orders',
      node_type: 'dbt-model',
      schedule_name: 'daily',
      status: 'succeeded',
    };
    const matchingTask: Task = {
      task_id: 'task-1',
      service_name: 'svc',
      schema_name: 'analytics',
      table_name: 'orders',
      job_name: '',
      status: 'running',
      attempt: 0,
      max_retries: 0,
      created_at: null,
    };

    expect(resolveNodeStatus(graphNode, [matchingTask])).toBe('running');
    expect(resolveNodeStatus(graphNode, [])).toBe('succeeded');
  });

  it('parses a node_id into service, schema, and table', () => {
    expect(parseNodeId('svc.analytics.orders')).toEqual({
      service_name: 'svc',
      schema_name: 'analytics',
      table_name: 'orders',
    });
  });

  it('returns empty strings for missing node_id segments', () => {
    expect(parseNodeId('only')).toEqual({
      service_name: 'only',
      schema_name: '',
      table_name: '',
    });
  });

  it('hides the header drift chip in latest mode regardless of drift', () => {
    expect(headerDriftLabel({ mode: 'latest', driftLabel: 'source 1 release behind latest' })).toBeNull();
  });

  it('shows the header drift chip in run mode', () => {
    expect(headerDriftLabel({ mode: 'run', driftLabel: 'source 1 release behind latest' }))
      .toBe('source 1 release behind latest');
  });

  it('returns null when there is no drift to display', () => {
    expect(headerDriftLabel({ mode: 'run', driftLabel: null })).toBeNull();
    expect(headerDriftLabel({ mode: 'latest', driftLabel: null })).toBeNull();
  });
});

function attempt(task_id: string, n: number, overrides: Partial<TaskAttempt> = {}): TaskAttempt {
  return {
    task_id, attempt: n, job_name: `${task_id}-${n}`, status: 'succeeded',
    failure_class: null, failure_reason: null,
    dispatched_at: null, queued_at: null, started_at: null, finished_at: null,
    execution_seconds: null, error_message: null, log_s3_key: null, run_results_uri: null,
    ...overrides,
  };
}

describe('task attempt helpers', () => {
  it('groups attempts per task in attempt order', () => {
    const grouped = groupAttemptsByTask([
      attempt('b', 0), attempt('a', 1), attempt('a', 0), attempt('a', 2),
    ]);
    expect([...grouped.keys()].sort()).toEqual(['a', 'b']);
    expect(grouped.get('a')!.map(a => a.attempt)).toEqual([0, 1, 2]);
    expect(grouped.get('b')!.map(a => a.attempt)).toEqual([0]);
  });

  it('keeps one row per (task, attempt) when a page boundary repeats a row', () => {
    // Offset paging over a set that grows between two page reads shifts every
    // later row down by one, so the last row of one page comes back as the
    // first row of the next. The later copy is the fresher read and wins.
    const grouped = groupAttemptsByTask([
      attempt('a', 0, { status: 'running' }),
      attempt('a', 0, { status: 'failed' }),
      attempt('a', 1, { status: 'running' }),
    ]);
    expect(grouped.get('a')!.map(a => [a.attempt, a.status])).toEqual([[0, 'failed'], [1, 'running']]);
  });

  it('gives dispatched and queued attempts the pending pill and every other status its own', () => {
    expect(attemptPillClass('dispatched')).toBe('pill-sm--pending');
    expect(attemptPillClass('queued')).toBe('pill-sm--pending');
    expect(attemptPillClass('running')).toBe('pill-sm--running');
    expect(attemptPillClass('succeeded')).toBe('pill-sm--succeeded');
    expect(attemptPillClass('failed')).toBe('pill-sm--failed');
    expect(attemptPillClass('cancelled')).toBe('pill-sm--cancelled');
  });

  it('gives a stopped attempt its own pill, not the pending default', () => {
    expect(attemptPillClass('stopped')).toBe('pill-sm--stopped');
  });

  it('gives a cancelling run and a stopping task their own pill classes', () => {
    expect(taskPillClass('cancelling')).toBe('pill-sm--cancelling');
    expect(taskPillClass('stopping')).toBe('pill-sm--stopping');
    expect(taskPillClass('cancelled')).toBe('pill-sm--cancelled');
    expect(taskPillClass('skipped')).toBe('pill-sm--skipped');
    expect(taskPillClass('dispatched')).toBe('pill-sm--dispatched');
    expect(taskPillClass('something-new')).toBe('pill-sm--pending');
  });

  it('titles a cancelling run and a stopping task by what they wait for', () => {
    expect(taskStatusTitle('cancelling')).toBe('cancelling — waiting for its Jobs to stop');
    expect(taskStatusTitle('stopping')).toBe('stopping — its Job is being deleted');
  });

  it('joins class and reason, shows either alone, and a dash when both are absent', () => {
    expect(attemptClassReason(attempt('a', 0, { failure_class: 'user_code', failure_reason: 'compile_error' })))
      .toBe('user_code · compile_error');
    expect(attemptClassReason(attempt('a', 0, { failure_class: 'infra' }))).toBe('infra');
    expect(attemptClassReason(attempt('a', 0, { failure_reason: 'oom' }))).toBe('oom');
    expect(attemptClassReason(attempt('a', 0))).toBe('—');
  });

  it('measures the wait from queued to started, null when either is missing', () => {
    expect(attemptWaitSeconds(attempt('a', 0, {
      queued_at: '2026-01-01T10:00:00Z', started_at: '2026-01-01T10:01:05Z',
    }))).toBe(65);
    expect(attemptWaitSeconds(attempt('a', 0, { queued_at: '2026-01-01T10:00:00Z' }))).toBeNull();
    expect(attemptWaitSeconds(attempt('a', 0, { started_at: '2026-01-01T10:00:00Z' }))).toBeNull();
  });

  it('reads a start stamped before its queue time as no wait', () => {
    // queued_at and started_at are separate status reports, stamped by
    // separate clocks; a start a few milliseconds "before" the queue time is
    // clock skew, not a negative wait.
    expect(attemptWaitSeconds(attempt('a', 0, {
      queued_at: '2026-01-01T10:00:00.050Z', started_at: '2026-01-01T10:00:00.010Z',
    }))).toBe(0);
  });

  it('formats fractional seconds as whole seconds', () => {
    expect(formatAttemptSeconds(75.3)).toBe('1m 15s');
    expect(formatAttemptSeconds(19.5)).toBe('20s');
    expect(formatAttemptSeconds(null)).toBe('—');
  });
});
