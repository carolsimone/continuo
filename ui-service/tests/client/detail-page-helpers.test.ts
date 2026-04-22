import { describe, expect, it } from 'vitest';
import {
  parseNodeId,
  resolveActiveGraph,
  resolveNodeStatus,
  toScheduleGraph,
} from '../../src/client/detail-page-helpers';
import { GraphNode, RunGraph, ScheduleGraph, Task } from '../../src/client/types';

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
      edges: [{ from_node_id: 'svc.analytics.orders', to_node_id: 'svc.raw.seed_orders' }],
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
      edges: [{ from_node_id: 'svc.analytics.orders', to_node_id: 'svc.raw.seed_orders' }],
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
      retry_count: 0,
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
});
