import { describe, it, expect } from 'vitest';
import {
  buildServiceColors,
  buildServiceDisplayGraph,
  isServiceVertexId,
  listServices,
  rollupStatus,
  serviceFromVertexId,
  serviceOfNode,
  serviceVertexId,
} from '../../src/client/service-helpers';
import type { GraphEdge, GraphNode, Task } from '../../src/client/types';

function node(id: string, status?: string): GraphNode {
  return { node_id: id, node_type: 'dbt-model', schedule_name: 'sched', status };
}

function edge(from: string, to: string): GraphEdge {
  return { from_node_id: from, to_node_id: to };
}

function task(nodeId: string, status: string): Task {
  const [service_name, schema_name, table_name] = nodeId.split('.');
  return {
    task_id: nodeId, service_name, schema_name, table_name,
    job_name: '', status, retry_count: 0, max_retries: 0, created_at: null,
  };
}

describe('service vertex ids', () => {
  it('round-trips a service name and never collides with model node ids', () => {
    const id = serviceVertexId('service-1');
    expect(isServiceVertexId(id)).toBe(true);
    expect(serviceFromVertexId(id)).toBe('service-1');
    expect(isServiceVertexId('service-1.analytics.table_a')).toBe(false);
  });

  it('serviceOfNode returns the first dotted segment', () => {
    expect(serviceOfNode('service-2.analytics.table_new')).toBe('service-2');
  });
});

describe('rollupStatus', () => {
  it('applies failed > running > pending > succeeded > cancelled precedence', () => {
    expect(rollupStatus(['succeeded', 'failed', 'running'])).toBe('failed');
    expect(rollupStatus(['succeeded', 'running'])).toBe('running');
    expect(rollupStatus(['succeeded', 'pending'])).toBe('pending');
    expect(rollupStatus(['succeeded', 'cancelled'])).toBe('succeeded');
    expect(rollupStatus(['cancelled', 'cancelled'])).toBe('cancelled');
  });

  it('falls back to external for unknown statuses and pending for empty input', () => {
    expect(rollupStatus(['external', 'external'])).toBe('external');
    expect(rollupStatus([])).toBe('pending');
  });
});

describe('buildServiceColors / listServices', () => {
  it('assigns the same color to a service regardless of input order', () => {
    const a = buildServiceColors(['svc-b', 'svc-a', 'svc-c']);
    const b = buildServiceColors(['svc-c', 'svc-b', 'svc-a']);
    ['svc-a', 'svc-b', 'svc-c'].forEach((svc) => {
      expect(a.get(svc)).toBeDefined();
      expect(a.get(svc)).toBe(b.get(svc));
    });
    expect(new Set(a.values()).size).toBe(3);
  });

  it('listServices unions graph nodes and tasks, sorted and deduped', () => {
    const services = listServices(
      [node('svc-b.s.x'), node('svc-a.s.y')],
      [task('svc-c.s.z', 'succeeded'), task('svc-a.s.y', 'succeeded')],
    );
    expect(services).toEqual(['svc-a', 'svc-b', 'svc-c']);
  });
});

describe('buildServiceDisplayGraph', () => {
  const nodes = [
    node('svc-a.s.n1'), node('svc-a.s.n2'),
    node('svc-b.s.m1'), node('svc-b.s.m2'),
  ];

  it('collapses every service to one vertex and dedupes cross-service edges', () => {
    const edges = [
      edge('svc-a.s.n1', 'svc-b.s.m1'),
      edge('svc-a.s.n2', 'svc-b.s.m2'), // second a→b dependency: deduped
      edge('svc-a.s.n1', 'svc-a.s.n2'), // intra-service: dropped as a self-loop
    ];
    const view = buildServiceDisplayGraph(nodes, edges, [], new Set(), true);
    expect(view.modelNodes).toEqual([]);
    expect(view.serviceVertices.map((v) => v.service)).toEqual(['svc-a', 'svc-b']);
    expect(view.serviceVertices.map((v) => v.nodeCount)).toEqual([2, 2]);
    expect(view.edges).toEqual([
      { from: serviceVertexId('svc-a'), to: serviceVertexId('svc-b') },
    ]);
  });

  it('keeps both directions of a service-level cycle (A→B and B→A via different models)', () => {
    const edges = [
      edge('svc-a.s.n1', 'svc-b.s.m1'),
      edge('svc-b.s.m2', 'svc-a.s.n2'),
    ];
    const view = buildServiceDisplayGraph(nodes, edges, [], new Set(), true);
    expect(view.edges).toEqual([
      { from: serviceVertexId('svc-a'), to: serviceVertexId('svc-b') },
      { from: serviceVertexId('svc-b'), to: serviceVertexId('svc-a') },
    ]);
  });

  it('expands only the requested service, rewiring edges to its model nodes', () => {
    const edges = [
      edge('svc-a.s.n1', 'svc-b.s.m1'),
      edge('svc-a.s.n1', 'svc-a.s.n2'),
    ];
    const view = buildServiceDisplayGraph(nodes, edges, [], new Set(['svc-a']), true);
    expect(view.modelNodes.map((n) => n.node_id)).toEqual(['svc-a.s.n1', 'svc-a.s.n2']);
    expect(view.serviceVertices.map((v) => v.service)).toEqual(['svc-b']);
    expect(view.edges).toEqual([
      { from: 'svc-a.s.n1', to: serviceVertexId('svc-b') },
      { from: 'svc-a.s.n1', to: 'svc-a.s.n2' },
    ]);
  });

  it('rolls up vertex status from task statuses with failed taking precedence', () => {
    const tasks = [
      task('svc-a.s.n1', 'succeeded'),
      task('svc-a.s.n2', 'failed'),
      task('svc-b.s.m1', 'running'),
      task('svc-b.s.m2', 'succeeded'),
    ];
    const view = buildServiceDisplayGraph(nodes, [], tasks, new Set(), true);
    expect(view.serviceVertices.find((v) => v.service === 'svc-a')?.status).toBe('failed');
    expect(view.serviceVertices.find((v) => v.service === 'svc-b')?.status).toBe('running');
  });

  it('reports pending vertices when colorByStatus is off', () => {
    const view = buildServiceDisplayGraph(nodes, [], [task('svc-a.s.n1', 'failed')], new Set(), false);
    view.serviceVertices.forEach((v) => expect(v.status).toBe('pending'));
  });
});
