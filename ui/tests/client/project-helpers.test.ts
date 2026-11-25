import { describe, it, expect } from 'vitest';
import {
  buildProjectColors,
  buildProjectDisplayGraph,
  isProjectVertexId,
  listProjects,
  rollupStatus,
  projectFromVertexId,
  projectOfNode,
  projectVertexId,
} from '../../src/client/project-helpers';
import type { GraphEdge, GraphNode, Task } from '../../src/client/types';

function node(id: string, status?: string): GraphNode {
  return { node_id: id, node_type: 'dbt-model', schedule_name: 'sched', status };
}

function edge(from: string, to: string): GraphEdge {
  return { from_node_id: from, to_node_id: to };
}

function task(nodeId: string, status: string): Task {
  const [project_name, schema_name, table_name] = nodeId.split('.');
  return {
    task_id: nodeId, project_name, schema_name, table_name,
    job_name: '', status, attempt: 0, max_retries: 0, created_at: null,
  };
}

describe('project vertex ids', () => {
  it('round-trips a project name and never collides with model node ids', () => {
    const id = projectVertexId('service-1');
    expect(isProjectVertexId(id)).toBe(true);
    expect(projectFromVertexId(id)).toBe('service-1');
    expect(isProjectVertexId('service-1.analytics.table_a')).toBe(false);
  });

  it('projectOfNode returns the first dotted segment', () => {
    expect(projectOfNode('service-2.analytics.table_new')).toBe('service-2');
  });
});

describe('rollupStatus', () => {
  it('applies failed > running > pending > skipped > succeeded > cancelled precedence', () => {
    expect(rollupStatus(['succeeded', 'failed', 'running'])).toBe('failed');
    expect(rollupStatus(['succeeded', 'running'])).toBe('running');
    expect(rollupStatus(['succeeded', 'pending'])).toBe('pending');
    expect(rollupStatus(['succeeded', 'skipped'])).toBe('skipped');
    expect(rollupStatus(['succeeded', 'cancelled'])).toBe('succeeded');
    expect(rollupStatus(['cancelled', 'cancelled'])).toBe('cancelled');
  });

  it('ranks queued and dispatched below running and above pending', () => {
    expect(rollupStatus(['queued', 'running'])).toBe('running');
    expect(rollupStatus(['succeeded', 'queued', 'dispatched'])).toBe('queued');
    expect(rollupStatus(['pending', 'dispatched'])).toBe('dispatched');
  });

  it('ranks stopping above running: a project being stopped reads stopping', () => {
    expect(rollupStatus(['running', 'stopping'])).toBe('stopping');
    expect(rollupStatus(['failed', 'stopping'])).toBe('failed');
    expect(rollupStatus(['succeeded', 'stopping'])).toBe('stopping');
    expect(rollupStatus(['queued', 'stopping'])).toBe('stopping');
  });

  it('reports a fully cascade-skipped project as skipped, not external', () => {
    expect(rollupStatus(['skipped', 'skipped'])).toBe('skipped');
  });

  it('falls back to external for unknown statuses and pending for empty input', () => {
    expect(rollupStatus(['external', 'external'])).toBe('external');
    expect(rollupStatus([])).toBe('pending');
  });
});

describe('buildProjectColors / listProjects', () => {
  it('assigns the same color to a project regardless of input order', () => {
    const a = buildProjectColors(['svc-b', 'svc-a', 'svc-c']);
    const b = buildProjectColors(['svc-c', 'svc-b', 'svc-a']);
    ['svc-a', 'svc-b', 'svc-c'].forEach((svc) => {
      expect(a.get(svc)).toBeDefined();
      expect(a.get(svc)).toBe(b.get(svc));
    });
    expect(new Set(a.values()).size).toBe(3);
  });

  it('listProjects unions graph nodes and tasks, sorted and deduped', () => {
    const projects = listProjects(
      [node('svc-b.s.x'), node('svc-a.s.y')],
      [task('svc-c.s.z', 'succeeded'), task('svc-a.s.y', 'succeeded')],
    );
    expect(projects).toEqual(['svc-a', 'svc-b', 'svc-c']);
  });
});

describe('buildProjectDisplayGraph', () => {
  const nodes = [
    node('svc-a.s.n1'), node('svc-a.s.n2'),
    node('svc-b.s.m1'), node('svc-b.s.m2'),
  ];

  it('collapses every project to one vertex and dedupes cross-project edges', () => {
    const edges = [
      edge('svc-a.s.n1', 'svc-b.s.m1'),
      edge('svc-a.s.n2', 'svc-b.s.m2'), // second a→b dependency: deduped
      edge('svc-a.s.n1', 'svc-a.s.n2'), // intra-project: dropped as a self-loop
    ];
    const view = buildProjectDisplayGraph(nodes, edges, [], new Set(), true);
    expect(view.modelNodes).toEqual([]);
    expect(view.projectVertices.map((v) => v.project)).toEqual(['svc-a', 'svc-b']);
    expect(view.projectVertices.map((v) => v.nodeCount)).toEqual([2, 2]);
    expect(view.edges).toEqual([
      { from: projectVertexId('svc-a'), to: projectVertexId('svc-b') },
    ]);
  });

  it('keeps both directions of a project-level cycle (A→B and B→A via different models)', () => {
    const edges = [
      edge('svc-a.s.n1', 'svc-b.s.m1'),
      edge('svc-b.s.m2', 'svc-a.s.n2'),
    ];
    const view = buildProjectDisplayGraph(nodes, edges, [], new Set(), true);
    expect(view.edges).toEqual([
      { from: projectVertexId('svc-a'), to: projectVertexId('svc-b') },
      { from: projectVertexId('svc-b'), to: projectVertexId('svc-a') },
    ]);
  });

  it('expands only the requested project, rewiring edges to its model nodes', () => {
    const edges = [
      edge('svc-a.s.n1', 'svc-b.s.m1'),
      edge('svc-a.s.n1', 'svc-a.s.n2'),
    ];
    const view = buildProjectDisplayGraph(nodes, edges, [], new Set(['svc-a']), true);
    expect(view.modelNodes.map((n) => n.node_id)).toEqual(['svc-a.s.n1', 'svc-a.s.n2']);
    expect(view.projectVertices.map((v) => v.project)).toEqual(['svc-b']);
    expect(view.edges).toEqual([
      { from: 'svc-a.s.n1', to: projectVertexId('svc-b') },
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
    const view = buildProjectDisplayGraph(nodes, [], tasks, new Set(), true);
    expect(view.projectVertices.find((v) => v.project === 'svc-a')?.status).toBe('failed');
    expect(view.projectVertices.find((v) => v.project === 'svc-b')?.status).toBe('running');
  });

  it('reports pending vertices when colorByStatus is off', () => {
    const view = buildProjectDisplayGraph(nodes, [], [task('svc-a.s.n1', 'failed')], new Set(), false);
    view.projectVertices.forEach((v) => expect(v.status).toBe('pending'));
  });
});
