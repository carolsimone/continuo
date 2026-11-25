import { GraphEdge, GraphNode, Task } from './types';
import { resolveNodeStatus } from './detail-page-helpers';

// Project vertices live in the same React Flow graph as model nodes; the
// prefix keeps their ids disjoint from "{project}.{schema}.{table}" node ids.
const PROJECT_VERTEX_PREFIX = 'svc:';

export function projectOfNode(nodeId: string): string {
  return nodeId.split('.')[0] ?? '';
}

export function projectVertexId(project: string): string {
  return `${PROJECT_VERTEX_PREFIX}${project}`;
}

export function isProjectVertexId(id: string): boolean {
  return id.startsWith(PROJECT_VERTEX_PREFIX);
}

export function projectFromVertexId(id: string): string {
  return id.slice(PROJECT_VERTEX_PREFIX.length);
}

// Accent palette for project identity. Deliberately distinct from the status
// hues (indigo/green/red) used to paint execution state.
const PROJECT_PALETTE = [
  '#0ea5e9', // sky
  '#f59e0b', // amber
  '#8b5cf6', // violet
  '#f43f5e', // rose
  '#14b8a6', // teal
  '#84cc16', // lime
  '#d946ef', // fuchsia
  '#f97316', // orange
];

export function listProjects(graphNodes: GraphNode[], tasks: Task[]): string[] {
  const projects = new Set<string>();
  graphNodes.forEach((n) => projects.add(projectOfNode(n.node_id)));
  tasks.forEach((t) => projects.add(t.project_name));
  projects.delete('');
  return [...projects].sort();
}

// Colors are assigned by sorted project name, so the same project always gets
// the same color in the graph and the table regardless of data order.
export function buildProjectColors(projects: string[]): Map<string, string> {
  const sorted = [...new Set(projects)].sort();
  return new Map(sorted.map((svc, i) => [svc, PROJECT_PALETTE[i % PROJECT_PALETTE.length]]));
}

// Most-severe-first. A project with a running task reads running; one whose
// tasks wait for a slot (queued) or for execution-controller (dispatched)
// reads that before pending. A project with a stopping task (a cancel is
// deleting its Job) reads stopping, above running. `skipped` (cascade-skipped downstream of a
// failure) outranks `succeeded` so a partially- or fully-skipped project is
// flagged rather than reading green.
const ROLLUP_PRECEDENCE = ['failed', 'stopping', 'running', 'queued', 'dispatched', 'pending', 'skipped', 'succeeded', 'cancelled'];

export function rollupStatus(statuses: string[]): string {
  for (const status of ROLLUP_PRECEDENCE) {
    if (statuses.includes(status)) return status;
  }
  return statuses.length > 0 ? 'external' : 'pending';
}

export interface ProjectVertex {
  project: string;
  status: string;
  nodeCount: number;
}

export interface DisplayEdge {
  from: string;
  to: string;
}

export interface ProjectDisplayGraph {
  projectVertices: ProjectVertex[]; // one per collapsed project
  modelNodes: GraphNode[]; // nodes of expanded projects, rendered as today
  edges: DisplayEdge[]; // deduped; endpoints are node ids or project vertex ids
}

// Maps the model-level DAG onto a mixed graph: collapsed projects become
// single vertices, expanded projects keep their model nodes. Edges whose two
// endpoints collapse into the same project vertex are dropped (self-loops);
// the remaining edges are deduped. The resulting project-level graph may be
// cyclic (svc A→B and B→A via different models) — callers must not assume a
// DAG.
export function buildProjectDisplayGraph(
  graphNodes: GraphNode[],
  graphEdges: GraphEdge[],
  tasks: Task[],
  expandedProjects: Set<string>,
  colorByStatus: boolean,
): ProjectDisplayGraph {
  const nodesByProject = new Map<string, GraphNode[]>();
  graphNodes.forEach((n) => {
    const svc = projectOfNode(n.node_id);
    const bucket = nodesByProject.get(svc);
    if (bucket) bucket.push(n);
    else nodesByProject.set(svc, [n]);
  });

  const projectVertices: ProjectVertex[] = [];
  const modelNodes: GraphNode[] = [];
  [...nodesByProject.keys()].sort().forEach((svc) => {
    const nodes = nodesByProject.get(svc)!;
    if (expandedProjects.has(svc)) {
      modelNodes.push(...nodes);
      return;
    }
    const statuses = colorByStatus ? nodes.map((n) => resolveNodeStatus(n, tasks)) : [];
    projectVertices.push({
      project: svc,
      status: colorByStatus ? rollupStatus(statuses) : 'pending',
      nodeCount: nodes.length,
    });
  });

  const endpoint = (nodeId: string): string => {
    const svc = projectOfNode(nodeId);
    return expandedProjects.has(svc) ? nodeId : projectVertexId(svc);
  };

  const seen = new Set<string>();
  const edges: DisplayEdge[] = [];
  graphEdges.forEach((e) => {
    const from = endpoint(e.from_node_id);
    const to = endpoint(e.to_node_id);
    if (from === to) return;
    const key = `${from}→${to}`;
    if (seen.has(key)) return;
    seen.add(key);
    edges.push({ from, to });
  });

  return { projectVertices, modelNodes, edges };
}
