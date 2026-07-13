import { GraphEdge, GraphNode, Task } from './types';
import { resolveNodeStatus } from './detail-page-helpers';

// Service vertices live in the same React Flow graph as model nodes; the
// prefix keeps their ids disjoint from "{service}.{schema}.{table}" node ids.
const SERVICE_VERTEX_PREFIX = 'svc:';

export function serviceOfNode(nodeId: string): string {
  return nodeId.split('.')[0] ?? '';
}

export function serviceVertexId(service: string): string {
  return `${SERVICE_VERTEX_PREFIX}${service}`;
}

export function isServiceVertexId(id: string): boolean {
  return id.startsWith(SERVICE_VERTEX_PREFIX);
}

export function serviceFromVertexId(id: string): string {
  return id.slice(SERVICE_VERTEX_PREFIX.length);
}

// Accent palette for service identity. Deliberately distinct from the status
// hues (indigo/green/red) used to paint execution state.
const SERVICE_PALETTE = [
  '#0ea5e9', // sky
  '#f59e0b', // amber
  '#8b5cf6', // violet
  '#f43f5e', // rose
  '#14b8a6', // teal
  '#84cc16', // lime
  '#d946ef', // fuchsia
  '#f97316', // orange
];

export function listServices(graphNodes: GraphNode[], tasks: Task[]): string[] {
  const services = new Set<string>();
  graphNodes.forEach((n) => services.add(serviceOfNode(n.node_id)));
  tasks.forEach((t) => services.add(t.service_name));
  services.delete('');
  return [...services].sort();
}

// Colors are assigned by sorted service name, so the same service always gets
// the same color in the graph and the table regardless of data order.
export function buildServiceColors(services: string[]): Map<string, string> {
  const sorted = [...new Set(services)].sort();
  return new Map(sorted.map((svc, i) => [svc, SERVICE_PALETTE[i % SERVICE_PALETTE.length]]));
}

// Most-severe-first, matching the row-sort precedence used by the nodes table.
const ROLLUP_PRECEDENCE = ['failed', 'running', 'pending', 'succeeded', 'cancelled'];

export function rollupStatus(statuses: string[]): string {
  for (const status of ROLLUP_PRECEDENCE) {
    if (statuses.includes(status)) return status;
  }
  return statuses.length > 0 ? 'external' : 'pending';
}

export interface ServiceVertex {
  service: string;
  status: string;
  nodeCount: number;
}

export interface DisplayEdge {
  from: string;
  to: string;
}

export interface ServiceDisplayGraph {
  serviceVertices: ServiceVertex[]; // one per collapsed service
  modelNodes: GraphNode[]; // nodes of expanded services, rendered as today
  edges: DisplayEdge[]; // deduped; endpoints are node ids or service vertex ids
}

// Projects the model-level DAG onto a mixed graph: collapsed services become
// single vertices, expanded services keep their model nodes. Edges whose two
// endpoints collapse into the same service vertex are dropped (self-loops);
// the remaining edges are deduped. The resulting service-level graph may be
// cyclic (svc A→B and B→A via different models) — callers must not assume a
// DAG.
export function buildServiceDisplayGraph(
  graphNodes: GraphNode[],
  graphEdges: GraphEdge[],
  tasks: Task[],
  expandedServices: Set<string>,
  colorByStatus: boolean,
): ServiceDisplayGraph {
  const nodesByService = new Map<string, GraphNode[]>();
  graphNodes.forEach((n) => {
    const svc = serviceOfNode(n.node_id);
    const bucket = nodesByService.get(svc);
    if (bucket) bucket.push(n);
    else nodesByService.set(svc, [n]);
  });

  const serviceVertices: ServiceVertex[] = [];
  const modelNodes: GraphNode[] = [];
  [...nodesByService.keys()].sort().forEach((svc) => {
    const nodes = nodesByService.get(svc)!;
    if (expandedServices.has(svc)) {
      modelNodes.push(...nodes);
      return;
    }
    const statuses = colorByStatus ? nodes.map((n) => resolveNodeStatus(n, tasks)) : [];
    serviceVertices.push({
      service: svc,
      status: colorByStatus ? rollupStatus(statuses) : 'pending',
      nodeCount: nodes.length,
    });
  });

  const endpoint = (nodeId: string): string => {
    const svc = serviceOfNode(nodeId);
    return expandedServices.has(svc) ? nodeId : serviceVertexId(svc);
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

  return { serviceVertices, modelNodes, edges };
}
