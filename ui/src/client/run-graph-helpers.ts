import type { GraphEdge } from './types';

export function computeDepth(nodeIds: string[], edges: GraphEdge[]): Record<string, number> {
  const known = new Set(nodeIds);
  const preds: Record<string, string[]> = {};
  for (const id of nodeIds) preds[id] = [];
  for (const edge of edges) {
    if (known.has(edge.from_node_id) && known.has(edge.to_node_id)) {
      preds[edge.to_node_id].push(edge.from_node_id);
    }
  }
  const depth: Record<string, number> = {};
  const visiting = new Set<string>();
  const walk = (id: string): number => {
    if (depth[id] != null) return depth[id];
    if (visiting.has(id)) return 0; // cycle guard: break the loop, treat as a root
    visiting.add(id);
    let max = 0;
    for (const pred of preds[id]) max = Math.max(max, walk(pred) + 1);
    visiting.delete(id);
    depth[id] = max;
    return max;
  };
  for (const id of nodeIds) walk(id);
  return depth;
}
