import type { GraphEdge } from './types';
import { serviceOfNode } from './service-helpers';

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

export function intraServiceOrder(
  nodeIds: string[],
  edges: GraphEdge[],
  startedAt: Record<string, string | null | undefined>,
): string[] {
  const depth = computeDepth(nodeIds, edges);
  const startKey = (id: string): number => {
    const s = startedAt[id];
    return s ? Date.parse(s) : Number.POSITIVE_INFINITY; // nulls last
  };
  return [...nodeIds].sort(
    (a, b) => (depth[a] - depth[b]) || (startKey(a) - startKey(b)) || a.localeCompare(b),
  );
}

// Width in px of the lane-label column, sized to the widest label so the first
// node column never starts under a long service name. A label is a chevron, a
// service-colour dot, the service name and a done/total roll-up pill; widths
// are estimated from the label's font metrics (12.5px semibold name, 11px
// pill) since the layout is computed before anything is rendered. Never
// narrower than the 104px default so short names keep today's proportions.
export interface LaneLabel { service: string; done: number; total: number; }
const LABEL_LEFT = 8, CHEVRON_W = 10, DOT_W = 8, LABEL_GAP = 6, LABEL_RIGHT_MARGIN = 16;
const NAME_CHAR_W = 7.4, PILL_CHAR_W = 6.6, PILL_PAD = 14;
export const MIN_LANE_LEFT = 104;
export function laneLabelColumnWidth(labels: LaneLabel[]): number {
  let widest = 0;
  for (const l of labels) {
    const name = l.service.length * NAME_CHAR_W;
    const pill = `${l.done}/${l.total}`.length * PILL_CHAR_W + PILL_PAD;
    const w = LABEL_LEFT + CHEVRON_W + LABEL_GAP + DOT_W + LABEL_GAP + name + LABEL_GAP + pill + LABEL_RIGHT_MARGIN;
    if (w > widest) widest = w;
  }
  return Math.max(MIN_LANE_LEFT, Math.ceil(widest));
}

export interface LaneNode { nodeId: string; service: string; depth: number; x: number; y: number; }
export interface LaneBand { service: string; top: number; height: number; }
export interface SwimlaneLayout { nodes: LaneNode[]; bands: LaneBand[]; width: number; height: number; maxDepth: number; }

export function buildSwimlaneLayout(
  nodeIds: string[],
  edges: GraphEdge[],
  serviceOrder: string[],
  collapsed: Set<string>,
  dims: { col?: number; laneLeft?: number; nodeH?: number; row?: number; top?: number } = {},
): SwimlaneLayout {
  const COL = dims.col ?? 158, LEFT = dims.laneLeft ?? MIN_LANE_LEFT, ROW = dims.row ?? 38, TOP = dims.top ?? 32;
  const depth = computeDepth(nodeIds, edges);
  const maxDepth = nodeIds.reduce((m, id) => Math.max(m, depth[id] ?? 0), 0);

  // busiest (service, depth) cell per lane → lane row count
  const maxSub: Record<string, number> = {};
  const cell: Record<string, number> = {};
  for (const s of serviceOrder) maxSub[s] = 1;
  for (const id of nodeIds) {
    const s = serviceOfNode(id);
    const key = `${s}:${depth[id]}`;
    cell[key] = (cell[key] ?? 0) + 1;
    if (cell[key] > (maxSub[s] ?? 1)) maxSub[s] = cell[key];
  }

  const bands: LaneBand[] = [];
  const top: Record<string, number> = {};
  let y = TOP;
  for (const s of serviceOrder) {
    const height = collapsed.has(s) ? 32 : 10 + maxSub[s] * ROW;
    top[s] = y;
    bands.push({ service: s, top: y, height });
    y += height;
  }

  const COLLAPSED_CELL_STRIDE = 18; // px between adjacent status cells in a collapsed lane
  const nodes: LaneNode[] = [];
  const cellIdx: Record<string, number> = {};
  for (const id of [...nodeIds].sort((a, b) => depth[a] - depth[b])) {
    const s = serviceOfNode(id);
    const d = depth[id];
    const key = `${s}:${d}`;
    const sub = (cellIdx[key] = cellIdx[key] == null ? 0 : cellIdx[key] + 1);
    const isCollapsed = collapsed.has(s);
    // A collapsed lane packs its nodes into one row of status cells; stagger
    // them along x by their in-cell index so two nodes sharing a service and
    // depth never render at the same coordinates (which would hide one behind
    // the other, e.g. a failed node under a succeeded one). Expanded lanes
    // stack by row (y) instead.
    nodes.push({
      nodeId: id, service: s, depth: d,
      x: LEFT + d * COL + (isCollapsed ? sub * COLLAPSED_CELL_STRIDE : 0),
      y: top[s] + 9 + (isCollapsed ? 0 : sub * ROW),
    });
  }
  return { nodes, bands, width: LEFT + (maxDepth + 1) * COL + 16, height: y + 8, maxDepth };
}
