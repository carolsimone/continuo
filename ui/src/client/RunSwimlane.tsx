import type { GraphNode, GraphEdge, Task } from './types';
import { buildSwimlaneLayout, laneLabelColumnWidth } from './run-graph-helpers';
import { serviceOfNode, buildServiceColors, rollupStatus } from './service-helpers';
import { resolveNodeStatus, parseNodeId } from './detail-page-helpers';

const CLS: Record<string, string> = { succeeded: 'ok', running: 'run', failed: 'fail', skipped: 'skip', cancelled: 'cancel', pending: 'pend' };
const clsOf = (s: string) => CLS[s] ?? 'pend';
// Legend order mirrors the run-progress header counts so the two read as one.
const LEGEND: [string, string][] = [
  ['ok', 'succeeded'], ['run', 'running'], ['fail', 'failed'], ['skip', 'skipped'], ['cancel', 'cancelled'], ['pend', 'pending'],
];

// Rendered node-box vs collapsed status-cell dimensions. Edge endpoints are
// anchored to whichever a node currently is, so a dependency into or out of a
// collapsed lane connects at the cell, not 120px away where the box would be.
const NODE_W = 134;
const NODE_H = 30;
const CELL_SIZE = 14;

// Status graph as swimlanes: one horizontal lane per service, columns are
// dependency depth. Node placement and lane sizing come from
// buildSwimlaneLayout; this component only paints nodes/edges/lane labels at
// the coordinates it returns, and lets a collapsed lane render as a strip of
// small `.swim-cell` squares instead of full node boxes. A node is coloured by
// its status alone (fill, border and dot); the service colour appears only on
// the lane label, since the lane a node sits in already says which service it
// belongs to. The label column is sized to the widest label so nodes never
// start under a long service name.
export default function RunSwimlane({
  graph, tasks, serviceOrder, collapsed, onLaneToggle,
}: {
  graph: { nodes: GraphNode[]; edges: GraphEdge[] };
  tasks: Task[];
  serviceOrder: string[];
  collapsed: Set<string>;
  onLaneToggle: (service: string) => void;
}) {
  const nodeIds = graph.nodes.map((n) => n.node_id);
  const colors = buildServiceColors(serviceOrder);
  const statusById = new Map(graph.nodes.map((n) => [n.node_id, resolveNodeStatus(n, tasks)]));

  const laneRollup = (service: string) => {
    const statuses = graph.nodes
      .filter((n) => serviceOfNode(n.node_id) === service)
      .map((n) => statusById.get(n.node_id)!);
    const done = statuses.filter(
      (s) => s === 'succeeded' || s === 'failed' || s === 'skipped' || s === 'cancelled',
    ).length;
    return { done, total: statuses.length, roll: rollupStatus(statuses) };
  };
  const rollups = new Map(serviceOrder.map((s) => [s, laneRollup(s)]));
  const laneLeft = laneLabelColumnWidth(
    serviceOrder.map((s) => ({ service: s, done: rollups.get(s)!.done, total: rollups.get(s)!.total })),
  );
  const layout = buildSwimlaneLayout(nodeIds, graph.edges, serviceOrder, collapsed, { laneLeft });
  const pos = new Map(layout.nodes.map((n) => [n.nodeId, n]));

  return (
    <>
    <div className="swim-wrap">
      <div className="swim-stage" style={{ position: 'relative', width: layout.width, height: layout.height }}>
        {layout.bands.map((b) => (
          <div
            key={b.service}
            className="swim-lane"
            style={{ position: 'absolute', top: b.top, height: b.height, width: layout.width }}
          />
        ))}
        <svg className="swim-edges" width={layout.width} height={layout.height} style={{ position: 'absolute', inset: 0 }}>
          <defs>
            <marker id="swim-ah" markerWidth="7" markerHeight="7" refX="5.5" refY="3" orient="auto">
              <path d="M0,0 L6,3 L0,6 Z" />
            </marker>
          </defs>
          {graph.edges.map((e, i) => {
            const a = pos.get(e.from_node_id);
            const b = pos.get(e.to_node_id);
            if (!a || !b) return null;
            const aWidth = collapsed.has(a.service) ? CELL_SIZE : NODE_W;
            const aHalfH = (collapsed.has(a.service) ? CELL_SIZE : NODE_H) / 2;
            const bHalfH = (collapsed.has(b.service) ? CELL_SIZE : NODE_H) / 2;
            const sx = a.x + aWidth;
            const sy = a.y + aHalfH;
            const tx = b.x;
            const ty = b.y + bHalfH;
            const mx = (sx + tx) / 2;
            const targetStatus = statusById.get(e.to_node_id);
            const hot = targetStatus === 'failed' || targetStatus === 'skipped';
            return (
              <path
                key={i}
                className={hot ? 'swim-edge-hot' : 'swim-edge'}
                d={`M${sx},${sy} C ${mx},${sy} ${mx},${ty} ${tx},${ty}`}
                markerEnd="url(#swim-ah)"
              />
            );
          })}
        </svg>
        {layout.bands.map((b) => {
          const { done, total, roll } = rollups.get(b.service)!;
          return (
            <div
              key={b.service}
              className="swim-lab"
              data-lane={b.service}
              style={{ position: 'absolute', top: b.top + 9, left: 8 }}
              onClick={() => onLaneToggle(b.service)}
            >
              <span className="chev">{collapsed.has(b.service) ? '▸' : '▾'}</span>
              <span className="swim-dot" style={{ background: colors.get(b.service) }} />
              {b.service} <span className={`swim-roll ${clsOf(roll)}`}>{done}/{total}</span>
            </div>
          );
        })}
        {layout.nodes.map((n) => {
          const status = clsOf(statusById.get(n.nodeId)!);
          if (collapsed.has(n.service)) {
            return (
              <div
                key={n.nodeId}
                className={`swim-cell ${status}`}
                title={n.nodeId}
                style={{ position: 'absolute', left: n.x, top: n.y }}
              />
            );
          }
          return (
            <div
              key={n.nodeId}
              className={`swim-node ${status}`}
              title={n.nodeId}
              style={{ position: 'absolute', left: n.x, top: n.y, width: NODE_W }}
            >
              <span className={`swim-sd ${status}`} />
              {parseNodeId(n.nodeId).table_name}
            </div>
          );
        })}
      </div>
    </div>
    <div className="swim-legend">
      {LEGEND.map(([cls, label]) => (
        <span key={cls}><i className={`swim-legend-key ${cls}`} />{label}</span>
      ))}
    </div>
    </>
  );
}
