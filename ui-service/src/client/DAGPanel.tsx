import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Background,
  Edge,
  MiniMap,
  Node,
  OnNodeDrag,
  ReactFlow,
  XYPosition,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import * as dagre from 'dagre';
import { GraphEdge, GraphNode, Task } from './types';
import { resolveNodeStatus, taskNodeId } from './detail-page-helpers';
import {
  DisplayEdge,
  ServiceVertex,
  buildServiceDisplayGraph,
  isServiceVertexId,
  serviceFromVertexId,
  serviceOfNode,
  serviceVertexId,
} from './service-helpers';

const NODE_WIDTH = 160;
const NODE_HEIGHT = 44;
const SERVICE_WIDTH = 200;
const SERVICE_HEIGHT = 56;

const EMPTY_SERVICES = new Set<string>();

type FocusRole = 'selected' | 'parent' | 'child' | 'dim' | null;

const STATUS_BG: Record<string, string> = {
  running: '#eef2ff',
  succeeded: '#f0fdf4',
  failed: '#fef2f2',
  pending: '#fff',
  cancelled: '#f8fafc',
  skipped: '#f1f5f9',
  external: '#fafafa',
};
const STATUS_BORDER: Record<string, string> = {
  running: '#a5b4fc',
  succeeded: '#86efac',
  failed: '#fca5a5',
  pending: '#d1d5db',
  cancelled: '#e2e8f0',
  skipped: '#cbd5e1',
  external: '#9ca3af',
};

// Borders are expressed with longhand properties only. Mixing the `border`
// shorthand with a `borderLeft*` accent breaks on re-render: React re-applies
// the changed shorthand, which resets the left border and drops the service
// accent (and warns about the shorthand/longhand mix).
function borderProps(
  width: string,
  style: string,
  color: string,
  accent: string | undefined,
  accentWidth: string,
): React.CSSProperties {
  return {
    borderWidth: width,
    borderStyle: style,
    borderColor: color,
    ...(accent
      ? {
          borderLeftWidth: accentWidth,
          borderLeftStyle: 'solid' as const,
          borderLeftColor: accent,
        }
      : {}),
  };
}

function nodeStyle(
  status: string,
  isExternal: boolean,
  role: FocusRole,
  accent?: string,
): React.CSSProperties {
  const base: React.CSSProperties = {
    borderRadius: 8,
    width: NODE_WIDTH,
    fontSize: 12,
    cursor: 'pointer',
    transition: 'opacity 0.15s, box-shadow 0.15s',
  };

  if (role === 'dim') {
    return {
      ...base,
      background: '#fff',
      ...borderProps('1px', 'solid', '#e5e7eb', accent, '4px'),
      opacity: 0.2,
    };
  }

  let bg = STATUS_BG[status] ?? '#f3f4f6';
  let borderColor = STATUS_BORDER[status] ?? '#d1d5db';
  let borderWidth = '1.5px';
  let borderStyleVal = isExternal ? 'dashed' : 'solid';
  let boxShadow: string | undefined;

  if (role === 'selected') {
    bg = '#eef2ff';
    borderColor = '#6366f1';
    borderWidth = '2px';
    borderStyleVal = 'solid';
    boxShadow = '0 0 0 3px rgba(99, 102, 241, 0.25)';
  } else if (role === 'parent') {
    bg = '#eef2ff';
    borderColor = '#a5b4fc';
    borderWidth = '1.5px';
    borderStyleVal = 'solid';
    boxShadow = '0 0 0 2px rgba(165, 180, 252, 0.3)';
  } else if (role === 'child') {
    bg = '#f0fdf4';
    borderColor = '#86efac';
    borderWidth = '1.5px';
    borderStyleVal = 'solid';
    boxShadow = '0 0 0 2px rgba(134, 239, 172, 0.3)';
  }

  return {
    ...base,
    background: bg,
    ...borderProps(borderWidth, borderStyleVal, borderColor, accent, '4px'),
    boxShadow,
  };
}

function serviceVertexStyle(
  status: string,
  accent: string,
  role: FocusRole,
): React.CSSProperties {
  const base: React.CSSProperties = {
    borderRadius: 10,
    width: SERVICE_WIDTH,
    fontSize: 12,
    cursor: 'pointer',
    transition: 'opacity 0.15s, box-shadow 0.15s',
  };

  if (role === 'dim') {
    return {
      ...base,
      background: '#fff',
      ...borderProps('1px', 'solid', '#e5e7eb', accent, '5px'),
      opacity: 0.2,
    };
  }

  let boxShadow: string | undefined;
  if (role === 'parent') boxShadow = '0 0 0 2px rgba(165, 180, 252, 0.3)';
  else if (role === 'child') boxShadow = '0 0 0 2px rgba(134, 239, 172, 0.3)';

  return {
    ...base,
    background: STATUS_BG[status] ?? '#f3f4f6',
    ...borderProps('1.5px', 'solid', STATUS_BORDER[status] ?? '#d1d5db', accent, '5px'),
    boxShadow,
  };
}

function edgeStyle(
  fromId: string,
  toId: string,
  selectedNodeId: string | null,
  parentIds: Set<string>,
  childIds: Set<string>,
): React.CSSProperties {
  if (!selectedNodeId) return { stroke: '#d1d5db', strokeWidth: 1.5 };
  if (toId === selectedNodeId && parentIds.has(fromId)) return { stroke: '#a5b4fc', strokeWidth: 2 };
  if (fromId === selectedNodeId && childIds.has(toId)) return { stroke: '#86efac', strokeWidth: 2 };
  return { stroke: '#e5e7eb', strokeWidth: 1, opacity: 0.2 };
}

function focusRole(
  id: string,
  selectedNodeId: string | null,
  parentIds: Set<string>,
  childIds: Set<string>,
): FocusRole {
  if (!selectedNodeId) return null;
  if (id === selectedNodeId) return 'selected';
  if (parentIds.has(id)) return 'parent';
  if (childIds.has(id)) return 'child';
  return 'dim';
}

function buildLayout(
  graphNodes: GraphNode[],
  graphEdges: GraphEdge[],
  tasks: Task[],
  selectedNodeId: string | null,
  colorByStatus: boolean,
  serviceView: boolean,
  expandedServices: Set<string>,
  serviceColors: Map<string, string>,
): { nodes: Node[]; edges: Edge[] } {
  let modelNodes: GraphNode[];
  let serviceVertices: ServiceVertex[];
  let displayEdges: DisplayEdge[];

  if (serviceView) {
    const view = buildServiceDisplayGraph(graphNodes, graphEdges, tasks, expandedServices, colorByStatus);
    modelNodes = view.modelNodes;
    serviceVertices = view.serviceVertices;
    displayEdges = view.edges;
  } else {
    modelNodes = graphNodes;
    serviceVertices = [];
    displayEdges = graphEdges.map((e) => ({ from: e.from_node_id, to: e.to_node_id }));
  }

  // The service-level projection may be cyclic even though the model graph is
  // a DAG; dagre's layout breaks cycles internally by reversing back-edges, so
  // a cyclic display graph lays out without special handling.
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: 'TB', nodesep: 40, ranksep: 60 });

  modelNodes.forEach((n) => g.setNode(n.node_id, { width: NODE_WIDTH, height: NODE_HEIGHT }));
  serviceVertices.forEach((v) =>
    g.setNode(serviceVertexId(v.service), { width: SERVICE_WIDTH, height: SERVICE_HEIGHT }),
  );
  displayEdges.forEach((e) => g.setEdge(e.from, e.to));
  dagre.layout(g);

  const parentIds = selectedNodeId
    ? new Set(displayEdges.filter((e) => e.to === selectedNodeId).map((e) => e.from))
    : new Set<string>();
  const childIds = selectedNodeId
    ? new Set(displayEdges.filter((e) => e.from === selectedNodeId).map((e) => e.to))
    : new Set<string>();

  const nodes: Node[] = modelNodes.map((n) => {
    const pos = g.node(n.node_id);
    const task = tasks.find((candidate) => taskNodeId(candidate) === n.node_id);
    const status = colorByStatus ? resolveNodeStatus(n, tasks) : 'pending';
    const isExternal = colorByStatus ? !task && !n.status : false;
    const role = focusRole(n.node_id, selectedNodeId, parentIds, childIds);
    const accent = serviceView ? serviceColors.get(serviceOfNode(n.node_id)) : undefined;

    return {
      id: n.node_id,
      position: { x: pos.x - NODE_WIDTH / 2, y: pos.y - NODE_HEIGHT / 2 },
      data: { label: n.node_id.split('.').pop() ?? n.node_id },
      style: nodeStyle(status, isExternal, role, accent),
    };
  });

  serviceVertices.forEach((v) => {
    const id = serviceVertexId(v.service);
    const pos = g.node(id);
    const accent = serviceColors.get(v.service) ?? '#94a3b8';
    nodes.push({
      id,
      position: { x: pos.x - SERVICE_WIDTH / 2, y: pos.y - SERVICE_HEIGHT / 2 },
      data: {
        label: (
          <div className="dag-service-vertex">
            <span className="dag-service-vertex-dot" style={{ background: accent }} />
            <span className="dag-service-vertex-name">{v.service}</span>
            <span className="dag-service-vertex-count">{v.nodeCount}</span>
          </div>
        ),
      },
      style: serviceVertexStyle(v.status, accent, focusRole(id, selectedNodeId, parentIds, childIds)),
    });
  });

  const edges: Edge[] = displayEdges.map((e, i) => ({
    id: `e-${i}`,
    source: e.from,
    target: e.to,
    type: 'smoothstep',
    style: edgeStyle(e.from, e.to, selectedNodeId, parentIds, childIds),
  }));

  return { nodes, edges };
}

// Overlays the positions the operator has dragged nodes to onto a freshly
// computed dagre layout. `buildLayout` derives every position from the
// topology alone, and it re-runs whenever the graph poll, a status change or a
// selection produces new inputs; without this overlay each of those would snap
// a moved node back to its computed slot. Nodes the operator has not touched
// keep following the automatic layout.
function applyManualPositions(nodes: Node[], manual: Map<string, XYPosition>): Node[] {
  if (manual.size === 0) return nodes;
  return nodes.map((node) => {
    const position = manual.get(node.id);
    return position ? { ...node, position } : node;
  });
}

function miniMapNodeColor(node: Node): string {
  const style = node.style as React.CSSProperties | undefined;
  const bg = style?.background as string | undefined;
  if (!bg) return '#e5e7eb';
  if (bg === '#eef2ff') return '#a5b4fc';
  if (bg === '#f0fdf4') return '#86efac';
  if (bg === '#fef2f2') return '#fca5a5';
  return '#e5e7eb';
}

interface Props {
  graphNodes: GraphNode[];
  graphEdges: GraphEdge[];
  tasks: Task[];
  selectedNodeId: string | null;
  onNodeClick: (nodeId: string | null) => void;
  colorByStatus?: boolean;
  // Service-level abstraction: collapsed services render as single vertices
  // with a rolled-up status; clicking one calls onServiceClick to toggle it.
  serviceView?: boolean;
  expandedServices?: Set<string>;
  onServiceClick?: (service: string) => void;
  serviceColors?: Map<string, string>;
}

export default function DAGPanel({
  graphNodes,
  graphEdges,
  tasks,
  selectedNodeId,
  onNodeClick,
  colorByStatus = true,
  serviceView = false,
  expandedServices = EMPTY_SERVICES,
  onServiceClick,
  serviceColors,
}: Props) {
  const colors = useMemo(
    () => serviceColors ?? new Map<string, string>(),
    [serviceColors],
  );
  const layout = useMemo(
    () =>
      buildLayout(
        graphNodes,
        graphEdges,
        tasks,
        selectedNodeId,
        colorByStatus,
        serviceView,
        expandedServices,
        colors,
      ),
    [graphEdges, graphNodes, selectedNodeId, tasks, colorByStatus, serviceView, expandedServices, colors],
  );
  // Positions the operator has dragged nodes to, keyed by canvas node id (a
  // model node id, or a service vertex id in service view). They live for as
  // long as the panel is mounted, so an arrangement built to isolate a few
  // services survives the graph poll, status updates and selection changes.
  const [manualPositions, setManualPositions] = useState<Map<string, XYPosition>>(
    () => new Map(),
  );
  const [nodes, setNodes, onNodesChange] = useNodesState(
    applyManualPositions(layout.nodes, manualPositions),
  );
  const [edges, setEdges, onEdgesChange] = useEdgesState(layout.edges);
  const { fitView, getViewport, zoomIn, zoomOut } = useReactFlow();

  const [zoomLevel, setZoomLevel] = useState(100);
  const [searchQuery, setSearchQuery] = useState('');
  const lastAutoFocusKeyRef = useRef<string | null>(null);
  const lastSearchMatchRef = useRef<string | null>(null);
  // Tracks whether the graph has ever been fitted, independent of the
  // selection/search suppression below. The canvas has no `fitView` or
  // `defaultViewport` prop, so without an initial fit it renders at the raw
  // default viewport; a wide DAG mounting with a node already selected (e.g.
  // picked from the Nodes table while the graph card was still loading)
  // would otherwise never get fitted at all.
  const hasFittedOnceRef = useRef(false);

  useEffect(() => {
    setNodes(applyManualPositions(layout.nodes, manualPositions));
    setEdges(layout.edges);
  }, [layout, manualPositions, setEdges, setNodes]);

  // React Flow reports every node moved by the gesture, which is more than one
  // when several are selected together.
  const handleNodeDragStop = useCallback<OnNodeDrag<Node>>(
    (_event, node, draggedNodes) => {
      const moved = draggedNodes.length > 0 ? draggedNodes : [node];
      setManualPositions((previous) => {
        const next = new Map(previous);
        moved.forEach((dragged) => next.set(dragged.id, { ...dragged.position }));
        return next;
      });
    },
    [],
  );

  const resetLayout = useCallback(() => {
    setManualPositions(new Map());
    // The automatic fit is suppressed while a selection or search is active so
    // it never yanks the view; an explicit reset is the operator asking for the
    // full graph back, so it re-frames unconditionally.
    if (layout.nodes.length > 0) {
      fitView({ padding: 0.1, nodes: layout.nodes, maxZoom: 1.5, duration: 300 });
    }
  }, [fitView, layout.nodes]);

  const focusFullGraph = useCallback(() => {
    if (layout.nodes.length === 0) {
      lastAutoFocusKeyRef.current = null;
      return;
    }
    // Every fit after the first is suppressed while a search or a selection
    // is active, so the view is never yanked away from what the user is
    // looking at. The very first fit is exempt from that suppression so the
    // graph is not left cropped at the default viewport when it mounts with
    // a node already selected.
    if ((searchQuery.trim() || selectedNodeId) && hasFittedOnceRef.current) {
      lastAutoFocusKeyRef.current = null;
      return;
    }

    const autoFocusKey = layout.nodes.map((node) => node.id).sort().join('|');
    if (lastAutoFocusKeyRef.current === autoFocusKey) return;
    lastAutoFocusKeyRef.current = autoFocusKey;
    fitView({
      padding: 0.1,
      nodes: layout.nodes,
      maxZoom: 1.5,
      duration: 300,
    });
    hasFittedOnceRef.current = true;
  }, [fitView, layout.nodes, searchQuery, selectedNodeId]);

  useEffect(() => {
    focusFullGraph();
  }, [focusFullGraph]);

  const viewportRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    const el = viewportRef.current;
    if (!el || typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(() => {
      lastAutoFocusKeyRef.current = null;
      focusFullGraph();
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, [focusFullGraph]);

  useEffect(() => {
    if (!searchQuery.trim()) {
      lastSearchMatchRef.current = null;
      return;
    }

    const term = searchQuery.toLowerCase();
    const match = graphNodes.find((n) => n.node_id.toLowerCase().includes(term));
    if (!match) {
      lastSearchMatchRef.current = null;
      onNodeClick(null);
      return;
    }

    if (lastSearchMatchRef.current === match.node_id) return;
    // Selecting the match also expands its service (handled by the parent), so
    // a node hidden inside a collapsed service becomes visible. Until it is in
    // the layout, skip fitView; the layout change re-runs this effect.
    onNodeClick(match.node_id);
    const matchInLayout = layout.nodes.filter((n) => n.id === match.node_id);
    if (matchInLayout.length === 0) return;
    lastSearchMatchRef.current = match.node_id;
    fitView({
      padding: 0.3,
      nodes: matchInLayout,
      maxZoom: 1.4,
      duration: 400,
    });
  }, [fitView, graphNodes, layout.nodes, onNodeClick, searchQuery]);

  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      if (isServiceVertexId(node.id)) {
        onServiceClick?.(serviceFromVertexId(node.id));
        return;
      }
      onNodeClick(node.id === selectedNodeId ? null : node.id);
    },
    [onNodeClick, onServiceClick, selectedNodeId],
  );

  return (
    <>
      <div className="dag-search-strip">
        <input
          className="dag-search-input"
          placeholder="Search nodes…"
          value={searchQuery}
          onChange={(e) => {
            const next = e.target.value;
            // Emptying the search box drops the selection the search made.
            // This belongs on the change event rather than in an effect keyed
            // on the query: such an effect also runs on mount, where the query
            // is empty, and would clear a selection made elsewhere on the page.
            if (next === '' && searchQuery !== '') onNodeClick(null);
            setSearchQuery(next);
          }}
        />
        {/* Only offered once there is something to undo, so the strip stays a
            search bar until the operator has actually rearranged the canvas. */}
        {manualPositions.size > 0 && (
          <button className="btn btn--secondary" onClick={resetLayout} type="button">
            Reset layout
          </button>
        )}
      </div>

      <div className="dag-viewport" ref={viewportRef}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={handleNodeClick}
          onNodeDragStop={handleNodeDragStop}
          onMove={() => setZoomLevel(Math.round(getViewport().zoom * 100))}
          minZoom={0.2}
          maxZoom={2}
          proOptions={{ hideAttribution: true }}
        >
          <Background color="#e2e8f0" gap={24} />
          <MiniMap nodeColor={miniMapNodeColor} maskColor="rgba(241,245,249,0.7)" />
        </ReactFlow>

        <div className="dag-zoom-controls">
          <button
            className="dag-zoom-btn"
            onClick={() => {
              zoomIn();
              setZoomLevel(Math.round(getViewport().zoom * 100));
            }}
            title="Zoom in"
            type="button"
          >
            +
          </button>
          <button
            className="dag-zoom-btn"
            onClick={() => {
              zoomOut();
              setZoomLevel(Math.round(getViewport().zoom * 100));
            }}
            title="Zoom out"
            type="button"
          >
            -
          </button>
        </div>
        <div
          className="dag-zoom-level"
          style={{ position: 'absolute', bottom: 4, left: 14, zIndex: 10 }}
        >
          {zoomLevel}%
        </div>
      </div>
    </>
  );
}
