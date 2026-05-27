import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Background,
  Edge,
  MiniMap,
  Node,
  ReactFlow,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import * as dagre from 'dagre';
import { GraphEdge, GraphNode, Task } from './types';
import { resolveNodeStatus, taskNodeId } from './detail-page-helpers';

const NODE_WIDTH = 160;
const NODE_HEIGHT = 44;

type FocusRole = 'selected' | 'parent' | 'child' | 'dim' | null;

function nodeStyle(
  status: string,
  isExternal: boolean,
  role: FocusRole,
): React.CSSProperties {
  const base: React.CSSProperties = {
    borderRadius: 8,
    width: NODE_WIDTH,
    fontSize: 12,
    cursor: 'pointer',
    transition: 'opacity 0.15s, box-shadow 0.15s',
  };

  if (role === 'dim') {
    return { ...base, background: '#fff', border: '1px solid #e5e7eb', opacity: 0.2 };
  }

  const bgMap: Record<string, string> = {
    running: '#eef2ff',
    succeeded: '#f0fdf4',
    failed: '#fef2f2',
    pending: '#fff',
    cancelled: '#f8fafc',
    external: '#fafafa',
  };
  const borderColorMap: Record<string, string> = {
    running: '#a5b4fc',
    succeeded: '#86efac',
    failed: '#fca5a5',
    pending: '#d1d5db',
    cancelled: '#e2e8f0',
    external: '#9ca3af',
  };

  let bg = bgMap[status] ?? '#f3f4f6';
  let borderColor = borderColorMap[status] ?? '#d1d5db';
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
    border: `${borderWidth} ${borderStyleVal} ${borderColor}`,
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

function buildLayout(
  graphNodes: GraphNode[],
  graphEdges: GraphEdge[],
  tasks: Task[],
  selectedNodeId: string | null,
  colorByStatus: boolean,
): { nodes: Node[]; edges: Edge[] } {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: 'TB', nodesep: 40, ranksep: 60 });

  graphNodes.forEach((n) => g.setNode(n.node_id, { width: NODE_WIDTH, height: NODE_HEIGHT }));
  graphEdges.forEach((e) => g.setEdge(e.from_node_id, e.to_node_id));
  dagre.layout(g);

  const parentIds = selectedNodeId
    ? new Set(graphEdges.filter((e) => e.to_node_id === selectedNodeId).map((e) => e.from_node_id))
    : new Set<string>();
  const childIds = selectedNodeId
    ? new Set(graphEdges.filter((e) => e.from_node_id === selectedNodeId).map((e) => e.to_node_id))
    : new Set<string>();

  const nodes: Node[] = graphNodes.map((n) => {
    const pos = g.node(n.node_id);
    const task = tasks.find((candidate) => taskNodeId(candidate) === n.node_id);
    const status = colorByStatus ? resolveNodeStatus(n, tasks) : 'pending';
    const isExternal = colorByStatus ? !task && !n.status : false;

    let role: FocusRole = null;
    if (selectedNodeId) {
      if (n.node_id === selectedNodeId) role = 'selected';
      else if (parentIds.has(n.node_id)) role = 'parent';
      else if (childIds.has(n.node_id)) role = 'child';
      else role = 'dim';
    }

    return {
      id: n.node_id,
      position: { x: pos.x - NODE_WIDTH / 2, y: pos.y - NODE_HEIGHT / 2 },
      data: { label: n.node_id.split('.').pop() ?? n.node_id },
      style: nodeStyle(status, isExternal, role),
    };
  });

  const edges: Edge[] = graphEdges.map((e, i) => ({
    id: `e-${i}`,
    source: e.from_node_id,
    target: e.to_node_id,
    type: 'smoothstep',
    style: edgeStyle(e.from_node_id, e.to_node_id, selectedNodeId, parentIds, childIds),
  }));

  return { nodes, edges };
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
}

export default function DAGPanel({
  graphNodes,
  graphEdges,
  tasks,
  selectedNodeId,
  onNodeClick,
  colorByStatus = true,
}: Props) {
  const layout = useMemo(
    () => buildLayout(graphNodes, graphEdges, tasks, selectedNodeId, colorByStatus),
    [graphEdges, graphNodes, selectedNodeId, tasks, colorByStatus],
  );
  const [nodes, setNodes, onNodesChange] = useNodesState(layout.nodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(layout.edges);
  const { fitView, getViewport, zoomIn, zoomOut } = useReactFlow();

  const [zoomLevel, setZoomLevel] = useState(100);
  const [searchQuery, setSearchQuery] = useState('');
  const lastAutoFocusKeyRef = useRef<string | null>(null);
  const lastSearchMatchRef = useRef<string | null>(null);

  useEffect(() => {
    setNodes(layout.nodes);
    setEdges(layout.edges);
  }, [layout, setEdges, setNodes]);

  const focusFullGraph = useCallback(() => {
    if (searchQuery.trim() || selectedNodeId || layout.nodes.length === 0) {
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
    lastSearchMatchRef.current = match.node_id;
    onNodeClick(match.node_id);
    fitView({
      padding: 0.3,
      nodes: layout.nodes.filter((n) => n.id === match.node_id),
      maxZoom: 1.4,
      duration: 400,
    });
  }, [fitView, graphNodes, layout.nodes, onNodeClick, searchQuery]);

  useEffect(() => {
    if (searchQuery === '') onNodeClick(null);
  }, [onNodeClick, searchQuery]);

  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      onNodeClick(node.id === selectedNodeId ? null : node.id);
    },
    [onNodeClick, selectedNodeId],
  );

  return (
    <>
      <div className="dag-search-strip">
        <input
          className="dag-search-input"
          placeholder="Search nodes…"
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
        />
      </div>

      <div className="dag-viewport" ref={viewportRef}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={handleNodeClick}
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
