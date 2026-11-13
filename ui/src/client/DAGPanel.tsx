import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Background,
  Edge,
  MiniMap,
  Node,
  OnMove,
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
import NodeTypeIcon, { isExternalNodeType } from './NodeTypeIcon';
import { externalNodeName, parseNodeId, resolveNodeStatus, taskNodeId } from './detail-page-helpers';
import { dormantTitle } from './node-helpers';
import { useSingleNodeRun } from './useSingleNodeRun';
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
  dispatched: '#fff',
  queued: '#f8fafc',
  stopping: '#fffbeb',
  running: '#eef2ff',
  succeeded: '#f0fdf4',
  failed: '#fef2f2',
  pending: '#fff',
  cancelled: '#f8fafc',
  skipped: '#f1f5f9',
  external: '#fafafa',
};
const STATUS_BORDER: Record<string, string> = {
  dispatched: '#cbd5e1',
  queued: '#94a3b8',
  stopping: '#fcd34d',
  running: '#a5b4fc',
  succeeded: '#86efac',
  failed: '#fca5a5',
  pending: '#d1d5db',
  cancelled: '#e2e8f0',
  skipped: '#cbd5e1',
  external: '#9ca3af',
};

// An unknown-external node and a dormant node belong to no schedule, and each
// carries one fixed look of its own in place of a run status, with a solid
// border: the dashed border means "a node of the graph that is not in the run
// on screen". The schedule's latest view draws this panel with no run status.
// In a run's own view (RunSwimlane, RunNodeTable), a dormant view that the run
// rebuilds below a seed is painted by its task's status.
interface FixedLook {
  background: string;
  borderColor: string;
  color?: string;
}

// An unknown-external node (a table another system loads, which no node builds) has no
// run status and no service: grey.
const UNKNOWN_EXTERNAL_BG = '#e2e8f0';
const UNKNOWN_EXTERNAL_LOOK: FixedLook = { background: UNKNOWN_EXTERNAL_BG, borderColor: '#94a3b8' };
const UNKNOWN_EXTERNAL_TITLE = 'A table continuo reads and does not build. continuo does not know who builds it.';

// A dormant node (one continuo builds that has no schedule tag): light grey,
// with muted text.
const DORMANT_BG = '#f1f5f9';
const DORMANT_LOOK: FixedLook = { background: DORMANT_BG, borderColor: '#94a3b8', color: '#475569' };

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
  look: FixedLook | null,
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

  // The dashed border means "a node of the graph that is not in the run on
  // screen"; a node with a fixed look is never drawn with it, whatever its status.
  let bg = look ? look.background : (STATUS_BG[status] ?? '#f3f4f6');
  let borderColor = look ? look.borderColor : (STATUS_BORDER[status] ?? '#d1d5db');
  let borderWidth = '1.5px';
  let borderStyleVal = isExternal && !look ? 'dashed' : 'solid';
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
    ...(look?.color ? { color: look.color } : {}),
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

// An unknown-external node is named with its schema, the only way to tell two such
// tables with the same name apart, and carries a chip that says what it is.
function unknownExternalLabel(nodeId: string): React.ReactNode {
  return (
    <div className="dag-node-label">
      <span>{externalNodeName(nodeId)}</span>
      <span className="dag-node-chip dag-node-chip--external">external</span>
    </div>
  );
}

// Reports why the run a ▶ asked for did not start, or null when a press starts
// another one.
type RunErrorHandler = (nodeId: string, message: string | null) => void;

// The ▶ on a dormant node starts a run of that node alone, from the live
// release, as the node page's "Run this node" does. The graph names no run by
// its id: a run an unanswered press made shows on it only as a later last-run
// time, which the graph's poll may bring before the press's error, so the
// operator has not seen it. A press therefore observes no run, and a repeated
// press sends the first one's request id until the server has given its final
// answer. The button is a control of its own on the node: its press neither
// selects nor drags the node.
function DormantRunButton({
  nodeId,
  onRunError,
}: {
  nodeId: string;
  onRunError: RunErrorHandler;
}) {
  const { service_name, schema_name, table_name } = parseNodeId(nodeId);
  const { runState, postRun } = useSingleNodeRun(
    { service: service_name, schema: schema_name, table: table_name },
    { operation: 'run' },
  );
  const className = [
    'btn',
    'btn--secondary',
    'btn--icon',
    'nodrag',
    runState === 'loading' ? 'is-loading' : '',
    runState === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');
  return (
    <button
      type="button"
      className={className}
      disabled={runState === 'loading' || runState === 'success'}
      aria-label={`Run ${table_name}`}
      title={`Run ${table_name} by hand`}
      onClick={(e) => {
        e.stopPropagation();
        onRunError(nodeId, null);
        void postRun({}).then((message) => {
          if (message !== null) onRunError(nodeId, message);
        });
      }}
      onKeyDown={(e) => e.stopPropagation()}
    >
      ▶
    </button>
  );
}

// A dormant node keeps its type icon and table name, and carries a chip that
// says what it is and the ▶ that runs it.
function dormantLabel(node: GraphNode, onRunError: RunErrorHandler): React.ReactNode {
  return (
    <div className="dag-node-label">
      <NodeTypeIcon nodeType={node.node_type} />
      <span>{parseNodeId(node.node_id).table_name || node.node_id}</span>
      <span className="dag-node-chip dag-node-chip--dormant">dormant</span>
      <DormantRunButton nodeId={node.node_id} onRunError={onRunError} />
    </div>
  );
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
  onRunError: RunErrorHandler,
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
    // The type decides first: an unknown-external node's id has an empty service
    // (".raw.ingest_probe"), so no service accent or lookup may be derived
    // from it.
    const unknownExternal = isExternalNodeType(n.node_type);
    const dormant = !unknownExternal && n.dormant;
    const task = tasks.find((candidate) => taskNodeId(candidate) === n.node_id);
    const status = colorByStatus ? resolveNodeStatus(n, tasks) : 'pending';
    const isExternal = colorByStatus ? !task && !n.status : false;
    const role = focusRole(n.node_id, selectedNodeId, parentIds, childIds);
    const accent = !unknownExternal && serviceView ? serviceColors.get(serviceOfNode(n.node_id)) : undefined;
    const look = unknownExternal ? UNKNOWN_EXTERNAL_LOOK : dormant ? DORMANT_LOOK : null;

    let label: React.ReactNode;
    let title: string | undefined;
    if (unknownExternal) {
      label = unknownExternalLabel(n.node_id);
      title = UNKNOWN_EXTERNAL_TITLE;
    } else if (dormant) {
      label = dormantLabel(n, onRunError);
      title = dormantTitle(n.last_run_at);
    } else {
      label = (
        <div className="dag-node-label">
          <NodeTypeIcon nodeType={n.node_type} />
          <span>{parseNodeId(n.node_id).table_name || n.node_id}</span>
        </div>
      );
    }

    return {
      id: n.node_id,
      position: { x: pos.x - NODE_WIDTH / 2, y: pos.y - NODE_HEIGHT / 2 },
      data: { label, dormant },
      ...(title ? { domAttributes: { title } } : {}),
      style: nodeStyle(status, isExternal, role, look, accent),
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
  if (bg === UNKNOWN_EXTERNAL_BG) return UNKNOWN_EXTERNAL_BG;
  // A skipped node shares the dormant fill; only a dormant node keeps it here.
  if (bg === DORMANT_BG && node.data?.dormant === true) return DORMANT_BG;
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
  // Why the run the latest ▶ press asked for did not start, named by its node;
  // null while no press has failed, and once another press is made.
  const [runError, setRunError] = useState<{ nodeId: string; message: string } | null>(null);
  const handleRunError = useCallback<RunErrorHandler>((nodeId, message) => {
    setRunError(message === null ? null : { nodeId, message });
  }, []);
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
        handleRunError,
      ),
    [graphEdges, graphNodes, selectedNodeId, tasks, colorByStatus, serviceView, expandedServices, colors, handleRunError],
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
  // Set once the operator zooms or pans the canvas themselves. From then on
  // the panel stops re-framing the graph for them: the automatic fit exists to
  // give a sensible starting view, not to overrule the one they chose. Without
  // this, a container resize or a cleared selection re-fits and throws away the
  // zoom seconds after it was set.
  const [viewportPinned, setViewportPinned] = useState(false);
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

  // A user gesture arrives with the DOM event that caused it; a move the panel
  // made itself (fitView, the zoom buttons) arrives with a null event. Only the
  // former means the operator has chosen their own framing.
  const handleMove = useCallback<OnMove>((event, viewport) => {
    setZoomLevel(Math.round(viewport.zoom * 100));
    if (event) setViewportPinned(true);
  }, []);

  const resetLayout = useCallback(() => {
    setManualPositions(new Map());
    setViewportPinned(false);
    // The automatic fit is suppressed while a selection or search is active, and
    // once the operator has framed the canvas themselves; an explicit reset is
    // them asking for the default back, so it re-frames unconditionally.
    if (layout.nodes.length > 0) {
      fitView({ padding: 0.1, nodes: layout.nodes, maxZoom: 1.5, duration: 300 });
    }
  }, [fitView, layout.nodes]);

  const focusFullGraph = useCallback(() => {
    if (layout.nodes.length === 0) {
      lastAutoFocusKeyRef.current = null;
      return;
    }
    // Leave a hand-set viewport alone. The key is deliberately not cleared
    // here: clearing it is what arms the next automatic fit, and there must
    // not be one until the operator resets the view.
    if (viewportPinned) return;
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
  }, [fitView, layout.nodes, searchQuery, selectedNodeId, viewportPinned]);

  useEffect(() => {
    focusFullGraph();
  }, [focusFullGraph]);

  const viewportRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    const el = viewportRef.current;
    if (!el || typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(() => {
      // A resize normally re-frames the graph, since the old fit was computed
      // against a container that no longer exists. Once the operator has framed
      // the canvas themselves that no longer applies, and clearing the key here
      // would arm a fit that undoes their zoom.
      if (viewportPinned) return;
      lastAutoFocusKeyRef.current = null;
      focusFullGraph();
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, [focusFullGraph, viewportPinned]);

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
            search bar until the operator has actually rearranged the canvas or
            framed it themselves. */}
        {(manualPositions.size > 0 || viewportPinned) && (
          <button className="btn btn--secondary" onClick={resetLayout} type="button">
            Reset layout
          </button>
        )}
      </div>

      {runError && (
        <div className="info-strip info-strip--error dag-run-error" role="alert">
          {`Could not run ${parseNodeId(runError.nodeId).table_name}: ${runError.message}`}
        </div>
      )}

      <div className="dag-viewport" ref={viewportRef}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={handleNodeClick}
          onNodeDragStop={handleNodeDragStop}
          onMove={handleMove}
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
              setViewportPinned(true);
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
              setViewportPinned(true);
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
