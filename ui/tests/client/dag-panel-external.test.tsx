// @vitest-environment jsdom
import { useState } from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, fireEvent } from '@testing-library/react';
import { MiniMap, ReactFlowProvider, type Node } from '@xyflow/react';
import DAGPanel from '../../src/client/DAGPanel';
import type { GraphNode, GraphEdge } from '../../src/client/types';

// jsdom measures no node, so the real minimap draws none of them. The mock
// records the colour function the panel hands the minimap and renders the real
// minimap unchanged.
const minimap = vi.hoisted(() => ({ nodeColor: undefined as undefined | ((node: Node) => string) }));
vi.mock('@xyflow/react', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@xyflow/react')>();
  return {
    ...actual,
    MiniMap: (props: React.ComponentProps<typeof MiniMap>) => {
      minimap.nodeColor = props.nodeColor as (node: Node) => string;
      return <actual.MiniMap {...props} />;
    },
  };
});

// An unknown-external node is a table a node reads and no node builds. Its wire id has
// an empty service, so it starts with a dot.
const UNKNOWN_EXTERNAL_ID = '.raw.ingest_probe';
const READER_ID = 's.a.m';

const NODES: GraphNode[] = [
  { node_id: UNKNOWN_EXTERNAL_ID, node_type: 'unknown-external', schedule_name: 's', dormant: false, last_run_at: null },
  { node_id: READER_ID, node_type: 'dbt-model', schedule_name: 's', dormant: false, last_run_at: null },
];
const EDGES: GraphEdge[] = [{ from_node_id: UNKNOWN_EXTERNAL_ID, to_node_id: READER_ID }];

function nodeIn(container: HTMLElement, id: string): HTMLElement {
  return container.querySelector(`.react-flow__node[data-id="${id}"]`) as HTMLElement;
}

function setup(props: {
  colorByStatus?: boolean;
  selectedNodeId?: string | null;
  onNodeClick?: (nodeId: string | null) => void;
  graphNodes?: GraphNode[];
  graphEdges?: GraphEdge[];
  serviceView?: boolean;
  expandedServices?: Set<string>;
  serviceColors?: Map<string, string>;
} = {}) {
  return render(
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={props.graphNodes ?? NODES}
        graphEdges={props.graphEdges ?? EDGES}
        tasks={[]}
        selectedNodeId={props.selectedNodeId ?? null}
        onNodeClick={props.onNodeClick ?? (() => {})}
        colorByStatus={props.colorByStatus ?? false}
        serviceView={props.serviceView}
        expandedServices={props.expandedServices}
        serviceColors={props.serviceColors}
      />
    </ReactFlowProvider>,
  );
}

// Holds the selection the way the schedule page does: a click reports the node
// to the page, and the page hands the selection back to the panel.
function SelectingPanel({ onNodeClick }: { onNodeClick: (nodeId: string | null) => void }) {
  const [selected, setSelected] = useState<string | null>(null);
  return (
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={NODES}
        graphEdges={EDGES}
        tasks={[]}
        selectedNodeId={selected}
        onNodeClick={(nodeId) => {
          onNodeClick(nodeId);
          setSelected(nodeId);
        }}
        colorByStatus={false}
      />
    </ReactFlowProvider>
  );
}

describe('DAGPanel unknown-external nodes', () => {
  it('draws an unknown-external node grey with a solid border, its schema in the label and an external chip', () => {
    const { container } = setup();
    const node = nodeIn(container, UNKNOWN_EXTERNAL_ID);
    expect(node.textContent).toContain('raw.ingest_probe');
    expect(node.querySelector('.dag-node-chip--external')?.textContent).toBe('external');
    expect(node.querySelector('[data-node-type-icon]')).toBeNull();
    expect(node.style.background).toBe('rgb(226, 232, 240)');
    expect(node.style.borderTopStyle).toBe('solid');
    expect(node.style.borderTopColor).toBe('rgb(148, 163, 184)');
  });

  it('carries no service accent on its left border, even when the empty service has a colour', () => {
    // The service view expands both services, so each node is drawn on its own,
    // and the colour map holds a colour for the empty service an unknown-external id
    // starts with: only the node type keeps the accent off the unknown-external node.
    const { container } = setup({
      serviceView: true,
      expandedServices: new Set(['', 's']),
      serviceColors: new Map([['', '#ff0000'], ['s', '#2563eb']]),
    });

    // A node of a real service in the same graph takes its service's accent.
    const reader = nodeIn(container, READER_ID);
    expect(reader.style.borderLeftColor).toBe('rgb(37, 99, 235)');
    expect(reader.style.borderLeftWidth).not.toBe(reader.style.borderTopWidth);

    const node = nodeIn(container, UNKNOWN_EXTERNAL_ID);
    expect(node.style.borderLeftStyle).toBe('solid');
    expect(node.style.borderLeftColor).toBe('rgb(148, 163, 184)');
    expect(node.style.borderLeftWidth).toBe(node.style.borderTopWidth);
  });

  it('stays solid when the panel colours by status, keeping the dashed look for nodes outside the run', () => {
    const { container } = setup({ colorByStatus: true });
    // A node with no task and no status is "not in the run on screen": dashed.
    expect(nodeIn(container, READER_ID).style.borderTopStyle).toBe('dashed');
    // An unknown-external node is in no run by nature, and is never drawn dashed.
    const node = nodeIn(container, UNKNOWN_EXTERNAL_ID);
    expect(node.style.borderTopStyle).toBe('solid');
    expect(node.style.background).toBe('rgb(226, 232, 240)');
    expect(node.style.borderTopColor).toBe('rgb(148, 163, 184)');
  });

  it('says what an unknown-external node is when hovered', () => {
    const { container } = setup();
    expect(nodeIn(container, UNKNOWN_EXTERNAL_ID).getAttribute('title'))
      .toBe('A table continuo reads and does not build. continuo does not know who builds it.');
  });

  it('never draws a dormant or external node with a dashed border, even with colorByStatus', () => {
    const dormantId = 's.a.dormant_m';
    const graphNodes: GraphNode[] = [
      ...NODES,
      { node_id: dormantId, node_type: 'dbt-model', schedule_name: '', dormant: true, last_run_at: null },
    ];
    const graphEdges: GraphEdge[] = [...EDGES, { from_node_id: dormantId, to_node_id: READER_ID }];
    const { container } = setup({ colorByStatus: true, graphNodes, graphEdges });

    // The reader has no task in the run on screen, so it is dashed. Neither the
    // external node nor the dormant node has one either, yet both keep a solid
    // border of their own colour: a fixed look never takes the dashed border.
    expect(nodeIn(container, READER_ID).style.borderTopStyle).toBe('dashed');
    for (const id of [UNKNOWN_EXTERNAL_ID, dormantId]) {
      const node = nodeIn(container, id);
      expect(node.style.borderTopStyle).toBe('solid');
      expect(node.style.borderTopColor).toBe('rgb(148, 163, 184)');
    }
    expect(nodeIn(container, dormantId).style.background).toBe('rgb(241, 245, 249)');
  });

  it('draws an unknown-external node grey in the minimap', () => {
    setup();
    // #e2e8f0 is the fill of an unknown-external node; the minimap paints it that grey.
    expect(minimap.nodeColor?.({ style: { background: '#e2e8f0' } } as unknown as Node)).toBe('#e2e8f0');
  });

  it('a click on an unknown-external node selects it and highlights the nodes it feeds', () => {
    const onNodeClick = vi.fn();
    const { container } = render(<SelectingPanel onNodeClick={onNodeClick} />);

    fireEvent.click(nodeIn(container, UNKNOWN_EXTERNAL_ID));
    expect(onNodeClick).toHaveBeenCalledWith(UNKNOWN_EXTERNAL_ID);

    // The selected role overrides the grey exactly as it does for any node.
    const selected = nodeIn(container, UNKNOWN_EXTERNAL_ID);
    expect(selected.style.borderTopColor).toBe('rgb(99, 102, 241)');
    expect(selected.style.background).toBe('rgb(238, 242, 255)');
    expect(selected.style.borderTopStyle).toBe('solid');
    // Its reader takes the child role.
    const reader = nodeIn(container, READER_ID);
    expect(reader.style.background).toBe('rgb(240, 253, 244)');
    expect(reader.style.borderTopColor).toBe('rgb(134, 239, 172)');
    // The chip is still on the node.
    expect(selected.querySelector('.dag-node-chip--external')?.textContent).toBe('external');
  });

  it('a click on the selected unknown-external node clears the selection', () => {
    const onNodeClick = vi.fn();
    const { container } = render(<SelectingPanel onNodeClick={onNodeClick} />);

    fireEvent.click(nodeIn(container, UNKNOWN_EXTERNAL_ID));
    fireEvent.click(nodeIn(container, UNKNOWN_EXTERNAL_ID));

    expect(onNodeClick).toHaveBeenLastCalledWith(null);
    expect(nodeIn(container, UNKNOWN_EXTERNAL_ID).style.background).toBe('rgb(226, 232, 240)');
  });

  it('shows an unknown-external node as a parent when its reader is selected, and dims an unrelated one', () => {
    const graphNodes: GraphNode[] = [
      ...NODES,
      { node_id: '.raw.other', node_type: 'unknown-external', schedule_name: 's', dormant: false, last_run_at: null },
      { node_id: 's.b.n', node_type: 'dbt-model', schedule_name: 's', dormant: false, last_run_at: null },
    ];
    const graphEdges: GraphEdge[] = [...EDGES, { from_node_id: '.raw.other', to_node_id: 's.b.n' }];
    const { container } = setup({ graphNodes, graphEdges, selectedNodeId: READER_ID });

    const parent = nodeIn(container, UNKNOWN_EXTERNAL_ID);
    expect(parent.style.background).toBe('rgb(238, 242, 255)');
    expect(parent.style.borderTopColor).toBe('rgb(165, 180, 252)');

    const unrelated = nodeIn(container, '.raw.other');
    expect(unrelated.style.opacity).toBe('0.2');
    expect(unrelated.style.background).toBe('rgb(255, 255, 255)');
  });

  it('other nodes keep the table-only label and no chip', () => {
    const { container } = setup();
    const reader = nodeIn(container, READER_ID);
    expect(reader.querySelector('.dag-node-chip')).toBeNull();
    expect(reader.querySelector('.dag-node-label > span')?.textContent).toBe('m');
    expect(reader.textContent).not.toContain('a.m');
    expect(reader.querySelector('[data-node-type-icon]')?.getAttribute('data-node-type-icon')).toBe('dbt');
  });
});
