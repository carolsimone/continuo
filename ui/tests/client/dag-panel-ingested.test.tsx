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

// An ingested node is a table a node reads and no node builds. Its wire id has
// an empty service, so it starts with a dot.
const INGESTED_ID = '.raw.ingest_probe';
const READER_ID = 's.a.m';

const NODES: GraphNode[] = [
  { node_id: INGESTED_ID, node_type: 'ingested', schedule_name: 's' },
  { node_id: READER_ID, node_type: 'dbt-model', schedule_name: 's' },
];
const EDGES: GraphEdge[] = [{ from_node_id: INGESTED_ID, to_node_id: READER_ID }];

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

describe('DAGPanel ingested nodes', () => {
  it('draws an ingested node grey with a solid border, its schema in the label and an ingested chip', () => {
    const { container } = setup();
    const node = nodeIn(container, INGESTED_ID);
    expect(node.textContent).toContain('raw.ingest_probe');
    expect(node.querySelector('.dag-ingested-chip')?.textContent).toBe('ingested');
    expect(node.querySelector('[data-node-type-icon]')).toBeNull();
    expect(node.style.background).toBe('rgb(226, 232, 240)');
    expect(node.style.borderTopStyle).toBe('solid');
    expect(node.style.borderTopColor).toBe('rgb(148, 163, 184)');
  });

  it('carries no service accent on its left border, even when the empty service has a colour', () => {
    // The service view expands both services, so each node is drawn on its own,
    // and the colour map holds a colour for the empty service an ingested id
    // starts with: only the node type keeps the accent off the ingested node.
    const { container } = setup({
      serviceView: true,
      expandedServices: new Set(['', 's']),
      serviceColors: new Map([['', '#ff0000'], ['s', '#2563eb']]),
    });

    // A node of a real service in the same graph takes its service's accent.
    const reader = nodeIn(container, READER_ID);
    expect(reader.style.borderLeftColor).toBe('rgb(37, 99, 235)');
    expect(reader.style.borderLeftWidth).not.toBe(reader.style.borderTopWidth);

    const node = nodeIn(container, INGESTED_ID);
    expect(node.style.borderLeftStyle).toBe('solid');
    expect(node.style.borderLeftColor).toBe('rgb(148, 163, 184)');
    expect(node.style.borderLeftWidth).toBe(node.style.borderTopWidth);
  });

  it('stays solid when the panel colours by status, keeping the dashed look for nodes outside the run', () => {
    const { container } = setup({ colorByStatus: true });
    // A node with no task and no status is "not in the run on screen": dashed.
    expect(nodeIn(container, READER_ID).style.borderTopStyle).toBe('dashed');
    // An ingested node is in no run by nature, and is never drawn dashed.
    const node = nodeIn(container, INGESTED_ID);
    expect(node.style.borderTopStyle).toBe('solid');
    expect(node.style.background).toBe('rgb(226, 232, 240)');
    expect(node.style.borderTopColor).toBe('rgb(148, 163, 184)');
  });

  it('draws an ingested node grey in the minimap', () => {
    setup();
    // #e2e8f0 is the fill of an ingested node; the minimap paints it that grey.
    expect(minimap.nodeColor?.({ style: { background: '#e2e8f0' } } as unknown as Node)).toBe('#e2e8f0');
  });

  it('a click on an ingested node selects it and highlights the nodes it feeds', () => {
    const onNodeClick = vi.fn();
    const { container } = render(<SelectingPanel onNodeClick={onNodeClick} />);

    fireEvent.click(nodeIn(container, INGESTED_ID));
    expect(onNodeClick).toHaveBeenCalledWith(INGESTED_ID);

    // The selected role overrides the grey exactly as it does for any node.
    const selected = nodeIn(container, INGESTED_ID);
    expect(selected.style.borderTopColor).toBe('rgb(99, 102, 241)');
    expect(selected.style.background).toBe('rgb(238, 242, 255)');
    expect(selected.style.borderTopStyle).toBe('solid');
    // Its reader takes the child role.
    const reader = nodeIn(container, READER_ID);
    expect(reader.style.background).toBe('rgb(240, 253, 244)');
    expect(reader.style.borderTopColor).toBe('rgb(134, 239, 172)');
    // The chip is still on the node.
    expect(selected.querySelector('.dag-ingested-chip')?.textContent).toBe('ingested');
  });

  it('a click on the selected ingested node clears the selection', () => {
    const onNodeClick = vi.fn();
    const { container } = render(<SelectingPanel onNodeClick={onNodeClick} />);

    fireEvent.click(nodeIn(container, INGESTED_ID));
    fireEvent.click(nodeIn(container, INGESTED_ID));

    expect(onNodeClick).toHaveBeenLastCalledWith(null);
    expect(nodeIn(container, INGESTED_ID).style.background).toBe('rgb(226, 232, 240)');
  });

  it('shows an ingested node as a parent when its reader is selected, and dims an unrelated one', () => {
    const graphNodes: GraphNode[] = [
      ...NODES,
      { node_id: '.raw.other', node_type: 'ingested', schedule_name: 's' },
      { node_id: 's.b.n', node_type: 'dbt-model', schedule_name: 's' },
    ];
    const graphEdges: GraphEdge[] = [...EDGES, { from_node_id: '.raw.other', to_node_id: 's.b.n' }];
    const { container } = setup({ graphNodes, graphEdges, selectedNodeId: READER_ID });

    const parent = nodeIn(container, INGESTED_ID);
    expect(parent.style.background).toBe('rgb(238, 242, 255)');
    expect(parent.style.borderTopColor).toBe('rgb(165, 180, 252)');

    const unrelated = nodeIn(container, '.raw.other');
    expect(unrelated.style.opacity).toBe('0.2');
    expect(unrelated.style.background).toBe('rgb(255, 255, 255)');
  });

  it('other nodes keep the table-only label and no chip', () => {
    const { container } = setup();
    const reader = nodeIn(container, READER_ID);
    expect(reader.querySelector('.dag-ingested-chip')).toBeNull();
    expect(reader.querySelector('.dag-node-label > span')?.textContent).toBe('m');
    expect(reader.textContent).not.toContain('a.m');
    expect(reader.querySelector('[data-node-type-icon]')?.getAttribute('data-node-type-icon')).toBe('dbt');
  });
});
