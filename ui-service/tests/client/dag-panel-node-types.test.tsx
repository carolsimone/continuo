// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { ReactFlowProvider } from '@xyflow/react';
import DAGPanel from '../../src/client/DAGPanel';
import type { GraphNode, GraphEdge } from '../../src/client/types';

const TYPED_NODES: GraphNode[] = [
  { node_id: 's.sch.a', node_type: 'dbt-model', schedule_name: 's' },
  { node_id: 's.sch.b', node_type: 'python-model', schedule_name: 's' },
  { node_id: 's.sch.c', node_type: 'python-csv', schedule_name: 's' },
  { node_id: 's.sch.d', node_type: '', schedule_name: 's' },
];
const EDGES: GraphEdge[] = [
  { from_node_id: 's.sch.a', to_node_id: 's.sch.b' },
  { from_node_id: 's.sch.b', to_node_id: 's.sch.c' },
];

function setup(colorByStatus?: boolean) {
  return render(
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={TYPED_NODES}
        graphEdges={EDGES}
        tasks={[]}
        selectedNodeId={null}
        onNodeClick={() => {}}
        colorByStatus={colorByStatus}
      />
    </ReactFlowProvider>,
  );
}

function iconIn(container: HTMLElement, nodeId: string): string | null {
  const node = container.querySelector(`.react-flow__node[data-id="${nodeId}"]`);
  const icon = node?.querySelector('[data-node-type-icon]');
  return icon ? icon.getAttribute('data-node-type-icon') : null;
}

describe('DAGPanel node-type icons', () => {
  it('marks each node with its family icon in run mode', () => {
    const { container } = setup(true);
    expect(iconIn(container, 's.sch.a')).toBe('dbt');
    expect(iconIn(container, 's.sch.b')).toBe('python');
    expect(iconIn(container, 's.sch.c')).toBe('python-csv');
  });

  it('marks nodes identically in latest mode (colorByStatus=false)', () => {
    const { container } = setup(false);
    expect(iconIn(container, 's.sch.a')).toBe('dbt');
    expect(iconIn(container, 's.sch.c')).toBe('python-csv');
  });

  it('renders no icon for an empty node_type', () => {
    const { container } = setup(true);
    expect(iconIn(container, 's.sch.d')).toBeNull();
  });

  it('keeps the node label text', () => {
    const { container } = setup(true);
    const node = container.querySelector('.react-flow__node[data-id="s.sch.c"]') as HTMLElement;
    expect(node.textContent).toContain('c');
  });
});
