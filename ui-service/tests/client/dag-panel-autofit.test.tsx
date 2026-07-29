// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render } from '@testing-library/react';
import { ReactFlowProvider } from '@xyflow/react';
import DAGPanel from '../../src/client/DAGPanel';
import type { GraphNode, GraphEdge, Task } from '../../src/client/types';

// `useReactFlow` is mocked below to return a spied `fitView`, so import
// order matters here: `vi.mock` calls are hoisted above these `const`s by
// Vitest, and the spy factory must therefore live in `vi.hoisted`.
const { fitViewSpy } = vi.hoisted(() => ({
  fitViewSpy: vi.fn(),
}));

vi.mock('@xyflow/react', async () => {
  const actual = await vi.importActual<typeof import('@xyflow/react')>('@xyflow/react');
  return {
    ...actual,
    useReactFlow: () => ({
      fitView: fitViewSpy,
      getViewport: () => ({ x: 0, y: 0, zoom: 1 }),
      zoomIn: vi.fn(),
      zoomOut: vi.fn(),
    }),
  };
});

const graphNodes: GraphNode[] = [
  { node_id: 's.sch.a', node_type: 'dbt-model', schedule_name: 's' },
  { node_id: 's.sch.b', node_type: 'dbt-model', schedule_name: 's' },
];
const graphEdges: GraphEdge[] = [
  { from_node_id: 's.sch.a', to_node_id: 's.sch.b' },
];
const tasks: Task[] = [];

function panel(props: { selectedNodeId: string | null; onNodeClick?: (nodeId: string | null) => void }) {
  return (
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={graphNodes}
        graphEdges={graphEdges}
        tasks={tasks}
        selectedNodeId={props.selectedNodeId}
        onNodeClick={props.onNodeClick ?? (() => {})}
      />
    </ReactFlowProvider>
  );
}

describe('DAGPanel first auto-fit', () => {
  beforeEach(() => {
    fitViewSpy.mockClear();
  });

  it('fits the graph once on mount even when a node is already selected', () => {
    // Mirrors clicking a node in the Nodes table while the graph card is
    // still loading: the panel mounts with `selectedNodeId` already set.
    render(panel({ selectedNodeId: 's.sch.a' }));

    expect(fitViewSpy).toHaveBeenCalledTimes(1);
  });

  it('does not re-fit when a node is selected after the first fit has happened', () => {
    const { rerender } = render(panel({ selectedNodeId: null }));
    expect(fitViewSpy).toHaveBeenCalledTimes(1);

    rerender(panel({ selectedNodeId: 's.sch.a' }));

    expect(fitViewSpy).toHaveBeenCalledTimes(1);
  });
});
