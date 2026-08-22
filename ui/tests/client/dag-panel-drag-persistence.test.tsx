// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { ReactFlowProvider } from '@xyflow/react';
import type { Node } from '@xyflow/react';
import DAGPanel from '../../src/client/DAGPanel';
import { serviceVertexId } from '../../src/client/service-helpers';
import type { GraphNode, GraphEdge, Task } from '../../src/client/types';

// The panel's drag handling is exercised through the props it hands to
// `ReactFlow`: `onNodeDragStop` is what React Flow calls once the pointer is
// released, carrying the node with its dropped position. Wrapping the real
// component (rather than replacing it) keeps the canvas rendering genuine, so
// the assertions below read the actual `transform` React Flow paints.
const { capturedFlowProps } = vi.hoisted(() => ({
  capturedFlowProps: { current: null as Record<string, never> | null },
}));

vi.mock('@xyflow/react', async () => {
  const actual = await vi.importActual<typeof import('@xyflow/react')>('@xyflow/react');
  return {
    ...actual,
    ReactFlow: (props: Record<string, never>) => {
      capturedFlowProps.current = props;
      return <actual.ReactFlow {...props} />;
    },
  };
});

interface FlowProps {
  nodes: Node[];
  onNodeDragStop: (event: unknown, node: Node, nodes: Node[]) => void;
}

function flowProps(): FlowProps {
  if (!capturedFlowProps.current) throw new Error('ReactFlow was never rendered');
  return capturedFlowProps.current as unknown as FlowProps;
}

// Each call returns fresh arrays with identical content, mirroring what the
// 5-second graph poll on DetailPage produces: the same topology arriving as
// new object identities.
function freshGraphNodes(): GraphNode[] {
  return [
    { node_id: 'core.sch.a', node_type: 'dbt-model', schedule_name: 's' },
    { node_id: 'core.sch.b', node_type: 'dbt-model', schedule_name: 's' },
    { node_id: 'marketing.sch.c', node_type: 'dbt-model', schedule_name: 's' },
  ];
}

function freshGraphEdges(): GraphEdge[] {
  return [
    { from_node_id: 'core.sch.a', to_node_id: 'core.sch.b' },
    { from_node_id: 'core.sch.b', to_node_id: 'marketing.sch.c' },
  ];
}

interface PanelOptions {
  selectedNodeId?: string | null;
  tasks?: Task[];
  serviceView?: boolean;
}

function panel(options: PanelOptions = {}) {
  return (
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={freshGraphNodes()}
        graphEdges={freshGraphEdges()}
        tasks={options.tasks ?? []}
        selectedNodeId={options.selectedNodeId ?? null}
        onNodeClick={() => {}}
        serviceView={options.serviceView}
        expandedServices={options.serviceView ? new Set<string>() : undefined}
        onServiceClick={() => {}}
      />
    </ReactFlowProvider>
  );
}

function transformOf(container: HTMLElement, nodeId: string): string {
  const el = container.querySelector(`[data-id="${nodeId}"]`);
  if (!el) throw new Error(`node ${nodeId} is not on the canvas`);
  return (el as HTMLElement).style.transform;
}

function dragTo(nodeId: string, x: number, y: number): void {
  const props = flowProps();
  const node = props.nodes.find((candidate) => candidate.id === nodeId);
  if (!node) throw new Error(`node ${nodeId} is not in the flow`);
  const dropped = { ...node, position: { x, y } };
  act(() => {
    props.onNodeDragStop({}, dropped, [dropped]);
  });
}

describe('DAGPanel manual node placement', () => {
  beforeEach(() => {
    capturedFlowProps.current = null;
  });

  it('keeps a dragged node where it was dropped when the polled graph refreshes', () => {
    const { container, rerender } = render(panel());
    const computed = transformOf(container, 'core.sch.b');

    dragTo('core.sch.b', 640, 320);
    expect(transformOf(container, 'core.sch.b')).toBe('translate(640px,320px)');
    expect(transformOf(container, 'core.sch.b')).not.toBe(computed);

    // The poll: same topology, new array identities.
    rerender(panel());

    expect(transformOf(container, 'core.sch.b')).toBe('translate(640px,320px)');
  });

  it('keeps a dragged node in place when task statuses change', () => {
    const { container, rerender } = render(panel());

    dragTo('marketing.sch.c', -120, 500);
    expect(transformOf(container, 'marketing.sch.c')).toBe('translate(-120px,500px)');

    rerender(
      panel({
        tasks: [
          {
            task_id: 'marketing.sch.c',
            service_name: 'marketing',
            schema_name: 'sch',
            table_name: 'c',
            job_name: '',
            status: 'failed',
            retry_count: 0,
            max_retries: 0,
            created_at: null,
          },
        ],
      }),
    );

    expect(transformOf(container, 'marketing.sch.c')).toBe('translate(-120px,500px)');
  });

  it('keeps a dragged node in place when the selection changes', () => {
    const { container, rerender } = render(panel());

    dragTo('core.sch.a', 42, 84);
    rerender(panel({ selectedNodeId: 'marketing.sch.c' }));

    expect(transformOf(container, 'core.sch.a')).toBe('translate(42px,84px)');
  });

  it('keeps a dragged service vertex in place across a refresh', () => {
    const { container, rerender } = render(panel({ serviceView: true }));
    const vertexId = serviceVertexId('marketing');

    dragTo(vertexId, 700, 40);
    rerender(panel({ serviceView: true }));

    expect(transformOf(container, vertexId)).toBe('translate(700px,40px)');
  });

  it('leaves nodes the user has not moved on the computed layout', () => {
    const { container, rerender } = render(panel());
    const untouched = transformOf(container, 'marketing.sch.c');

    dragTo('core.sch.b', 640, 320);
    rerender(panel());

    expect(transformOf(container, 'marketing.sch.c')).toBe(untouched);
  });

  it('offers Reset layout only once a node has been moved, and restores the computed layout', () => {
    const { container } = render(panel());
    const computed = transformOf(container, 'core.sch.b');

    expect(screen.queryByRole('button', { name: /reset layout/i })).toBeNull();

    dragTo('core.sch.b', 640, 320);
    const reset = screen.getByRole('button', { name: /reset layout/i });

    fireEvent.click(reset);

    expect(transformOf(container, 'core.sch.b')).toBe(computed);
    expect(screen.queryByRole('button', { name: /reset layout/i })).toBeNull();
  });
});
