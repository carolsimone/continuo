// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { ReactFlowProvider } from '@xyflow/react';
import DAGPanel from '../../src/client/DAGPanel';
import type { GraphNode, GraphEdge, Task } from '../../src/client/types';

function setup(props: {
  colorByStatus?: boolean;
  tasks: Task[];
  selectedNodeId?: string | null;
}) {
  const graphNodes: GraphNode[] = [
    { node_id: 's.sch.a', node_type: 'dbt-model', schedule_name: 's' },
    { node_id: 's.sch.b', node_type: 'dbt-model', schedule_name: 's' },
  ];
  const graphEdges: GraphEdge[] = [
    { from_node_id: 's.sch.a', to_node_id: 's.sch.b' },
  ];

  return render(
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={graphNodes}
        graphEdges={graphEdges}
        tasks={props.tasks}
        selectedNodeId={props.selectedNodeId ?? null}
        onNodeClick={() => {}}
        colorByStatus={props.colorByStatus}
      />
    </ReactFlowProvider>,
  );
}

const succeededTasks: Task[] = [
  {
    task_id: 's.sch.a', service_name: 's', schema_name: 'sch', table_name: 'a',
    job_name: '', status: 'succeeded', retry_count: 0, max_retries: 0, created_at: null,
  },
  {
    task_id: 's.sch.b', service_name: 's', schema_name: 'sch', table_name: 'b',
    job_name: '', status: 'succeeded', retry_count: 0, max_retries: 0, created_at: null,
  },
];

describe('DAGPanel colorByStatus', () => {
  it('paints nodes pending-style when colorByStatus=false even if matching tasks succeeded', () => {
    const { container } = setup({ colorByStatus: false, tasks: succeededTasks });
    const nodes = container.querySelectorAll('.react-flow__node');
    expect(nodes.length).toBeGreaterThan(0);
    nodes.forEach(n => {
      const style = (n as HTMLElement).style;
      // Pending style: white background, solid 1.5px grey border, no dashed (external) treatment.
      expect(style.background).toBe('rgb(255, 255, 255)');
      expect(style.border).toContain('solid');
      expect(style.border).toContain('rgb(209, 213, 219)');
    });
  });

  it('paints nodes succeeded-style when colorByStatus defaults to true', () => {
    const { container } = setup({ tasks: succeededTasks });
    const nodes = container.querySelectorAll('.react-flow__node');
    expect(nodes.length).toBeGreaterThan(0);
    nodes.forEach(n => {
      const style = (n as HTMLElement).style;
      // Succeeded style: light-green background #f0fdf4.
      expect(style.background).toBe('rgb(240, 253, 244)');
    });
  });

  it('still applies selected/dim focus roles when colorByStatus=false', () => {
    // Use a 3-node graph: a→b (child relationship), c is unrelated to a.
    // Selecting a: b becomes 'child', c becomes 'dim' — verifies focus roles
    // operate independently of colorByStatus.
    const graphNodes: GraphNode[] = [
      { node_id: 's.sch.a', node_type: 'dbt-model', schedule_name: 's' },
      { node_id: 's.sch.b', node_type: 'dbt-model', schedule_name: 's' },
      { node_id: 's.sch.c', node_type: 'dbt-model', schedule_name: 's' },
    ];
    const graphEdges: GraphEdge[] = [
      { from_node_id: 's.sch.a', to_node_id: 's.sch.b' },
    ];
    const tasks: Task[] = [
      ...succeededTasks,
      {
        task_id: 's.sch.c', service_name: 's', schema_name: 'sch', table_name: 'c',
        job_name: '', status: 'succeeded', retry_count: 0, max_retries: 0, created_at: null,
      },
    ];
    const { container } = render(
      <ReactFlowProvider>
        <DAGPanel
          graphNodes={graphNodes}
          graphEdges={graphEdges}
          tasks={tasks}
          selectedNodeId="s.sch.a"
          onNodeClick={() => {}}
          colorByStatus={false}
        />
      </ReactFlowProvider>,
    );
    const nodes = container.querySelectorAll('.react-flow__node');
    expect(nodes.length).toBe(3);

    const selected = container.querySelector('.react-flow__node[data-id="s.sch.a"]') as HTMLElement;
    const other = container.querySelector('.react-flow__node[data-id="s.sch.c"]') as HTMLElement;
    expect(selected).not.toBeNull();
    expect(other).not.toBeNull();

    // Selected node: indigo border (#6366f1 → rgb(99, 102, 241)) overriding neutral base.
    expect(selected.style.border).toContain('rgb(99, 102, 241)');
    // Non-selected, non-related node: dimmed to opacity 0.2.
    expect(other.style.opacity).toBe('0.2');
  });
});
