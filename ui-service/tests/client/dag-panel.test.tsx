// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { ReactFlowProvider } from '@xyflow/react';
import DAGPanel from '../../src/client/DAGPanel';
import type { GraphNode, GraphEdge, Task } from '../../src/client/types';

function setup(props: {
  colorByStatus?: boolean;
  tasks: Task[];
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
        selectedNodeId={null}
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
});
