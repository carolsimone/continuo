// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, fireEvent } from '@testing-library/react';
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
      expect(style.borderStyle).toBe('solid');
      expect(style.borderColor).toBe('rgb(209, 213, 219)');
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
    expect(selected.style.borderColor).toBe('rgb(99, 102, 241)');
    // Non-selected, non-related node: dimmed to opacity 0.2.
    expect(other.style.opacity).toBe('0.2');
  });
});

// ---- Service-level view ----

const SVC_NODES: GraphNode[] = [
  { node_id: 'svc-a.sch.n1', node_type: 'dbt-model', schedule_name: 's' },
  { node_id: 'svc-a.sch.n2', node_type: 'dbt-model', schedule_name: 's' },
  { node_id: 'svc-b.sch.m1', node_type: 'dbt-model', schedule_name: 's' },
];
const SVC_EDGES: GraphEdge[] = [
  { from_node_id: 'svc-a.sch.n1', to_node_id: 'svc-a.sch.n2' },
  { from_node_id: 'svc-a.sch.n2', to_node_id: 'svc-b.sch.m1' },
];
const SVC_TASKS: Task[] = [
  {
    task_id: 'svc-a.sch.n1', service_name: 'svc-a', schema_name: 'sch', table_name: 'n1',
    job_name: '', status: 'failed', retry_count: 0, max_retries: 0, created_at: null,
  },
  {
    task_id: 'svc-a.sch.n2', service_name: 'svc-a', schema_name: 'sch', table_name: 'n2',
    job_name: '', status: 'succeeded', retry_count: 0, max_retries: 0, created_at: null,
  },
  {
    task_id: 'svc-b.sch.m1', service_name: 'svc-b', schema_name: 'sch', table_name: 'm1',
    job_name: '', status: 'succeeded', retry_count: 0, max_retries: 0, created_at: null,
  },
];

function setupServiceView(expanded: Set<string>, onServiceClick = () => {}) {
  const serviceColors = new Map([['svc-a', '#0ea5e9'], ['svc-b', '#f59e0b']]);
  return render(
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={SVC_NODES}
        graphEdges={SVC_EDGES}
        tasks={SVC_TASKS}
        selectedNodeId={null}
        onNodeClick={() => {}}
        serviceView
        expandedServices={expanded}
        onServiceClick={onServiceClick}
        serviceColors={serviceColors}
      />
    </ReactFlowProvider>,
  );
}

describe('DAGPanel service view', () => {
  it('renders one vertex per collapsed service and no model nodes by default', () => {
    const { container } = setupServiceView(new Set());
    const ids = [...container.querySelectorAll('.react-flow__node')].map(n => n.getAttribute('data-id'));
    expect(ids.sort()).toEqual(['svc:svc-a', 'svc:svc-b']);
  });

  it('paints a collapsed service vertex with its rolled-up status (failed wins)', () => {
    const { container } = setupServiceView(new Set());
    const vertexA = container.querySelector('.react-flow__node[data-id="svc:svc-a"]') as HTMLElement;
    // failed background #fef2f2
    expect(vertexA.style.background).toBe('rgb(254, 242, 242)');
    const vertexB = container.querySelector('.react-flow__node[data-id="svc:svc-b"]') as HTMLElement;
    // succeeded background #f0fdf4
    expect(vertexB.style.background).toBe('rgb(240, 253, 244)');
  });

  it('shows the service name and node count on the vertex', () => {
    const { container } = setupServiceView(new Set());
    const vertexA = container.querySelector('.react-flow__node[data-id="svc:svc-a"]') as HTMLElement;
    expect(vertexA.textContent).toContain('svc-a');
    expect(vertexA.textContent).toContain('2');
  });

  it('reveals a service\'s model nodes when it is expanded, keeping others collapsed', () => {
    const { container } = setupServiceView(new Set(['svc-a']));
    const ids = [...container.querySelectorAll('.react-flow__node')].map(n => n.getAttribute('data-id'));
    expect(ids.sort()).toEqual(['svc-a.sch.n1', 'svc-a.sch.n2', 'svc:svc-b']);
  });

  it('invokes onServiceClick with the service name when its vertex is clicked', () => {
    const onServiceClick = vi.fn();
    const { container } = setupServiceView(new Set(), onServiceClick);
    const vertexA = container.querySelector('.react-flow__node[data-id="svc:svc-a"]') as HTMLElement;
    fireEvent.click(vertexA);
    expect(onServiceClick).toHaveBeenCalledWith('svc-a');
  });

  it('keeps the service accent on a selected model node (longhand border regression)', () => {
    // The focus role rewrites the border; the left service accent must
    // survive it. A `border` shorthand mixed with a `borderLeft` longhand
    // used to drop the accent on re-render.
    const serviceColors = new Map([['svc-a', '#0ea5e9'], ['svc-b', '#f59e0b']]);
    const { container } = render(
      <ReactFlowProvider>
        <DAGPanel
          graphNodes={SVC_NODES}
          graphEdges={SVC_EDGES}
          tasks={SVC_TASKS}
          selectedNodeId="svc-a.sch.n1"
          onNodeClick={() => {}}
          serviceView
          expandedServices={new Set(['svc-a'])}
          onServiceClick={() => {}}
          serviceColors={serviceColors}
        />
      </ReactFlowProvider>,
    );
    const selected = container.querySelector('.react-flow__node[data-id="svc-a.sch.n1"]') as HTMLElement;
    // Assert the three non-accent sides individually: jsdom serializes the
    // `border-color` shorthand getter as the 4-value form when the left side
    // differs, so checking the shorthand would compare against the expansion.
    expect(selected.style.borderTopColor).toBe('rgb(99, 102, 241)'); // focus indigo
    expect(selected.style.borderRightColor).toBe('rgb(99, 102, 241)'); // focus indigo
    expect(selected.style.borderBottomColor).toBe('rgb(99, 102, 241)'); // focus indigo
    expect(selected.style.borderLeftColor).toBe('rgb(14, 165, 233)'); // svc-a accent survives
    // A dimmed collapsed vertex keeps its accent too.
    const dimVertex = container.querySelector('.react-flow__node[data-id="svc:svc-b"]') as HTMLElement;
    expect(dimVertex.style.opacity).toBe('0.2');
    expect(dimVertex.style.borderLeftColor).toBe('rgb(245, 158, 11)'); // svc-b accent
  });

  it('lays out a cyclic service graph without crashing', () => {
    // svc-a→svc-b via n1→m1 and svc-b→svc-a via m1→n2: the collapsed service
    // graph is a 2-cycle even though the model graph is a DAG.
    const cyclicEdges: GraphEdge[] = [
      { from_node_id: 'svc-a.sch.n1', to_node_id: 'svc-b.sch.m1' },
      { from_node_id: 'svc-b.sch.m1', to_node_id: 'svc-a.sch.n2' },
    ];
    const { container } = render(
      <ReactFlowProvider>
        <DAGPanel
          graphNodes={SVC_NODES}
          graphEdges={cyclicEdges}
          tasks={SVC_TASKS}
          selectedNodeId={null}
          onNodeClick={() => {}}
          serviceView
          expandedServices={new Set()}
          onServiceClick={() => {}}
          serviceColors={new Map()}
        />
      </ReactFlowProvider>,
    );
    const ids = [...container.querySelectorAll('.react-flow__node')].map(n => n.getAttribute('data-id'));
    expect(ids.sort()).toEqual(['svc:svc-a', 'svc:svc-b']);
  });
});
