import { ReactFlowProvider } from '@xyflow/react';
import DAGPanel from './DAGPanel';
import type { ScheduleGraph } from './types';
import { listServices } from './service-helpers';

// Renders a schedule's dependency graph as pure structure: every node at its
// model level (no collapsed service vertices), no run-status coloring, and no
// selection or service-toggle interaction. Used wherever the topology itself
// — not a run's outcome — is the thing being shown.
export default function TopologyPanel({ graph }: { graph: ScheduleGraph }) {
  const expandedServices = new Set(listServices(graph.nodes, []));

  return (
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={graph.nodes}
        graphEdges={graph.edges}
        tasks={[]}
        selectedNodeId={null}
        onNodeClick={() => {}}
        colorByStatus={false}
        serviceView={false}
        expandedServices={expandedServices}
        onServiceClick={() => {}}
      />
    </ReactFlowProvider>
  );
}
