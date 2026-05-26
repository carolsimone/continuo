import { GraphNode, RunGraph, ScheduleGraph, Task } from './types';

export function taskNodeId(task: Task): string {
  return `${task.service_name}.${task.schema_name}.${task.table_name}`;
}

export function toScheduleGraph(runGraph: RunGraph | null): ScheduleGraph | null {
  if (!runGraph) return null;

  return {
    nodes: runGraph.nodes.map((node) => ({
      node_id: node.node_id,
      node_type: node.node_type,
      schedule_name: node.schedule_name,
      status: node.status,
    })),
    edges: runGraph.edges,
  };
}

export function resolveNodeStatus(node: GraphNode, tasks: Task[]): string {
  const task = tasks.find((candidate) => taskNodeId(candidate) === node.node_id);
  return task?.status ?? node.status ?? 'external';
}

interface ResolveActiveGraphArgs {
  mode?: 'run' | 'latest';
  scheduleGraph: ScheduleGraph | null;
  liveRunGraph: RunGraph | null;
  selectedRunGraph: RunGraph | null;
  selectedRunId: string | null;
}

export function resolveActiveGraph({
  mode,
  scheduleGraph,
  liveRunGraph,
  selectedRunGraph,
  selectedRunId,
}: ResolveActiveGraphArgs): ScheduleGraph | null {
  if (mode === 'latest') return scheduleGraph;
  if (selectedRunId) {
    return toScheduleGraph(selectedRunGraph);
  }

  return toScheduleGraph(liveRunGraph) ?? scheduleGraph;
}

export function parseNodeId(nodeId: string): {
  service_name: string;
  schema_name: string;
  table_name: string;
} {
  const [service_name = '', schema_name = '', table_name = ''] = nodeId.split('.');
  return { service_name, schema_name, table_name };
}
