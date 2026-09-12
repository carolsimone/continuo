// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, fireEvent } from '@testing-library/react';
import RunSwimlane from '../../src/client/RunSwimlane';
import type { GraphNode, GraphEdge, Task } from '../../src/client/types';

const node = (id: string): GraphNode => ({ node_id: id, node_type: 'dbt-model', schedule_name: 's' });
const task = (id: string, status: string): Task => {
  const [service_name, schema_name, table_name] = id.split('.');
  return { task_id: id, service_name, schema_name, table_name, job_name: id, status, retry_count: 0, max_retries: 2, created_at: null };
};
const graph = {
  nodes: [node('core.a.seed'), node('finance.a.rev'), node('finance.a.bad')],
  edges: [{ from_node_id: 'core.a.seed', to_node_id: 'finance.a.rev' } as GraphEdge,
          { from_node_id: 'finance.a.rev', to_node_id: 'finance.a.bad' } as GraphEdge],
};
const tasks = [task('core.a.seed', 'succeeded'), task('finance.a.rev', 'succeeded'), task('finance.a.bad', 'failed')];

describe('RunSwimlane', () => {
  it('renders a lane per service and a node per graph node', () => {
    const { container, getByText } = render(
      <RunSwimlane graph={graph} tasks={tasks} serviceOrder={['core', 'finance']} collapsed={new Set()} onLaneToggle={vi.fn()} />,
    );
    expect(getByText('core')).toBeInTheDocument();
    expect(getByText('finance')).toBeInTheDocument();
    expect(container.querySelectorAll('.swim-node').length).toBe(3);
  });

  it('marks an edge into a failed node as hot', () => {
    const { container } = render(
      <RunSwimlane graph={graph} tasks={tasks} serviceOrder={['core', 'finance']} collapsed={new Set()} onLaneToggle={vi.fn()} />,
    );
    expect(container.querySelectorAll('path.swim-edge-hot').length).toBeGreaterThanOrEqual(1);
  });

  it('collapsing a lane switches its nodes to strip cells', () => {
    const onLaneToggle = vi.fn();
    const { container, getByText, rerender } = render(
      <RunSwimlane graph={graph} tasks={tasks} serviceOrder={['core', 'finance']} collapsed={new Set()} onLaneToggle={onLaneToggle} />,
    );
    fireEvent.click(getByText('finance').closest('[data-lane]')!);
    expect(onLaneToggle).toHaveBeenCalledWith('finance');
    rerender(
      <RunSwimlane graph={graph} tasks={tasks} serviceOrder={['core', 'finance']} collapsed={new Set(['finance'])} onLaneToggle={onLaneToggle} />,
    );
    expect(container.querySelectorAll('.swim-cell').length).toBeGreaterThanOrEqual(1);
  });
});
