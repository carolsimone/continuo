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

  it('renders a graph node with no matching task as pending', () => {
    const g = { nodes: [node('core.a.orphan')], edges: [] as GraphEdge[] };
    const { container } = render(
      <RunSwimlane graph={g} tasks={[]} serviceOrder={['core']} collapsed={new Set()} onLaneToggle={vi.fn()} />,
    );
    expect(container.querySelectorAll('.swim-node.pend').length).toBe(1);
  });

  it('lays out a zero-edge graph with every node at depth 0 instead of throwing', () => {
    const g = { nodes: [node('core.a.x'), node('core.a.y')], edges: [] as GraphEdge[] };
    const { container } = render(
      <RunSwimlane graph={g} tasks={[]} serviceOrder={['core']} collapsed={new Set()} onLaneToggle={vi.fn()} />,
    );
    const nodes = container.querySelectorAll<HTMLElement>('.swim-node');
    expect(nodes.length).toBe(2);
    const lefts = new Set(Array.from(nodes).map((el) => el.style.left));
    expect(lefts.size).toBe(1);
    expect(Number.isFinite(parseFloat(Array.from(lefts)[0]))).toBe(true);
    expect(container.querySelectorAll('.swim-node.pend').length).toBe(2);
  });

  it('paints a cancelled node with the cancel class, not pending', () => {
    const g = { nodes: [node('core.a.stopped')], edges: [] as GraphEdge[] };
    const { container } = render(
      <RunSwimlane graph={g} tasks={[task('core.a.stopped', 'cancelled')]} serviceOrder={['core']} collapsed={new Set()} onLaneToggle={vi.fn()} />,
    );
    expect(container.querySelector('.swim-node.cancel')).toBeTruthy();
    expect(container.querySelector('.swim-node.pend')).toBeNull();
  });

  it('anchors an edge to a collapsed source cell, not 120px past it where the box would be', () => {
    const g = {
      nodes: [node('core.a.up'), node('finance.a.down')],
      edges: [{ from_node_id: 'core.a.up', to_node_id: 'finance.a.down' } as GraphEdge],
    };
    const { container } = render(
      <RunSwimlane graph={g} tasks={[]} serviceOrder={['core', 'finance']} collapsed={new Set(['core'])} onLaneToggle={vi.fn()} />,
    );
    const cell = container.querySelector<HTMLElement>('.swim-cell')!;
    const cellLeft = parseFloat(cell.style.left); // collapsed source is a 14px cell
    const d = container.querySelector('path.swim-edge, path.swim-edge-hot')!.getAttribute('d')!;
    const startX = parseFloat(d.match(/^M([\d.]+),/)![1]);
    // the edge must start at the cell's right edge (cellLeft + 14), not cellLeft + 134
    expect(startX).toBeGreaterThanOrEqual(cellLeft + 14 - 1);
    expect(startX).toBeLessThanOrEqual(cellLeft + 14 + 1);
  });

  it('starts the node columns after the widest lane label instead of under it', () => {
    const g = { nodes: [node('core.a.x'), node('a-very-long-service-name.a.y')], edges: [] as GraphEdge[] };
    const shortOnly = render(
      <RunSwimlane graph={{ nodes: [node('core.a.x')], edges: [] }} tasks={[]} serviceOrder={['core']} collapsed={new Set()} onLaneToggle={vi.fn()} />,
    );
    const shortLeft = parseFloat(shortOnly.container.querySelector<HTMLElement>('.swim-node')!.style.left);
    shortOnly.unmount();
    const { container } = render(
      <RunSwimlane graph={g} tasks={[]} serviceOrder={['core', 'a-very-long-service-name']} collapsed={new Set()} onLaneToggle={vi.fn()} />,
    );
    const lefts = Array.from(container.querySelectorAll<HTMLElement>('.swim-node')).map((el) => parseFloat(el.style.left));
    for (const left of lefts) expect(left).toBeGreaterThan(shortLeft);
  });

  it('paints status on the node itself and leaves the service colour to the lane label', () => {
    const { container } = render(
      <RunSwimlane graph={graph} tasks={tasks} serviceOrder={['core', 'finance']} collapsed={new Set()} onLaneToggle={vi.fn()} />,
    );
    expect(container.querySelectorAll('.swim-node.ok').length).toBe(2);
    expect(container.querySelectorAll('.swim-node.fail').length).toBe(1);
    for (const el of Array.from(container.querySelectorAll<HTMLElement>('.swim-node'))) {
      expect(el.style.borderColor).toBe('');
    }
    expect(container.querySelectorAll('.swim-lab .swim-dot').length).toBe(2);
  });

  it('shows a legend that names every status the header counts', () => {
    const { container } = render(
      <RunSwimlane graph={graph} tasks={tasks} serviceOrder={['core', 'finance']} collapsed={new Set()} onLaneToggle={vi.fn()} />,
    );
    const legend = container.querySelector('.swim-legend')!;
    expect(legend).toBeTruthy();
    for (const label of ['succeeded', 'running', 'failed', 'skipped', 'cancelled', 'pending']) {
      expect(legend.textContent).toContain(label);
    }
  });
});
