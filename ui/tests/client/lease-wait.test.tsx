// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import NodeTableBody from '../../src/client/NodeTableBody';
import RunNodeTable from '../../src/client/RunNodeTable';
import RunSwimlane from '../../src/client/RunSwimlane';
import {
  leaseWaitOf,
  leaseWaitTitle,
  resolveNodeStatus,
  resolveNodeWait,
  runPagePath,
  tasksByNodeId,
  toScheduleGraph,
} from '../../src/client/detail-page-helpers';
import type { GraphEdge, GraphNode, RunGraph, Task, TaskAttempt } from '../../src/client/types';

const HOLDER = '9f1c2d3e-4b5a-6789-abcd-ef0123456789';
const SINCE = '2026-10-24T10:05:00Z';

const task = (name: string, status: string, overrides: Partial<Task> = {}): Task => ({
  task_id: name, service_name: 'core', schema_name: 'a', table_name: name,
  job_name: name, status, attempt: 0, max_retries: 2, created_at: null,
  ...overrides,
});
const waiting = (name: string, holder: string | null = HOLDER) =>
  task(name, 'pending', { wait_reason: 'node_lease', waiting_on_run_id: holder, waiting_since: SINCE });
const noAttempts = new Map<string, TaskAttempt[]>();

describe('lease wait helpers', () => {
  it('reads a wait only for a task waiting on its node lease', () => {
    expect(leaseWaitOf(waiting('t'))).toEqual({ holderRunId: HOLDER, since: SINCE });
    expect(leaseWaitOf(waiting('t', null))).toEqual({ holderRunId: null, since: SINCE });
    expect(leaseWaitOf(task('t', 'pending'))).toBeNull();
    expect(leaseWaitOf(task('t', 'pending', { wait_reason: 'something-else' }))).toBeNull();
    expect(leaseWaitOf(undefined)).toBeNull();
  });

  it('titles a wait by the holder run and when the wait began', () => {
    const at = new Date(SINCE).toLocaleString();
    expect(leaseWaitTitle({ holderRunId: HOLDER, since: SINCE }))
      .toBe(`waiting for run 9f1c2d3e to finish writing this node since ${at}`);
    expect(leaseWaitTitle({ holderRunId: null, since: SINCE }))
      .toBe(`waiting for its turn to write this node since ${at}`);
    expect(leaseWaitTitle({ holderRunId: HOLDER, since: null }))
      .toBe('waiting for run 9f1c2d3e to finish writing this node');
  });

  it('links a run by its id', () => {
    expect(runPagePath(HOLDER)).toBe(`/run/${HOLDER}`);
  });

  it('resolves a graph node\'s wait from its task, else from the run graph', () => {
    const graphNode: GraphNode = {
      node_id: 'core.a.x', node_type: 'dbt-model', schedule_name: 's', status: 'pending',
      wait_reason: 'node_lease', waiting_on_run_id: 'graph-holder', waiting_since: SINCE,
    };
    expect(resolveNodeWait(graphNode, [waiting('x')])).toEqual({ holderRunId: HOLDER, since: SINCE });
    expect(resolveNodeWait(graphNode, [])).toEqual({ holderRunId: 'graph-holder', since: SINCE });
    expect(resolveNodeWait(graphNode, [task('x', 'dispatched')])).toBeNull();
  });

  it('resolves a node\'s status and wait the same from the tasks indexed by node', () => {
    const graphNode: GraphNode = {
      node_id: 'core.a.x', node_type: 'dbt-model', schedule_name: 's', status: 'succeeded',
      wait_reason: 'node_lease', waiting_on_run_id: 'graph-holder', waiting_since: SINCE,
    };
    const tasks = [task('y', 'running'), waiting('x'), task('x', 'dispatched')];
    const byNode = tasksByNodeId(tasks);
    expect([...byNode.keys()]).toEqual(['core.a.y', 'core.a.x']);
    expect(byNode.get('core.a.x')).toBe(tasks[1]);
    expect(resolveNodeStatus(graphNode, byNode)).toBe(resolveNodeStatus(graphNode, tasks));
    expect(resolveNodeStatus(graphNode, byNode)).toBe('pending');
    expect(resolveNodeWait(graphNode, byNode)).toEqual(resolveNodeWait(graphNode, tasks));
    expect(resolveNodeWait(graphNode, byNode)).toEqual({ holderRunId: HOLDER, since: SINCE });
    const empty = tasksByNodeId([]);
    expect(resolveNodeStatus(graphNode, empty)).toBe('succeeded');
    expect(resolveNodeWait(graphNode, empty)).toEqual({ holderRunId: 'graph-holder', since: SINCE });
  });

  it('carries a run graph node\'s wait into the graph the run page draws', () => {
    const runGraph: RunGraph = {
      nodes: [{ node_id: 'core.a.x', node_type: 'dbt-model', schedule_name: 's', status: 'pending',
        wait_reason: 'node_lease', waiting_on_run_id: HOLDER, waiting_since: SINCE }],
      edges: [], run_promotion_seq: 1, latest_promotion_seq: 1,
    };
    expect(toScheduleGraph(runGraph)?.nodes[0]).toMatchObject({
      wait_reason: 'node_lease', waiting_on_run_id: HOLDER, waiting_since: SINCE,
    });
  });
});

describe('the waiting for node pill', () => {
  it('shows on a run page row waiting for its node, linking the holder run', () => {
    render(
      <RunNodeTable tasks={[waiting('orders'), task('customers', 'pending')]} attemptsByTask={noAttempts}
        edges={[]} expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    const pill = screen.getByText('waiting for node');
    expect(pill.tagName).toBe('A');
    expect(pill.getAttribute('href')).toBe(`/run/${HOLDER}`);
    expect(pill.getAttribute('title')).toBe(leaseWaitTitle({ holderRunId: HOLDER, since: SINCE }));
    expect(pill.className).toContain('pill-sm--waiting');
    expect(pill.closest('tr')!.textContent).toContain('orders');
    expect(screen.getByText('customers').closest('tr')!.textContent).not.toContain('waiting for node');
    // The row's own pending pill stays: the wait explains it, it does not replace it.
    expect(pill.closest('td')!.querySelector('.pill-sm--pending')?.textContent).toBe('pending');
  });

  it('is not a link while no task holds the node', () => {
    render(
      <RunNodeTable tasks={[waiting('orders', null)]} attemptsByTask={noAttempts}
        edges={[]} expandedServices={new Set(['core'])} onServiceToggle={vi.fn()} />,
    );
    const pill = screen.getByText('waiting for node');
    expect(pill.tagName).toBe('SPAN');
    expect(pill.getAttribute('title')).toBe(leaseWaitTitle({ holderRunId: null, since: SINCE }));
  });

  it('opening the holder run does not select the row', () => {
    const onNodeSelect = vi.fn();
    render(
      <NodeTableBody groups={[{ service: 'core', status: 'pending', tasks: [waiting('orders')] }]}
        attemptsByTask={noAttempts} edges={[]} startedAt={{}} forceExpanded expandedServices={new Set(['core'])}
        onServiceToggle={vi.fn()} onNodeSelect={onNodeSelect} />,
    );
    // jsdom cannot open another page; a capture listener cancels the link's
    // navigation before React handles the click.
    const noNavigation = (e: Event) => e.preventDefault();
    document.addEventListener('click', noNavigation, true);
    try {
      fireEvent.click(screen.getByText('waiting for node'));
    } finally {
      document.removeEventListener('click', noNavigation, true);
    }
    expect(onNodeSelect).not.toHaveBeenCalled();

    fireEvent.click(screen.getByText('orders'));
    expect(onNodeSelect).toHaveBeenCalledWith('core.a.orders');
  });

  it('marks a waiting node on the run graph, linking the holder run', () => {
    const node = (id: string): GraphNode => ({ node_id: id, node_type: 'dbt-model', schedule_name: 's' });
    const graph = { nodes: [node('core.a.orders'), node('core.a.customers')], edges: [] as GraphEdge[] };
    const { container } = render(
      <RunSwimlane graph={graph} tasks={[waiting('orders'), task('customers', 'pending')]}
        serviceOrder={['core']} collapsed={new Set()} onLaneToggle={vi.fn()} />,
    );
    const pills = container.querySelectorAll('.swim-wait');
    expect(pills.length).toBe(1);
    expect(pills[0].textContent).toBe('waiting for node');
    expect(pills[0].getAttribute('href')).toBe(`/run/${HOLDER}`);
    expect(pills[0].getAttribute('title')).toBe(leaseWaitTitle({ holderRunId: HOLDER, since: SINCE }));
    expect(container.querySelectorAll('.swim-node.pend').length).toBe(2);
  });

  it('a collapsed lane names the wait in its cell\'s title', () => {
    const graph = { nodes: [{ node_id: 'core.a.orders', node_type: 'dbt-model', schedule_name: 's' }], edges: [] as GraphEdge[] };
    const { container } = render(
      <RunSwimlane graph={graph} tasks={[waiting('orders')]}
        serviceOrder={['core']} collapsed={new Set(['core'])} onLaneToggle={vi.fn()} />,
    );
    expect(container.querySelector('.swim-wait')).toBeNull();
    expect(container.querySelector('.swim-cell')!.getAttribute('title'))
      .toBe(`core.a.orders — ${leaseWaitTitle({ holderRunId: HOLDER, since: SINCE })}`);
  });
});
