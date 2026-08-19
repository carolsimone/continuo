// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import NodesPanel from '../../src/client/NodesPanel';
import type { Task, TaskExecution } from '../../src/client/types';

function task(nodeId: string, status: string): Task {
  const [service_name, schema_name, table_name] = nodeId.split('.');
  return {
    task_id: nodeId, service_name, schema_name, table_name,
    job_name: '', status, retry_count: 0, max_retries: 0, created_at: null,
  };
}

const TASKS: Task[] = [
  task('svc-a.sch.n1', 'succeeded'),
  task('svc-a.sch.n2', 'succeeded'),
  task('svc-b.sch.m1', 'failed'),
];

const COLORS = new Map([['svc-a', '#0ea5e9'], ['svc-b', '#f59e0b']]);

function setup(overrides: {
  expandedServices?: Set<string>;
  onServiceToggle?: (svc: string) => void;
  onNodeSelect?: (id: string | null) => void;
  executions?: TaskExecution[];
  selectedNodeId?: string | null;
} = {}) {
  return render(
    <NodesPanel
      tasks={TASKS}
      executions={overrides.executions ?? []}
      selectedNodeId={overrides.selectedNodeId ?? null}
      onNodeSelect={overrides.onNodeSelect ?? (() => {})}
      expandedServices={overrides.expandedServices ?? new Set()}
      onServiceToggle={overrides.onServiceToggle ?? (() => {})}
      serviceColors={COLORS}
    />,
  );
}

describe('NodesPanel service grouping', () => {
  it('renders one collapsed group header per service and hides node rows by default', () => {
    const { container } = setup();
    expect(screen.getByText('svc-a')).toBeInTheDocument();
    expect(screen.getByText('svc-b')).toBeInTheDocument();
    const counts = [...container.querySelectorAll('.nodes-group-count')].map(el => el.textContent);
    expect(counts.sort()).toEqual(['1', '2']);
    expect(screen.queryByText('n1')).toBeNull();
    expect(screen.queryByText('m1')).toBeNull();
  });

  it('orders groups most-severe-first (failed svc-b before succeeded svc-a)', () => {
    const { container } = setup();
    const names = [...container.querySelectorAll('.nodes-group-name')].map(el => el.textContent);
    expect(names).toEqual(['svc-b', 'svc-a']);
  });

  it('shows a rolled-up status pill and the service color dot on the header', () => {
    const { container } = setup();
    const headers = [...container.querySelectorAll('.nodes-group-row')];
    const svcB = headers.find(h => h.textContent?.includes('svc-b'))!;
    expect(svcB.querySelector('.pill-sm--failed')?.textContent).toBe('failed');
    const dot = svcB.querySelector('.nodes-group-dot') as HTMLElement;
    expect(dot.style.background).toBe('rgb(245, 158, 11)'); // #f59e0b
  });

  it('reveals the node rows of an expanded service with columns unchanged', () => {
    setup({ expandedServices: new Set(['svc-a']) });
    expect(screen.getByText('n1')).toBeInTheDocument();
    expect(screen.getByText('n2')).toBeInTheDocument();
    expect(screen.queryByText('m1')).toBeNull(); // svc-b stays collapsed
    // Node subtitle (service · schema) preserved inside the expanded group.
    expect(screen.getAllByText('svc-a · sch')).toHaveLength(2);
  });

  it('invokes onServiceToggle when a group header is clicked or activated by keyboard', () => {
    const onServiceToggle = vi.fn();
    const { container } = setup({ onServiceToggle });
    const header = [...container.querySelectorAll('.nodes-group-row')]
      .find(h => h.textContent?.includes('svc-a'))!;
    fireEvent.click(header);
    expect(onServiceToggle).toHaveBeenCalledWith('svc-a');
    fireEvent.keyDown(header, { key: 'Enter' });
    expect(onServiceToggle).toHaveBeenCalledTimes(2);
  });

  it('marks group headers with aria-expanded reflecting their state', () => {
    const { container } = setup({ expandedServices: new Set(['svc-a']) });
    const headers = [...container.querySelectorAll('.nodes-group-row')];
    const svcA = headers.find(h => h.textContent?.includes('svc-a'))!;
    const svcB = headers.find(h => h.textContent?.includes('svc-b'))!;
    expect(svcA.getAttribute('aria-expanded')).toBe('true');
    expect(svcB.getAttribute('aria-expanded')).toBe('false');
  });

  it('still selects a node row and renders execution details inside an expanded group', () => {
    const onNodeSelect = vi.fn();
    const executions: TaskExecution[] = [{
      id: 'exec-1', task_id: 'svc-b.sch.m1', error_message: 'boom',
      execution_time_seconds: 3, started_at: '2026-01-01T00:00:00Z',
      completed_at: '2026-01-01T00:01:00Z', log_s3_key: 'logs/m1.log',
      run_results_uri: null,
    }];
    setup({ expandedServices: new Set(['svc-b']), executions, onNodeSelect });

    fireEvent.click(screen.getByText('m1'));
    expect(onNodeSelect).toHaveBeenCalledWith('svc-b.sch.m1');
    expect(screen.getAllByText('boom').length).toBeGreaterThan(0);
    expect(screen.getByRole('link', { name: /logs/i }).getAttribute('href'))
      .toContain(encodeURIComponent('logs/m1.log'));
  });

  it('orders same-status rows by completed_at descending (last finished first)', () => {
    // n1 finished earlier than n2; without a tiebreak they'd stay in task order (n1, n2).
    const executions: TaskExecution[] = [
      {
        id: 'exec-n1', task_id: 'svc-a.sch.n1', error_message: null,
        execution_time_seconds: 1, started_at: '2026-01-01T00:00:00Z',
        completed_at: '2026-01-01T00:01:00Z', log_s3_key: null, run_results_uri: null,
      },
      {
        id: 'exec-n2', task_id: 'svc-a.sch.n2', error_message: null,
        execution_time_seconds: 1, started_at: '2026-01-01T00:02:00Z',
        completed_at: '2026-01-01T00:03:00Z', log_s3_key: null, run_results_uri: null,
      },
    ];
    const { container } = setup({ expandedServices: new Set(['svc-a']), executions });
    const names = [...container.querySelectorAll('.nodes-node-name')].map(el => el.textContent);
    expect(names).toEqual(['n2', 'n1']);
  });

  it('renders a Completed column sourced from completed_at, dash when absent', () => {
    // Column order: Node, Status, Attempt, Error, Started, Completed, Logs.
    const COMPLETED_COL = 5;
    const executions: TaskExecution[] = [{
      id: 'exec-n1', task_id: 'svc-a.sch.n1', error_message: null,
      execution_time_seconds: 1, started_at: '2026-01-01T00:00:00Z',
      completed_at: '2026-01-01T00:01:00Z', log_s3_key: null, run_results_uri: null,
    }];
    const { container } = setup({ expandedServices: new Set(['svc-a']), executions });
    expect(screen.getByText('Completed')).toBeInTheDocument();
    const rows = [...container.querySelectorAll('.nodes-table tbody tr')]
      .filter(r => !r.classList.contains('nodes-group-row'));
    const n1Row = rows.find(r => r.textContent?.includes('n1'))!;
    const n2Row = rows.find(r => r.textContent?.includes('n2'))!;
    expect(n1Row.querySelectorAll('td')[COMPLETED_COL].textContent).toBe(
      new Date('2026-01-01T00:01:00Z').toLocaleTimeString(),
    );
    expect(n2Row.querySelectorAll('td')[COMPLETED_COL].textContent).toBe('—');
  });
});
