// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import RunView from '../../../src/client/detail-page/RunView';
import type { ScheduleGraph, Task } from '../../../src/client/types';

const task = {
  task_id: 't1', service_name: 'svc1', schema_name: 'public', table_name: 'orders',
  job_name: 'orders', status: 'running', attempt: 0, max_retries: 0, created_at: null,
} as unknown as Task;
const graph: ScheduleGraph = {
  nodes: [{ node_id: 'svc1.public.orders', node_type: 'dbt-model', schedule_name: 's' }],
  edges: [],
};

function renderRunView(overrides: Partial<React.ComponentProps<typeof RunView>> = {}, path = '/schedule/s') {
  const props: React.ComponentProps<typeof RunView> = {
    state: 'ready',
    selectedRunId: null,
    tasks: [task],
    attemptsByTask: new Map(),
    activeGraph: graph,
    services: ['svc1'],
    expandedServices: new Set(['svc1']),
    onServiceToggle: vi.fn(),
    pastRuns: {
      runs: [], liveRunId: null, liveStatus: null, selectedRunId: null,
      onSelectRun: vi.fn(), page: 0, pageSize: 10, total: 0, onPageChange: vi.fn(),
    },
    ...overrides,
  };
  return render(<MemoryRouter initialEntries={[path]}><RunView {...props} /></MemoryRouter>);
}

describe('RunView', () => {
  it('renders "No runs yet." for state no-runs', () => {
    renderRunView({ state: 'no-runs', tasks: [] });
    expect(screen.getByText('No runs yet.')).toBeInTheDocument();
    expect(screen.queryByTestId('run-progress-pct')).toBeNull();
  });

  it('renders "Loading run…" for state loading, and names the snapshot of a chosen run', () => {
    const { unmount } = renderRunView({ state: 'loading', tasks: [] });
    expect(screen.getByText('Loading run…')).toBeInTheDocument();
    expect(screen.getByText('Fetching the latest run…')).toBeInTheDocument();
    unmount();

    renderRunView({ state: 'loading', tasks: [], selectedRunId: 'old' });
    expect(screen.getByText('Fetching the historical run snapshot…')).toBeInTheDocument();
  });

  it('renders the progress header and the list for state ready', () => {
    renderRunView();
    expect(screen.getByTestId('run-progress-pct')).toBeInTheDocument();
    expect(screen.getByText('orders')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /^run$/i })).toHaveClass('tabs__tab--active');
  });

  it('switches to the swimlane with the Graph toggle', async () => {
    const { container } = renderRunView();
    expect(container.querySelector('.swim-node')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: /^graph$/i }));
    await waitFor(() => expect(container.querySelector('.swim-node')).toBeTruthy());
    fireEvent.click(screen.getByRole('button', { name: /^list$/i }));
    expect(container.querySelector('.swim-node')).toBeNull();
  });

  it('shows the past runs list on the Past runs tab, with the history size on the tab', () => {
    const onSelectRun = vi.fn();
    const { container } = renderRunView({
      pastRuns: {
        runs: [{ run_id: 'r1', schedule_name: 's', status: 'failed', created_at: '2026-09-01T10:00:00Z', completed_at: '2026-09-01T10:05:00Z' }],
        liveRunId: null, liveStatus: null, selectedRunId: null,
        onSelectRun, page: 0, pageSize: 10, total: 7, onPageChange: vi.fn(),
      },
    }, '/schedule/s?panel=runs');
    expect(screen.queryByTestId('run-progress-pct')).toBeNull();
    expect(screen.getByRole('tab', { name: /past runs/i })).toHaveTextContent('7');
    fireEvent.click(container.querySelector('.run-row')!);
    expect(onSelectRun).toHaveBeenCalledWith('r1');
  });
});
