// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import DetailPage from '../../src/client/DetailPage';

function mockFetchSequence(routes: Record<string, () => Promise<unknown>>) {
  return vi.fn(async (input: RequestInfo | URL) => {
    const url = typeof input === 'string' ? input : input.toString();
    for (const [pattern, handler] of Object.entries(routes)) {
      if (url.includes(pattern)) {
        const body = await handler();
        return { ok: true, status: 200, json: async () => body } as Response;
      }
    }
    return { ok: true, status: 200, json: async () => ({}) } as Response;
  });
}

const RUN_ID = 'run-1';
const SCHED = 'hourly-events';
const SAMPLE_NODE_ID = 'svc1.public.orders'; // used in freshRoutes mock data

function withRouter(initialState: unknown) {
  return (
    <MemoryRouter
      initialEntries={[
        { pathname: `/schedule/${SCHED}`, state: initialState },
      ]}
    >
      <Routes>
        <Route path="/schedule/:name" element={<DetailPage />} />
      </Routes>
    </MemoryRouter>
  );
}

beforeEach(() => {
  // Don't fake setInterval — DetailPage uses it for polling and the test
  // would hang. We just want to flush microtasks.
});
afterEach(() => {
  vi.restoreAllMocks();
});


function freshRoutes() {
  return {
    [`/api/runs/${RUN_ID}/graph`]: async () => ({
      nodes: [
        {
          node_id: SAMPLE_NODE_ID,
          node_type: 'dbt-model',
          schedule_name: SCHED,
          status: 'failed',
        },
      ],
      edges: [],
      run_topology_generation: 7,
      latest_topology_generation: 7,
    }),
    [`/api/schedules/${SCHED}/graph`]: async () => ({
      nodes: [
        { node_id: SAMPLE_NODE_ID, node_type: 'dbt-model', schedule_name: SCHED },
      ],
      edges: [],
    }),
    [`/api/schedulers/${RUN_ID}/tasks`]: async () => ({
      tasks: [
        {
          task_id: 't1',
          service_name: 'svc1',
          schema_name: 'public',
          table_name: 'orders',
          job_name: 'orders',
          status: 'failed',
          retry_count: 0,
          max_retries: 0,
          created_at: null,
        },
      ],
    }),
    [`/api/schedulers/${RUN_ID}/executions`]: async () => ({ executions: [] }),
    [`/api/schedules/${SCHED}/runs`]: async () => ({ runs: [] }),
    [`/api/schedulers/${RUN_ID}/rerun`]: async () => ({ ok: true }),
    [`/api/schedulers/${RUN_ID}`]: async () => ({
      scheduler: {
        schedule_id: RUN_ID,
        schedule_name: SCHED,
        status: 'failed',
        created_at: null,
        started_at: null,
        completed_at: null,
        cancelled_at: null,
        cancelled_by: '',
      },
    }),
  };
}

function failedRoutes() {
  return {
    ...freshRoutes(),
    [`/api/schedulers/${RUN_ID}`]: async () => ({
      scheduler: {
        schedule_id: RUN_ID,
        schedule_name: SCHED,
        status: 'SCHEDULER_STATUS_FAILED',
        created_at: null,
        started_at: null,
        completed_at: null,
        cancelled_at: null,
        cancelled_by: '',
      },
    }),
  };
}

describe('DetailPage — run-level Rerun button', () => {
  it('shows Rerun button when latest run is FAILED', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    expect(await screen.findByRole('button', { name: /^↺ Rerun failed \(this snapshot\)$/ })).toBeInTheDocument();
  });

  it('hides Rerun button when latest run is SUCCEEDED', async () => {
    const routes = {
      ...freshRoutes(),
      [`/api/schedulers/${RUN_ID}`]: async () => ({
        scheduler: {
          schedule_id: RUN_ID,
          schedule_name: SCHED,
          status: 'SCHEDULER_STATUS_SUCCEEDED',
          created_at: null,
          started_at: null,
          completed_at: null,
          cancelled_at: null,
          cancelled_by: '',
        },
      }),
    };
    const fetchMock = mockFetchSequence(routes);
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    // Wait for the scheduler fetch to resolve before asserting absence.
    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/schedulers/${RUN_ID}`))).toBe(true);
    });
    expect(screen.queryByRole('button', { name: /^↺ Rerun failed \(this snapshot\)$/ })).toBeNull();
  });

  it('posts empty body to /api/schedulers/:id/rerun on click', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const rerunBtn = await screen.findByRole('button', { name: /^↺ Rerun failed \(this snapshot\)$/ });
    fireEvent.click(rerunBtn);

    await waitFor(() => {
      const calls = fetchMock.mock.calls as unknown as [string, RequestInit?][];
      const rerunCall = calls.find(c => String(c[0]).includes(`/api/schedulers/${RUN_ID}/rerun`));
      expect(rerunCall).toBeDefined();
      expect(rerunCall![1]).toMatchObject({ method: 'POST', body: '{}' });
    });
  });

  it('shows drift badge when stale', async () => {
    const routes = {
      ...failedRoutes(),
      [`/api/runs/${RUN_ID}/graph`]: async () => ({
        nodes: [],
        edges: [],
        run_topology_generation: 5,
        latest_topology_generation: 8,
      }),
    };
    const fetchMock = mockFetchSequence(routes);
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const thisBtn = await screen.findByRole('button', { name: /^↺ Rerun failed \(this snapshot\)$/ });
    const wrapper = thisBtn.closest('.rerun-this-snapshot-group');
    expect(wrapper).toBeTruthy();
    expect(wrapper).toHaveTextContent(/source 3 gen behind latest/);
  });

  it('turns the same Rerun button green and relabels on success', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/schedulers/${RUN_ID}/rerun`]: async () => ({ ok: true }),
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const rerunBtn = await screen.findByRole('button', { name: /^↺ Rerun failed \(this snapshot\)$/ });
    fireEvent.click(rerunBtn);

    const success = await screen.findByRole('button', { name: /^✓ Rerun triggered$/ });
    expect(success.className).toContain('success');
  });
});

describe('DetailPage — run-level Rebase button', () => {
  it('shows Rebase button when latest run is FAILED', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    expect(
      await screen.findByRole('button', { name: /^↪ Rerun failed \(latest snapshot\)$/ }),
    ).toBeInTheDocument();
  });

  it('hides Rebase button when latest run is SUCCEEDED', async () => {
    const routes = {
      ...freshRoutes(),
      [`/api/schedulers/${RUN_ID}`]: async () => ({
        scheduler: {
          schedule_id: RUN_ID,
          schedule_name: SCHED,
          status: 'SCHEDULER_STATUS_SUCCEEDED',
          created_at: null,
          started_at: null,
          completed_at: null,
          cancelled_at: null,
          cancelled_by: '',
        },
      }),
    };
    const fetchMock = mockFetchSequence(routes);
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/schedulers/${RUN_ID}`))).toBe(true);
    });
    expect(screen.queryByRole('button', { name: /^↪ Rerun failed \(latest snapshot\)$/ })).toBeNull();
  });

  it('POSTs /api/schedulers/:id/rebase on click', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/schedulers/${RUN_ID}/rebase`]: async () => ({ ok: true }),
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const rebaseBtn = await screen.findByRole('button', { name: /^↪ Rerun failed \(latest snapshot\)$/ });
    fireEvent.click(rebaseBtn);

    await waitFor(() => {
      const calls = fetchMock.mock.calls as unknown as [string, RequestInit?][];
      const rebaseCall = calls.find(c => String(c[0]).includes(`/api/schedulers/${RUN_ID}/rebase`));
      expect(rebaseCall).toBeDefined();
      expect(rebaseCall![1]).toMatchObject({ method: 'POST' });
    });
  });
});

describe('DetailPage — Trigger run topbar button', () => {
  it('renders Trigger run in the topbar and POSTs /api/schedules/:name/trigger', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/schedules/${SCHED}/trigger`]: async () => ({ schedule_id: 'new-id' }),
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const btn = await screen.findByRole('button', { name: /^▶ Trigger run$/ });
    fireEvent.click(btn);

    await waitFor(() => {
      const calls = fetchMock.mock.calls as unknown as [string, RequestInit?][];
      const triggerCall = calls.find(c => String(c[0]).includes(`/api/schedules/${SCHED}/trigger`));
      expect(triggerCall).toBeDefined();
      expect(triggerCall![1]).toMatchObject({ method: 'POST' });
    });
  });

  it('disables the topbar Trigger run button while a run is live', async () => {
    // Use freshRoutes() but override the scheduler endpoint to return a
    // non-terminal status so liveRunExists becomes true.
    const fetchMock = mockFetchSequence({
      ...freshRoutes(),
      [`/api/schedulers/${RUN_ID}`]: async () => ({
        scheduler: {
          schedule_id: RUN_ID,
          schedule_name: SCHED,
          status: 'SCHEDULER_STATUS_RUNNING',
          created_at: null,
          started_at: null,
          completed_at: null,
          cancelled_at: null,
          cancelled_by: '',
        },
      }),
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    // Wait for scheduler fetch to land so liveRunExists is computed.
    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/schedulers/${RUN_ID}`))).toBe(true);
    });
    const btn = await screen.findByRole('button', { name: /^▶ Trigger run$/ });
    expect(btn).toBeDisabled();
  });
});

describe('DetailPage — Open node detail link', () => {
  it('renders an "Open node detail" link when a node is selected and links to the node page', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    // Wait for graph + nodes panel to render then select the node from the NodesPanel.
    // The node row contains table_name "orders" in nodes-node-name, and "svc1 · public" in nodes-node-schema
    const nodeButton = await screen.findByRole('button', { name: /orders/i });
    fireEvent.click(nodeButton);

    const link = await screen.findByRole('link', { name: /open node detail/i });
    expect(link.getAttribute('href')).toBe(`/schedule/${SCHED}/node/${SAMPLE_NODE_ID}`);
  });

  it('does NOT show the link when no node is selected', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    // Without clicking any node, the link should not be rendered.
    // Use waitFor to give the page time to finish initial fetches.
    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/runs/${RUN_ID}/graph`))).toBe(true);
    });
    expect(screen.queryByRole('link', { name: /open node detail/i })).toBeNull();
  });
});

describe('DetailPage — Trigger run success cue', () => {
  it('turns the Trigger run button green and relabels on success', async () => {
    const fetchMock = mockFetchSequence({
      ...freshRoutes(),
      [`/api/schedules/${SCHED}/trigger`]: async () => ({ ok: true }),
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const triggerBtn = await screen.findByRole('button', { name: /^▶ Trigger run$/ });
    fireEvent.click(triggerBtn);

    const success = await screen.findByRole('button', { name: /^✓ Triggered$/ });
    expect(success.className).toContain('success');
  });
});
