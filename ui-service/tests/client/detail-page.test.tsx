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
const SAMPLE_NODE_ID = 'svc1.public.orders';

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

async function selectFirstNode() {
  // The Nodes panel renders a <tr role="button"> per task — click that
  // (rather than the DAG node which has duplicate text) to set selectedNodeId
  // and mount the focus legend with the Rerun button.
  const row = await screen.findByRole('button', { name: /orders/i });
  fireEvent.click(row);
}

// findEnabledRerunButton waits for the Rerun button to mount AND for
// liveRunGraph to load (the button is disabled until the graph fetch resolves).
// Using button.disabled === false as the gate is the only DOM-observable signal
// that handleRerun will see liveRunGraph != null and compute drift correctly.
async function findEnabledRerunButton(): Promise<HTMLButtonElement> {
  const btn = (await screen.findByRole(
    'button',
    { name: /Rerun node/i },
    { timeout: 10000 },
  )) as HTMLButtonElement;
  await waitFor(() => expect(btn.disabled).toBe(false), { timeout: 10000 });
  return btn;
}

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

describe('DetailPage — rerun gating by drift state', () => {
  it('FRESH: clicks Rerun → POSTs immediately, no dialog', async () => {
    const fetchMock = mockFetchSequence(freshRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    await selectFirstNode();
    const rerunBtn = await findEnabledRerunButton();
    fireEvent.click(rerunBtn);

    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/schedulers/${RUN_ID}/rerun`))).toBe(true);
    });
    expect(screen.queryByText('Stale topology')).toBeNull();
    expect(screen.queryByText('Topology unknown')).toBeNull();
  });

  it('STALE: clicks Rerun → dialog appears, no POST until Confirm', async () => {
    const routes = freshRoutes();
    routes[`/api/runs/${RUN_ID}/graph`] = async () => ({
      nodes: [
        {
          node_id: SAMPLE_NODE_ID,
          node_type: 'dbt-model',
          schedule_name: SCHED,
          status: 'failed',
        },
      ],
      edges: [],
      run_topology_generation: 5,
      latest_topology_generation: 7,
    });
    const fetchMock = mockFetchSequence(routes);
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    await selectFirstNode();
    const rerunBtn = await findEnabledRerunButton();
    fireEvent.click(rerunBtn);

    expect(await screen.findByText('Stale topology')).toBeInTheDocument();
    const calls = fetchMock.mock.calls.map(c => String(c[0]));
    expect(calls.some(u => u.includes(`/api/schedulers/${RUN_ID}/rerun`))).toBe(false);

    fireEvent.click(screen.getByRole('button', { name: 'Rerun with old snapshot' }));
    await waitFor(() => {
      const calls2 = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls2.some(u => u.includes(`/api/schedulers/${RUN_ID}/rerun`))).toBe(true);
    });
  });

  it('UNKNOWN: clicks Rerun → dialog with unknown copy, Cancel does not fire POST', async () => {
    const routes = freshRoutes();
    routes[`/api/runs/${RUN_ID}/graph`] = async () => ({
      nodes: [
        {
          node_id: SAMPLE_NODE_ID,
          node_type: 'dbt-model',
          schedule_name: SCHED,
          status: 'failed',
        },
      ],
      edges: [],
      run_topology_generation: 0,
      latest_topology_generation: 7,
    });
    const fetchMock = mockFetchSequence(routes);
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    await selectFirstNode();
    const rerunBtn = await findEnabledRerunButton();
    fireEvent.click(rerunBtn);

    expect(await screen.findByText('Topology unknown')).toBeInTheDocument();
    expect(screen.getByText('no version recorded')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    const calls = fetchMock.mock.calls.map(c => String(c[0]));
    expect(calls.some(u => u.includes(`/api/schedulers/${RUN_ID}/rerun`))).toBe(false);
  });
});

describe('DetailPage — failed-stale-rerun (safety-net)', () => {
  it('failed run with stale drift → dialog mounts; Confirm dispatches the rerun POST', async () => {
    const routes = freshRoutes();
    routes[`/api/runs/${RUN_ID}/graph`] = async () => ({
      nodes: [
        {
          node_id: SAMPLE_NODE_ID,
          node_type: 'dbt-model',
          schedule_name: SCHED,
          status: 'failed',
        },
      ],
      edges: [],
      run_topology_generation: 5,
      latest_topology_generation: 7,
    });
    // Confirm scheduler returns failed status — the modal must still fire.
    routes[`/api/schedulers/${RUN_ID}`] = async () => ({
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
    });
    const fetchMock = mockFetchSequence(routes);
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    await selectFirstNode();
    fireEvent.click(await findEnabledRerunButton());

    expect(await screen.findByText('Stale topology')).toBeInTheDocument();
    expect(screen.getByText('5 → 7')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Rerun with old snapshot' }));
    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/schedulers/${RUN_ID}/rerun`))).toBe(true);
    });
  });
});
