// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router';
import DetailPage from '../../src/client/DetailPage';
import { TaskExecution } from '../../src/client/types';

// The `url` argument lets a route handler inspect query params (e.g. `offset`)
// to serve a distinct page per request. Existing handlers that ignore the
// extra argument (`async () => (...)`) keep working unchanged.
function mockFetchSequence(routes: Record<string, (url: string) => Promise<unknown>>) {
  return vi.fn(async (input: RequestInfo | URL) => {
    const url = typeof input === 'string' ? input : input.toString();
    for (const [pattern, handler] of Object.entries(routes)) {
      if (url.includes(pattern)) {
        const body = await handler(url);
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
  it('shows Rerun failed button when latest run is FAILED', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    expect(await screen.findByRole('button', { name: /rerun failed/i })).toBeInTheDocument();
  });

  it('hides Rerun failed button when latest run is SUCCEEDED', async () => {
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
    expect(screen.queryByRole('button', { name: /rerun failed/i })).toBeNull();
  });

  it('opens the modal when Rerun failed button is clicked', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const rerunBtn = await screen.findByRole('button', { name: /rerun failed/i });
    fireEvent.click(rerunBtn);

    expect(await screen.findByRole('dialog')).toBeInTheDocument();
  });

  it('posts empty body to /api/schedulers/:id/rerun when modal submits with "this snapshot"', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const rerunBtn = await screen.findByRole('button', { name: /rerun failed/i });
    fireEvent.click(rerunBtn);

    // Modal opens; default is "this snapshot" — click Rerun
    const submitBtn = await screen.findByRole('button', { name: /^Rerun$/ });
    fireEvent.click(submitBtn);

    await waitFor(() => {
      const calls = fetchMock.mock.calls as unknown as [string, RequestInit?][];
      const rerunCall = calls.find(c => String(c[0]).includes(`/api/schedulers/${RUN_ID}/rerun`));
      expect(rerunCall).toBeDefined();
      expect(rerunCall![1]).toMatchObject({ method: 'POST', body: '{}' });
    });
  });

  it('turns the Rerun failed button green and relabels on success', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/schedulers/${RUN_ID}/rerun`]: async () => ({ ok: true }),
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const rerunBtn = await screen.findByRole('button', { name: /rerun failed/i });
    fireEvent.click(rerunBtn);

    const submitBtn = await screen.findByRole('button', { name: /^Rerun$/ });
    fireEvent.click(submitBtn);

    const success = await screen.findByRole('button', { name: /Reran/i });
    expect(success.className).toContain('is-success');
  });
});

describe('DetailPage — run-level Rebase button (via modal)', () => {
  it('shows the modal with both "This snapshot" and "Latest snapshot" radio options', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const rerunBtn = await screen.findByRole('button', { name: /rerun failed/i });
    fireEvent.click(rerunBtn);

    expect(await screen.findByLabelText(/this snapshot/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/latest snapshot/i)).toBeInTheDocument();
  });

  it('hides Rerun failed button when latest run is SUCCEEDED', async () => {
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
    expect(screen.queryByRole('button', { name: /rerun failed/i })).toBeNull();
  });

  it('POSTs /api/schedulers/:id/rebase when "Latest snapshot" is selected and submitted', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/schedulers/${RUN_ID}/rebase`]: async () => ({ ok: true }),
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const rerunBtn = await screen.findByRole('button', { name: /rerun failed/i });
    fireEvent.click(rerunBtn);

    // Switch to "latest snapshot" radio
    const latestRadio = await screen.findByLabelText(/latest snapshot/i);
    fireEvent.click(latestRadio);

    const submitBtn = screen.getByRole('button', { name: /^Rerun$/ });
    fireEvent.click(submitBtn);

    await waitFor(() => {
      const calls = fetchMock.mock.calls as unknown as [string, RequestInit?][];
      const rebaseCall = calls.find(c => String(c[0]).includes(`/api/schedulers/${RUN_ID}/rebase`));
      expect(rebaseCall).toBeDefined();
      expect(rebaseCall![1]).toMatchObject({ method: 'POST' });
    });
  });

  it('turns the Rerun failed button green and relabels on rebase success', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/schedulers/${RUN_ID}/rebase`]: async () => ({ ok: true }),
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const rerunBtn = await screen.findByRole('button', { name: /rerun failed/i });
    fireEvent.click(rerunBtn);

    const latestRadio = await screen.findByLabelText(/latest snapshot/i);
    fireEvent.click(latestRadio);

    const submitBtn = screen.getByRole('button', { name: /^Rerun$/ });
    fireEvent.click(submitBtn);

    const success = await screen.findByRole('button', { name: /Reran/i });
    expect(success.className).toContain('is-success');
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

  it('uses .btn .btn--secondary classes (no .trigger-run-btn)', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const btn = await screen.findByRole('button', { name: /^▶ Trigger run$/ });
    expect(btn.className).toContain('btn');
    expect(btn.className).toContain('btn--secondary');
    expect(btn.className).not.toContain('trigger-run-btn');
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
    expect(link.getAttribute('href')).toBe(`/node/${SAMPLE_NODE_ID}`);
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

  it('keeps the node selected when the graph card mounts after the click', async () => {
    // The Nodes table renders from the tasks fetch and the graph card from the
    // graph fetch. Holding the graph fetch behind a gate forces the order this
    // test is about: the user clicks a node row, and only then does the DAG
    // panel mount.
    let releaseGraph: () => void = () => {};
    const graphGate = new Promise<void>((resolve) => {
      releaseGraph = resolve;
    });
    const base = failedRoutes();
    const routes = {
      ...base,
      [`/api/schedules/${SCHED}/graph`]: async () => {
        await graphGate;
        return base[`/api/schedules/${SCHED}/graph`]();
      },
      [`/api/runs/${RUN_ID}/graph`]: async () => {
        await graphGate;
        return base[`/api/runs/${RUN_ID}/graph`]();
      },
    };
    const fetchMock = mockFetchSequence(routes);
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const nodeButton = await screen.findByRole('button', { name: /orders/i });
    fireEvent.click(nodeButton);

    // The graph card is still loading, so the click landed before the DAG mounted.
    expect(screen.getByText('Loading graph')).toBeInTheDocument();

    releaseGraph();
    await waitFor(() => expect(screen.queryByText('Loading graph')).toBeNull());

    expect(
      await screen.findByRole('link', { name: /open node detail/i }),
    ).toBeInTheDocument();
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

    const success = await screen.findByRole('button', { name: /^Triggered$/ });
    expect(success.className).toContain('is-success');
  });
});

describe('DetailPage — page structure (Task 5 migration)', () => {
  it('wraps in .page and .page-header; .detail-page and .detail-topbar are gone', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    const { container } = render(withRouter({ last_run_id: RUN_ID }));

    // Wait for initial render
    await screen.findByRole('button', { name: /^▶ Trigger run$/ });

    expect(container.querySelector('.page')).toBeTruthy();
    expect(container.querySelector('.page-header')).toBeTruthy();
    expect(container.querySelector('.detail-page')).toBeNull();
    expect(container.querySelector('.detail-topbar')).toBeNull();
  });

  it('renders exactly ONE Rerun failed button when status is terminal-failed', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    // Wait for page to settle
    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/schedulers/${RUN_ID}`))).toBe(true);
    });

    const rerunBtns = screen.getAllByRole('button', { name: /rerun failed/i });
    expect(rerunBtns).toHaveLength(1);
  });

  it('does not show buttons for "this snapshot" or "latest snapshot" in the topbar', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/schedulers/${RUN_ID}`))).toBe(true);
    });

    expect(screen.queryByRole('button', { name: /this snapshot/i })).toBeNull();
    expect(screen.queryByRole('button', { name: /latest snapshot/i })).toBeNull();
  });

  it('clicking the Rerun failed button opens a dialog', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    const btn = await screen.findByRole('button', { name: /rerun failed/i });
    fireEvent.click(btn);

    expect(await screen.findByRole('dialog')).toBeInTheDocument();
    // Modal contains both radio choices
    expect(screen.getByLabelText(/this snapshot/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/latest snapshot/i)).toBeInTheDocument();
  });

  it('submitting "this snapshot" (default) POSTs to the rerun endpoint', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    fireEvent.click(await screen.findByRole('button', { name: /rerun failed/i }));
    fireEvent.click(await screen.findByRole('button', { name: /^Rerun$/ }));

    await waitFor(() => {
      const calls = fetchMock.mock.calls as unknown as [string, RequestInit?][];
      const hit = calls.find(c => String(c[0]).includes(`/api/schedulers/${RUN_ID}/rerun`));
      expect(hit).toBeDefined();
      expect(hit![1]).toMatchObject({ method: 'POST', body: '{}' });
    });
  });

  it('switching to "latest snapshot" then submitting POSTs to the rebase endpoint', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/schedulers/${RUN_ID}/rebase`]: async () => ({ ok: true }),
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    fireEvent.click(await screen.findByRole('button', { name: /rerun failed/i }));
    fireEvent.click(await screen.findByLabelText(/latest snapshot/i));
    fireEvent.click(screen.getByRole('button', { name: /^Rerun$/ }));

    await waitFor(() => {
      const calls = fetchMock.mock.calls as unknown as [string, RequestInit?][];
      const hit = calls.find(c => String(c[0]).includes(`/api/schedulers/${RUN_ID}/rebase`));
      expect(hit).toBeDefined();
      expect(hit![1]).toMatchObject({ method: 'POST' });
    });
  });

  it('trigger error renders as .info-strip--error, not as an inline sibling of the button', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/schedules/${SCHED}/trigger`]: async () => {
        throw new Error('network failure');
      },
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    fireEvent.click(await screen.findByRole('button', { name: /^▶ Trigger run$/ }));

    const errorEl = await screen.findByText(/Request failed — please try again/);
    expect(errorEl.closest('.info-strip--error')).toBeTruthy();
    // Confirm it is NOT a sibling inside .page-action-row
    expect(errorEl.closest('.page-action-row')).toBeNull();
  });
});

function withRouterAt(initialPath: string) {
  return (
    <MemoryRouter initialEntries={[initialPath]}>
      <Routes>
        <Route path="/schedule/:name" element={<DetailPage />} />
        <Route path="/schedule/:name/latest" element={<DetailPage mode="latest" />} />
      </Routes>
    </MemoryRouter>
  );
}

describe('DetailPage — right-column panel tabs', () => {
  beforeEach(() => { vi.stubGlobal('fetch', mockFetchSequence(freshRoutes())); });
  afterEach(() => { vi.unstubAllGlobals(); });

  it('defaults to the Nodes panel on /schedule/:name', async () => {
    render(withRouterAt(`/schedule/${SCHED}`));
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /nodes/i })).toHaveClass('tabs__tab--active');
    });
    expect(screen.getByRole('tab', { name: /past runs/i })).not.toHaveClass('tabs__tab--active');
  });

  it('selects the Past Runs panel when ?panel=runs is in the URL', async () => {
    render(withRouterAt(`/schedule/${SCHED}?panel=runs`));
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /past runs/i })).toHaveClass('tabs__tab--active');
    });
  });

  it('falls back to Nodes when ?panel is unknown', async () => {
    render(withRouterAt(`/schedule/${SCHED}?panel=garbage`));
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /nodes/i })).toHaveClass('tabs__tab--active');
    });
  });

  it('does not render panel tabs on /schedule/:name/latest (latest mode shows section header instead)', async () => {
    const { container } = render(withRouterAt(`/schedule/${SCHED}/latest`));
    await waitFor(() => {
      expect(container.querySelector('.tabs--panel')).toBeNull();
      expect(screen.queryByRole('tab', { name: /nodes/i })).toBeNull();
    });
  });

  it('does not render two separate detail-nodes-card and detail-runs-card sections anymore', async () => {
    const { container } = render(withRouterAt(`/schedule/${SCHED}`));
    await waitFor(() => {
      expect(container.querySelectorAll('.detail-nodes-card')).toHaveLength(0);
      expect(container.querySelectorAll('.detail-runs-card')).toHaveLength(0);
    });
  });
});

const LATEST_SCHED = 'topology-only';
const LATEST_RUN_ID = 'past-run-1';

function withLatestRouter() {
  return (
    <MemoryRouter initialEntries={[`/schedule/${LATEST_SCHED}/latest`]}>
      <Routes>
        <Route path="/schedule/:name/latest" element={<DetailPage mode="latest" />} />
      </Routes>
    </MemoryRouter>
  );
}

function latestRoutes() {
  return {
    [`/api/schedules/${LATEST_SCHED}/graph`]: async () => ({
      nodes: [
        { node_id: 'svc.sch.a', node_type: 'dbt-model', schedule_name: LATEST_SCHED },
      ],
      edges: [],
      topology_generation: 5,
    }),
    [`/api/schedules/${LATEST_SCHED}/runs`]: async () => ({
      runs: [
        {
          run_id: LATEST_RUN_ID,
          schedule_name: LATEST_SCHED,
          terminal_status: 'succeeded',
          created_at: '2026-05-26T11:39:00Z',
          completed_at: '2026-05-26T11:40:00Z',
        },
      ],
    }),
    '/api/schedules': async () => ({
      schedules: [{ schedule_name: LATEST_SCHED, last_run_id: LATEST_RUN_ID }],
    }),
    [`/api/runs/${LATEST_RUN_ID}/graph`]: async () => ({
      nodes: [
        { node_id: 'svc.sch.a', node_type: 'dbt-model', schedule_name: LATEST_SCHED, status: 'succeeded' },
      ],
      edges: [],
      run_topology_generation: 4,
      latest_topology_generation: 5,
    }),
    [`/api/schedulers/${LATEST_RUN_ID}`]: async () => ({
      scheduler: {
        schedule_id: LATEST_RUN_ID,
        schedule_name: LATEST_SCHED,
        status: 'scheduler_status_succeeded',
        created_at: null,
        started_at: null,
        completed_at: null,
        cancelled_at: null,
        cancelled_by: '',
      },
    }),
    [`/api/schedulers/${LATEST_RUN_ID}/tasks`]: async () => ({
      tasks: [
        {
          task_id: 't1',
          service_name: 'svc',
          schema_name: 'sch',
          table_name: 'a',
          job_name: '',
          status: 'succeeded',
          retry_count: 0,
          max_retries: 0,
          created_at: null,
        },
      ],
    }),
    [`/api/schedulers/${LATEST_RUN_ID}/executions`]: async () => ({ executions: [] }),
  };
}

describe('DetailPage in latest mode', () => {
  it('renders a Past Runs section header and no panel tab strip', async () => {
    const fetchMock = mockFetchSequence(latestRoutes());
    vi.stubGlobal('fetch', fetchMock);
    try {
      const { container } = render(withLatestRouter());
      await waitFor(() => {
        const titles = container.querySelectorAll('.section-header__title');
        expect(Array.from(titles).some(el => el.textContent === 'Past Runs')).toBe(true);
      });
      expect(container.querySelector('.tabs--panel')).toBeNull();
      expect(screen.queryByRole('tab', { name: /nodes/i })).toBeNull();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('shows the catalog badge regardless of past runs', async () => {
    const fetchMock = mockFetchSequence(latestRoutes());
    vi.stubGlobal('fetch', fetchMock);
    try {
      render(withLatestRouter());
      await waitFor(() => {
        expect(screen.getByText('catalog')).toBeInTheDocument();
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('does not poll /tasks or /executions in latest mode', async () => {
    const fetchMock = mockFetchSequence(latestRoutes());
    vi.stubGlobal('fetch', fetchMock);
    try {
      render(withLatestRouter());
      await waitFor(() => screen.getByText('catalog'));
      const urls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(urls.some(u => /\/api\/schedulers\/[^/]+\/tasks$/.test(u))).toBe(false);
      expect(urls.some(u => /\/api\/schedulers\/[^/]+\/executions$/.test(u))).toBe(false);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe('DetailPage — executions pagination regression (Task 7)', () => {
  // The executions endpoint pages at 200 rows. Regression: DetailPage used to
  // issue a single raw fetch and silently dropped any execution past row 200.
  // This fixture serves 412 executions across three pages (200/200/12) for a
  // single task, with the execution holding the true latest `started_at` (and
  // therefore the one NodesPanel should render) sitting at index 400 — deep
  // into the third page. A mock that ignored `offset` would defeat this test,
  // so the handler below slices the full list by the requested offset/limit.
  const TOTAL_EXECUTIONS = 412;
  const LATEST_INDEX = 400;
  const LATEST_ERROR_MESSAGE = 'DISTINCT_ERROR_BEYOND_PAGE_1';
  const LATEST_LOG_S3_KEY = 'logs/task-t1-exec-400.log';

  function buildExecutions(): TaskExecution[] {
    const execs: TaskExecution[] = [];
    for (let i = 0; i < TOTAL_EXECUTIONS; i++) {
      if (i === LATEST_INDEX) {
        execs.push({
          id: `exec-${i}`,
          task_id: 't1',
          error_message: LATEST_ERROR_MESSAGE,
          execution_time_seconds: 12,
          started_at: '2026-01-01T10:00:00Z', // strictly later than every other row below
          completed_at: '2026-01-01T10:01:00Z',
          log_s3_key: LATEST_LOG_S3_KEY,
        });
      } else {
        execs.push({
          id: `exec-${i}`,
          task_id: 't1',
          error_message: `stale-error-${i}`,
          execution_time_seconds: 1,
          started_at: '2026-01-01T00:00:00Z',
          completed_at: '2026-01-01T00:01:00Z',
          log_s3_key: `logs/task-t1-exec-${i}.log`,
        });
      }
    }
    return execs;
  }

  function pagedExecutionsHandler(url: string) {
    const parsed = new URL(url, 'http://localhost');
    const offset = Number(parsed.searchParams.get('offset') ?? '0');
    const limit = Number(parsed.searchParams.get('limit') ?? '200');
    const all = buildExecutions();
    return Promise.resolve({
      executions: all.slice(offset, offset + limit),
      total_count: all.length,
    });
  }

  it('renders the error message and log link of an execution beyond page 1 (index 400 of 412)', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/schedulers/${RUN_ID}/executions`]: pagedExecutionsHandler,
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    // The true latest execution lives on page 3 (offset=400); if DetailPage
    // regressed to a single fetch, only the stale page-1 execution would ever
    // be considered "latest" for task t1, and this text would never appear.
    // (ErrorCell renders the message twice — a truncated span plus a
    // full-text div revealed via CSS on "more" — hence findAllByText.)
    expect((await screen.findAllByText(LATEST_ERROR_MESSAGE)).length).toBeGreaterThan(0);

    const logLink = screen.getByRole('link', { name: /logs/i });
    expect(logLink.getAttribute('href')).toContain(encodeURIComponent(LATEST_LOG_S3_KEY));

    // Guard against a mock that ignores `offset` (which would pass even
    // against a broken single-fetch implementation): assert the component
    // actually requested pages 2 and 3.
    const calls = fetchMock.mock.calls.map(c => String(c[0]));
    const executionCalls = calls.filter(u => u.includes(`/api/schedulers/${RUN_ID}/executions`));
    expect(executionCalls.some(u => u.includes('offset=200'))).toBe(true);
    expect(executionCalls.some(u => u.includes('offset=400'))).toBe(true);
  });
});

describe('DetailPage — service grouping sync between graph and table', () => {
  const NODE_A = 'svc1.public.orders';
  const NODE_B = 'svc2.public.payments';

  function multiServiceRoutes() {
    return {
      ...failedRoutes(),
      [`/api/runs/${RUN_ID}/graph`]: async () => ({
        nodes: [
          { node_id: NODE_A, node_type: 'dbt-model', schedule_name: SCHED, status: 'succeeded' },
          { node_id: NODE_B, node_type: 'dbt-model', schedule_name: SCHED, status: 'failed' },
        ],
        edges: [{ from_node_id: NODE_A, to_node_id: NODE_B }],
        run_topology_generation: 7,
        latest_topology_generation: 7,
      }),
      [`/api/schedulers/${RUN_ID}/tasks`]: async () => ({
        tasks: [
          {
            task_id: 't1', service_name: 'svc1', schema_name: 'public', table_name: 'orders',
            job_name: '', status: 'succeeded', retry_count: 0, max_retries: 0, created_at: null,
          },
          {
            task_id: 't2', service_name: 'svc2', schema_name: 'public', table_name: 'payments',
            job_name: '', status: 'failed', retry_count: 0, max_retries: 0, created_at: null,
          },
        ],
      }),
    };
  }

  // Node names appear both as graph vertices and table rows once a service is
  // expanded, so table assertions go through the row-name class, not by text.
  function tableNodeNames(container: HTMLElement): (string | null)[] {
    return [...container.querySelectorAll('.nodes-node-name')].map(el => el.textContent);
  }

  it('defaults both panels to collapsed service groups when several services exist', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(multiServiceRoutes()));
    try {
      const { container } = render(withRouter({ last_run_id: RUN_ID }));

      // Graph: one vertex per service, no model-node vertices.
      await waitFor(() => {
        expect(container.querySelector('.react-flow__node[data-id="svc:svc1"]')).toBeTruthy();
        expect(container.querySelector('.react-flow__node[data-id="svc:svc2"]')).toBeTruthy();
      });
      expect(container.querySelector(`.react-flow__node[data-id="${NODE_A}"]`)).toBeNull();

      // Table: group headers visible, node rows hidden.
      expect(container.querySelectorAll('.nodes-group-row')).toHaveLength(2);
      expect(screen.queryByText('orders')).toBeNull();
      expect(screen.queryByText('payments')).toBeNull();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('expanding a group in the table reveals its model nodes in the graph', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(multiServiceRoutes()));
    try {
      const { container } = render(withRouter({ last_run_id: RUN_ID }));
      await waitFor(() => {
        expect(container.querySelectorAll('.nodes-group-row').length).toBe(2);
      });

      const svc1Header = [...container.querySelectorAll('.nodes-group-row')]
        .find(h => h.textContent?.includes('svc1'))!;
      fireEvent.click(svc1Header);

      await waitFor(() => {
        expect(tableNodeNames(container)).toContain('orders');
        expect(container.querySelector(`.react-flow__node[data-id="${NODE_A}"]`)).toBeTruthy();
      });
      // The other service stays collapsed in both panels.
      expect(container.querySelector('.react-flow__node[data-id="svc:svc2"]')).toBeTruthy();
      expect(tableNodeNames(container)).not.toContain('payments');
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('clicking a service vertex in the graph expands its group in the table', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(multiServiceRoutes()));
    try {
      const { container } = render(withRouter({ last_run_id: RUN_ID }));
      await waitFor(() => {
        expect(container.querySelector('.react-flow__node[data-id="svc:svc2"]')).toBeTruthy();
      });

      fireEvent.click(container.querySelector('.react-flow__node[data-id="svc:svc2"]')!);

      await waitFor(() => {
        expect(tableNodeNames(container)).toContain('payments');
      });
      expect(tableNodeNames(container)).not.toContain('orders');
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('collapsing the service of the selected node clears the selection', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(multiServiceRoutes()));
    try {
      const { container } = render(withRouter({ last_run_id: RUN_ID }));
      await waitFor(() => {
        expect(container.querySelectorAll('.nodes-group-row').length).toBe(2);
      });

      const svc1Header = [...container.querySelectorAll('.nodes-group-row')]
        .find(h => h.textContent?.includes('svc1'))!;
      fireEvent.click(svc1Header);
      await waitFor(() => {
        expect(tableNodeNames(container)).toContain('orders');
      });
      const ordersRow = [...container.querySelectorAll('.nodes-node-name')]
        .find(el => el.textContent === 'orders')!.closest('tr')!;
      fireEvent.click(ordersRow);
      expect(await screen.findByRole('link', { name: /open node detail/i })).toBeInTheDocument();

      fireEvent.click(svc1Header); // collapse again
      await waitFor(() => {
        expect(screen.queryByRole('link', { name: /open node detail/i })).toBeNull();
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
