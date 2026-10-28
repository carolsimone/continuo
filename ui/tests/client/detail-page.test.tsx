// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent, within } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useNavigate } from 'react-router';
import DetailPage from '../../src/client/DetailPage';
import { TaskAttempt } from '../../src/client/types';

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
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
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
      run_promotion_seq: 7,
      latest_promotion_seq: 7,
    }),
    [`/api/schedules/${SCHED}/graph`]: async () => ({
      nodes: [
        { node_id: SAMPLE_NODE_ID, node_type: 'dbt-model', schedule_name: SCHED },
      ],
      edges: [],
    }),
    [`/api/runs/${RUN_ID}/tasks`]: async () => ({
      tasks: [
        {
          task_id: 't1',
          service_name: 'svc1',
          schema_name: 'public',
          table_name: 'orders',
          job_name: 'orders',
          status: 'failed',
          attempt: 0,
          max_retries: 0,
          created_at: null,
        },
      ],
    }),
    [`/api/runs/${RUN_ID}/attempts`]: async () => ({ attempts: [], total_count: 0 }),
    [`/api/schedules/${SCHED}/runs`]: async () => ({ runs: [] }),
    [`/api/runs/${RUN_ID}/rerun`]: async () => ({ ok: true }),
    [`/api/runs/${RUN_ID}`]: async () => ({
      run_id: RUN_ID,
      schedule_name: SCHED,
      status: 'failed',
      created_at: null,
      started_at: null,
      completed_at: null,
      cancelled_at: null,
      cancelled_by: '',
    }),
  };
}

function failedRoutes() {
  return {
    ...freshRoutes(),
    [`/api/runs/${RUN_ID}`]: async () => ({
      run_id: RUN_ID,
      schedule_name: SCHED,
      status: 'failed',
      created_at: null,
      started_at: null,
      completed_at: null,
      cancelled_at: null,
      cancelled_by: '',
    }),
  };
}

// REBASE_PREVIEW is the preview the rerun-failed dialog shows before a rebase.
const REBASE_PREVIEW = {
  release_id: 'rel-7', promotion_seq: 7, total_nodes: 8, will_run: 4,
  failed_or_skipped: 4, new_nodes: 0, changed_upstream: 0, downstream: 0, inherited: 4,
  changed_nodes: [],
};

// rebaseFetch answers the rebase preview and the rebase POST by exact path —
// mockFetchSequence matches by substring in insertion order, so its
// `/api/runs/${RUN_ID}` route would otherwise answer both — and every
// other request from routes the way mockFetchSequence does.
function rebaseFetch(
  routes: Record<string, (url: string) => Promise<unknown>>,
  rebaseReply: { status: number; body: unknown } = { status: 200, body: {} },
) {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString();
    const path = url.split('?')[0];
    if (path === `/api/runs/${RUN_ID}/rebase-preview`) {
      return { ok: true, status: 200, json: async () => REBASE_PREVIEW } as Response;
    }
    if (path === `/api/runs/${RUN_ID}/rebase` && init?.method === 'POST') {
      const { status, body } = rebaseReply;
      return { ok: status >= 200 && status < 300, status, json: async () => body } as Response;
    }
    for (const [pattern, handler] of Object.entries(routes)) {
      if (url.includes(pattern)) {
        const body = await handler(url);
        return { ok: true, status: 200, json: async () => body } as Response;
      }
    }
    return { ok: true, status: 200, json: async () => ({}) } as Response;
  });
}

// submitLatest opens the rerun-failed dialog, chooses "Latest snapshot",
// waits for its preview and confirms.
async function submitLatest() {
  fireEvent.click(await screen.findByRole('button', { name: /rerun failed/i }));
  fireEvent.click(await screen.findByLabelText(/latest snapshot/i));
  await screen.findByText('Will run');
  await waitFor(() => expect(screen.getByRole('button', { name: /^Rerun$/ })).toBeEnabled());
  fireEvent.click(screen.getByRole('button', { name: /^Rerun$/ }));
}

describe('DetailPage — run-level Rerun button', () => {
  it('shows Rerun failed button when latest run is FAILED', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    expect(await screen.findByRole('button', { name: /rerun failed/i })).toBeInTheDocument();
  });

  it('says why a run that failed without a failed task failed', async () => {
    const routes = failedRoutes();
    const base = routes[`/api/runs/${RUN_ID}`];
    routes[`/api/runs/${RUN_ID}`] = async (url: string) => {
      const body = (await base(url)) as Record<string, unknown>;
      return { ...body, failure_reason: 'stalled' };
    };
    vi.stubGlobal('fetch', mockFetchSequence(routes));

    render(withRouter({ last_run_id: RUN_ID }));

    expect(await screen.findByText('failed: stalled')).toBeInTheDocument();
  });

  it('hides Rerun failed button when latest run is SUCCEEDED', async () => {
    const routes = {
      ...freshRoutes(),
      [`/api/runs/${RUN_ID}`]: async () => ({
        run_id: RUN_ID,
        schedule_name: SCHED,
        status: 'succeeded',
        created_at: null,
        started_at: null,
        completed_at: null,
        cancelled_at: null,
        cancelled_by: '',
      }),
    };
    const fetchMock = mockFetchSequence(routes);
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    // Wait for the scheduler fetch to resolve before asserting absence.
    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/runs/${RUN_ID}`))).toBe(true);
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

  it('posts the request id to /api/runs/:id/rerun when modal submits with "this snapshot"', async () => {
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
      const rerunCall = calls.find(c => String(c[0]).includes(`/api/runs/${RUN_ID}/rerun`));
      expect(rerunCall).toBeDefined();
      expect(rerunCall![1]).toMatchObject({ method: 'POST' });
      expect(JSON.parse(String(rerunCall![1]!.body))).toEqual({ request_id: expect.stringMatching(UUID) });
    });
  });

  it('turns the Rerun failed button green and relabels on success', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/runs/${RUN_ID}/rerun`]: async () => ({ ok: true }),
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
      [`/api/runs/${RUN_ID}`]: async () => ({
        run_id: RUN_ID,
        schedule_name: SCHED,
        status: 'succeeded',
        created_at: null,
        started_at: null,
        completed_at: null,
        cancelled_at: null,
        cancelled_by: '',
      }),
    };
    const fetchMock = mockFetchSequence(routes);
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/runs/${RUN_ID}`))).toBe(true);
    });
    expect(screen.queryByRole('button', { name: /rerun failed/i })).toBeNull();
  });

  it('POSTs /api/runs/:id/rebase on the previewed release when "Latest snapshot" is submitted', async () => {
    const fetchMock = rebaseFetch(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));
    await submitLatest();

    await waitFor(() => {
      const calls = fetchMock.mock.calls as unknown as [string, RequestInit?][];
      const rebaseCall = calls.find(c => c[0] === `/api/runs/${RUN_ID}/rebase`);
      expect(rebaseCall).toBeDefined();
      expect(rebaseCall![1]).toMatchObject({ method: 'POST' });
      expect(JSON.parse(String(rebaseCall![1]!.body))).toEqual({
        request_id: expect.stringMatching(UUID),
        include_changed_upstream: true,
        expected_release_id: 'rel-7',
      });
    });
    expect(fetchMock.mock.calls.map(c => String(c[0])))
      .toContain(`/api/runs/${RUN_ID}/rebase-preview?include_changed_upstream=true`);
  });

  it('turns the Rerun failed button green and relabels on rebase success', async () => {
    vi.stubGlobal('fetch', rebaseFetch(failedRoutes()));

    render(withRouter({ last_run_id: RUN_ID }));
    await submitLatest();

    const success = await screen.findByRole('button', { name: /Reran/i });
    expect(success.className).toContain('is-success');
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('keeps a refused rebase inside the dialog and leaves the page without an error strip', async () => {
    vi.stubGlobal('fetch', rebaseFetch(failedRoutes(), { status: 409, body: { error: 'schedule already has an active run' } }));

    render(withRouter({ last_run_id: RUN_ID }));
    await submitLatest();

    const strip = (await screen.findByText('schedule already has an active run')).closest('.info-strip--error');
    expect(strip).toBeTruthy();
    expect(strip!.closest('[role="dialog"]')).toBeTruthy();
    expect(screen.getAllByText('schedule already has an active run')).toHaveLength(1);
    expect(screen.getByRole('button', { name: /rerun failed/i })).not.toHaveClass('is-success');
  });
});

describe('DetailPage — Trigger run topbar button', () => {
  it('renders Trigger run in the topbar and POSTs /api/schedules/:name/trigger', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/schedules/${SCHED}/trigger`]: async () => ({ run_id: 'new-id' }),
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

  it('DetailPage trigger: the body carries a request id, and a press after a timeout sends it again', async () => {
    const ids: string[] = [];
    const routes = mockFetchSequence(failedRoutes());
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).includes(`/api/schedules/${SCHED}/trigger`)) {
        ids.push(JSON.parse(String(init?.body)).request_id);
        if (ids.length === 1) throw new Error('timeout');
        return { ok: true, status: 200, json: async () => ({ run_id: 'new-id' }) } as Response;
      }
      return routes(input);
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    fireEvent.click(await screen.findByRole('button', { name: /^▶ Trigger run$/ }));
    await screen.findByText('Request failed — please try again');
    fireEvent.click(await screen.findByRole('button', { name: /^▶ Trigger run$/ }));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[0]).toMatch(UUID);
    expect(ids[1]).toBe(ids[0]);
  });

  it('DetailPage trigger: a press hours after an unanswered one, with no newer run observed, is a retry with the same id', async () => {
    const ids: string[] = [];
    const routes = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).includes(`/api/schedules/${SCHED}/trigger`)) {
        ids.push(JSON.parse(String(init?.body)).request_id);
        if (ids.length === 1) throw new Error('timeout');
        return { ok: true, status: 200, json: async () => ({ run_id: 'new-id' }) } as Response;
      }
      return routes(input);
    }));
    vi.useFakeTimers({ toFake: ['Date'] });
    try {
      render(withRouter({ last_run_id: RUN_ID }));

      fireEvent.click(await screen.findByRole('button', { name: /^▶ Trigger run$/ }));
      await screen.findByText('Request failed — please try again');
      vi.setSystemTime(Date.now() + 6 * 60 * 60 * 1000);
      fireEvent.click(await screen.findByRole('button', { name: /^▶ Trigger run$/ }));

      await waitFor(() => expect(ids).toHaveLength(2));
      expect(ids[1]).toBe(ids[0]);
    } finally {
      vi.useRealTimers();
    }
  });

  it('DetailPage trigger: a press after the page arrives at the schedule again and reads a newer run is a new action', async () => {
    const ids: string[] = [];
    const routes = mockFetchSequence({
      ...failedRoutes(),
      '/api/runs/run-2/graph': async () => ({ nodes: [], edges: [] }),
      '/api/runs/run-2/tasks': async () => ({ tasks: [] }),
      '/api/runs/run-2/attempts': async () => ({ attempts: [], total_count: 0 }),
      '/api/runs/run-2': async () => ({ run_id: 'run-2', schedule_name: SCHED, status: 'succeeded' }),
    });
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).includes(`/api/schedules/${SCHED}/trigger`)) {
        ids.push(JSON.parse(String(init?.body)).request_id);
        if (ids.length === 1) throw new Error('timeout');
        return { ok: true, status: 200, json: async () => ({ run_id: 'new-id' }) } as Response;
      }
      return routes(input);
    }));
    // Moves between schedules without leaving the page, the way a link to
    // another schedule does, so the page keeps its request id.
    function Nav() {
      const navigate = useNavigate();
      return (
        <>
          <button onClick={() => navigate('/schedule/other', { state: { last_run_id: null } })}>to other</button>
          <button onClick={() => navigate(`/schedule/${SCHED}`, { state: { last_run_id: 'run-2' } })}>back</button>
        </>
      );
    }
    render(
      <MemoryRouter initialEntries={[{ pathname: `/schedule/${SCHED}`, state: { last_run_id: RUN_ID } }]}>
        <Nav />
        <Routes>
          <Route path="/schedule/:name" element={<DetailPage />} />
        </Routes>
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole('button', { name: /^▶ Trigger run$/ }));
    await screen.findByText('Request failed — please try again');
    fireEvent.click(screen.getByRole('button', { name: 'to other' }));
    // Arriving at the schedule again reads its newest run, the one the
    // unanswered press made.
    fireEvent.click(screen.getByRole('button', { name: 'back' }));
    fireEvent.click(await screen.findByRole('button', { name: /^▶ Trigger run$/ }));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toMatch(UUID);
    expect(ids[1]).not.toBe(ids[0]);
  });

  it('disables the topbar Trigger run button while a run is live', async () => {
    // Use freshRoutes() but override the scheduler endpoint to return a
    // non-terminal status so liveRunExists becomes true.
    const fetchMock = mockFetchSequence({
      ...freshRoutes(),
      [`/api/runs/${RUN_ID}`]: async () => ({
        run_id: RUN_ID,
        schedule_name: SCHED,
        status: 'running',
        created_at: null,
        started_at: null,
        completed_at: null,
        cancelled_at: null,
        cancelled_by: '',
      }),
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    // Wait for scheduler fetch to land so liveRunExists is computed.
    await waitFor(() => {
      const calls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(calls.some(u => u.includes(`/api/runs/${RUN_ID}`))).toBe(true);
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

// The per-node focus legend and its "Open node detail" link belong to the
// node-level dependency graph, which the Run view no longer carries (the Run
// tab is status-first). That graph and its legend live on the topology-catalog
// (latest) view; the legend + link behaviour is covered against that view in
// detail-page-legend.test.tsx.

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
      expect(calls.some(u => u.includes(`/api/runs/${RUN_ID}`))).toBe(true);
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
      expect(calls.some(u => u.includes(`/api/runs/${RUN_ID}`))).toBe(true);
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
      const hit = calls.find(c => String(c[0]).includes(`/api/runs/${RUN_ID}/rerun`));
      expect(hit).toBeDefined();
      expect(hit![1]).toMatchObject({ method: 'POST' });
      expect(JSON.parse(String(hit![1]!.body))).toEqual({ request_id: expect.stringMatching(UUID) });
    });
  });

  it('switching to "latest snapshot" then submitting POSTs to the rebase endpoint', async () => {
    const fetchMock = rebaseFetch(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));
    await submitLatest();

    await waitFor(() => {
      const calls = fetchMock.mock.calls as unknown as [string, RequestInit?][];
      const hit = calls.find(c => c[0] === `/api/runs/${RUN_ID}/rebase`);
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

describe('DetailPage — page tabs (Run / Past runs)', () => {
  beforeEach(() => { vi.stubGlobal('fetch', mockFetchSequence(freshRoutes())); });
  afterEach(() => { vi.unstubAllGlobals(); });

  it('defaults to the Run tab on /schedule/:name', async () => {
    render(withRouterAt(`/schedule/${SCHED}`));
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /^run$/i })).toHaveClass('tabs__tab--active');
    });
    expect(screen.queryByRole('tab', { name: /topology/i })).toBeNull(); // Topology is a homepage tab only
    expect(screen.getByRole('tab', { name: /past runs/i })).not.toHaveClass('tabs__tab--active');
  });

  it('selects the Past Runs panel when ?panel=runs is in the URL', async () => {
    render(withRouterAt(`/schedule/${SCHED}?panel=runs`));
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /past runs/i })).toHaveClass('tabs__tab--active');
    });
  });

  it('has no Topology tab and falls back to Run for the old ?panel=topology URL', async () => {
    render(withRouterAt(`/schedule/${SCHED}?panel=topology`));
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /^run$/i })).toHaveClass('tabs__tab--active');
    });
    expect(screen.queryByRole('tab', { name: /topology/i })).toBeNull();
  });

  it('falls back to Run when ?panel is unknown', async () => {
    render(withRouterAt(`/schedule/${SCHED}?panel=garbage`));
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /^run$/i })).toHaveClass('tabs__tab--active');
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
      promotion_seq: 5,
    }),
    [`/api/schedules/${LATEST_SCHED}/runs`]: async () => ({
      runs: [
        {
          run_id: LATEST_RUN_ID,
          schedule_name: LATEST_SCHED,
          status: 'succeeded',
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
      run_promotion_seq: 4,
      latest_promotion_seq: 5,
    }),
    [`/api/runs/${LATEST_RUN_ID}`]: async () => ({
      run_id: LATEST_RUN_ID,
      schedule_name: LATEST_SCHED,
      status: 'succeeded',
      created_at: null,
      started_at: null,
      completed_at: null,
      cancelled_at: null,
      cancelled_by: '',
    }),
    [`/api/runs/${LATEST_RUN_ID}/tasks`]: async () => ({
      tasks: [
        {
          task_id: 't1',
          service_name: 'svc',
          schema_name: 'sch',
          table_name: 'a',
          job_name: '',
          status: 'succeeded',
          attempt: 0,
          max_retries: 0,
          created_at: null,
        },
      ],
    }),
    [`/api/runs/${LATEST_RUN_ID}/attempts`]: async () => ({ attempts: [], total_count: 0 }),
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

  it('does not poll /tasks or /attempts in latest mode', async () => {
    const fetchMock = mockFetchSequence(latestRoutes());
    vi.stubGlobal('fetch', fetchMock);
    try {
      render(withLatestRouter());
      await waitFor(() => screen.getByText('catalog'));
      const urls = fetchMock.mock.calls.map(c => String(c[0]));
      // fetchAllPages appends ?limit=&offset=, so the patterns are not
      // end-anchored: an anchored one could never match a paged request.
      expect(urls.some(u => /\/api\/schedulers\/[^/]+\/tasks/.test(u))).toBe(false);
      expect(urls.some(u => /\/api\/schedulers\/[^/]+\/attempts/.test(u))).toBe(false);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('labels the header with the live promotion seq', async () => {
    const fetchMock = mockFetchSequence(latestRoutes());
    vi.stubGlobal('fetch', fetchMock);
    try {
      render(withLatestRouter());
      const chip = await screen.findByLabelText('live promotion seq');
      expect(chip).toHaveTextContent('promotion 5');
      expect(chip.closest('.info-strip--info')).toBeInTheDocument();
      expect(screen.queryByText(/topology v/)).toBeNull();
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe('DetailPage in latest mode — Past Runs paging', () => {
  const TOTAL_RUNS = 25;

  // Serves the requested page of a TOTAL_RUNS-long history, newest first. Run
  // 0 failed; every other run succeeded.
  function pagedRunsRoutes() {
    return {
      ...latestRoutes(),
      [`/api/schedules/${LATEST_SCHED}/runs`]: async (url: string) => {
        const params = new URL(url, 'http://x').searchParams;
        const limit = Number(params.get('limit'));
        const offset = Number(params.get('offset'));
        const ids = Array.from({ length: TOTAL_RUNS }, (_, i) => i).slice(offset, offset + limit);
        return {
          runs: ids.map(i => ({
            run_id: `hist-run-${i}`,
            schedule_name: LATEST_SCHED,
            status: i === 0 ? 'failed' : 'succeeded',
            created_at: '2026-05-26T11:39:00Z',
            completed_at: '2026-05-26T11:40:00Z',
            failure_reason: i === 0 ? 'stalled' : null,
          })),
          total_count: TOTAL_RUNS,
        };
      },
      '/api/runs/hist-run-0/graph': async () => ({
        nodes: [{ node_id: 'svc.sch.a', node_type: 'dbt-model', schedule_name: LATEST_SCHED, status: 'failed' }],
        edges: [],
        run_promotion_seq: 4,
        latest_promotion_seq: 5,
      }),
    };
  }

  it('loads the first page, shows the true total and pages with Older and Newer', async () => {
    const fetchMock = mockFetchSequence(pagedRunsRoutes());
    vi.stubGlobal('fetch', fetchMock);
    try {
      const { container } = render(withLatestRouter());

      await waitFor(() => expect(container.querySelectorAll('.run-row')).toHaveLength(10));
      expect(container.querySelector('.section-header__count')).toHaveTextContent(String(TOTAL_RUNS));
      expect(screen.getByText('1–10 of 25')).toBeInTheDocument();
      const runUrls = () => fetchMock.mock.calls.map(c => String(c[0])).filter(u => /\/runs\?/.test(u));
      expect(runUrls()[0]).toContain('limit=10&offset=0');

      fireEvent.click(screen.getByRole('button', { name: 'Older' }));
      await screen.findByText('11–20 of 25');
      expect(runUrls().some(u => u.includes('offset=10'))).toBe(true);

      fireEvent.click(screen.getByRole('button', { name: 'Newer' }));
      await screen.findByText('1–10 of 25');
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('keeps describing a selected run after paging away from its page', async () => {
    const fetchMock = mockFetchSequence(pagedRunsRoutes());
    vi.stubGlobal('fetch', fetchMock);
    try {
      const { container } = render(withLatestRouter());
      await waitFor(() => expect(container.querySelectorAll('.run-row')).toHaveLength(10));

      // The first row is the failed run 0.
      fireEvent.click(container.querySelectorAll('.run-row')[0]);
      await waitFor(() => expect(container.querySelector('.pill')).toHaveTextContent(/failed/i));

      fireEvent.click(screen.getByRole('button', { name: 'Older' }));
      await screen.findByText('11–20 of 25');

      expect(container.querySelector('.pill')).toHaveTextContent(/failed/i);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('hides the pager when the history fits on one page', async () => {
    const fetchMock = mockFetchSequence({
      ...latestRoutes(),
      [`/api/schedules/${LATEST_SCHED}/runs`]: async () => ({ runs: [], total_count: 3 }),
    });
    vi.stubGlobal('fetch', fetchMock);
    try {
      const { container } = render(withLatestRouter());
      await waitFor(() => expect(container.querySelector('.section-header__count')).toHaveTextContent('3'));
      expect(screen.queryByRole('button', { name: 'Older' })).toBeNull();
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe('DetailPage — Run tab polling', () => {
  // Only setInterval is faked: the page polls through it, while waitFor and
  // the mocked fetches keep running on real timers and microtasks.
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  const schedulerAt = (id: string, status: string) => ({
    run_id: id, schedule_name: SCHED, status,
    created_at: null, started_at: null, completed_at: null, cancelled_at: null, cancelled_by: '',
  });
  const taskAt = (status: string) => ({
    tasks: [{
      task_id: 't1', service_name: 'svc1', schema_name: 'public', table_name: 'orders',
      job_name: 'orders', status, attempt: 0, max_retries: 0, created_at: null,
    }],
  });
  const attemptAt = (status: string) => ({
    total_count: 1,
    attempts: [{
      task_id: 't1', attempt: 0, job_name: 'orders', status,
      failure_class: null, failure_reason: null,
      dispatched_at: null, queued_at: null, started_at: null, finished_at: null,
      execution_seconds: null, error_message: null, log_s3_key: null, run_results_uri: null,
    } satisfies TaskAttempt],
  });
  const rowStatus = () => screen.getByText('orders').closest('tr')?.querySelector('.pill-sm')?.textContent;
  const callsTo = (fetchMock: ReturnType<typeof vi.fn>, path: string) =>
    fetchMock.mock.calls.map(c => String(c[0])).filter(u => u.startsWith(path)).length;

  it('reads the tasks and attempts once more after the poll that sees the run end', async () => {
    // The run ends between the poll's row reads and its scheduler read: that
    // poll sees the run succeeded but rows that still say running. Only a
    // read after it returns the settled rows.
    let ended = false;
    let staleTasks = true;
    let staleAttempts = true;
    const fetchMock = mockFetchSequence({
      ...freshRoutes(),
      [`/api/runs/${RUN_ID}/tasks`]: async () => {
        if (ended && !staleTasks) return taskAt('succeeded');
        if (ended) staleTasks = false;
        return taskAt('running');
      },
      [`/api/runs/${RUN_ID}/attempts`]: async () => {
        if (ended && !staleAttempts) return attemptAt('succeeded');
        if (ended) staleAttempts = false;
        return attemptAt('running');
      },
      [`/api/runs/${RUN_ID}`]: async () => schedulerAt(RUN_ID, ended ? 'succeeded' : 'running'),
    });
    vi.stubGlobal('fetch', fetchMock);
    try {
      render(withRouter({ last_run_id: RUN_ID }));
      await waitFor(() => expect(rowStatus()).toBe('running'));
      const before = callsTo(fetchMock, `/api/runs/${RUN_ID}/tasks`);

      ended = true;
      await vi.advanceTimersByTimeAsync(5000);

      await waitFor(() => expect(rowStatus()).toBe('succeeded'));
      expect(callsTo(fetchMock, `/api/runs/${RUN_ID}/tasks`)).toBe(before + 2);

      // The run is terminal: the poll has stopped.
      await vi.advanceTimersByTimeAsync(15000);
      expect(callsTo(fetchMock, `/api/runs/${RUN_ID}/tasks`)).toBe(before + 2);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('does not poll a selected past run', async () => {
    const OLD_RUN_ID = 'run-old';
    const fetchMock = mockFetchSequence({
      ...freshRoutes(),
      [`/api/runs/${RUN_ID}/tasks`]: async () => taskAt('running'),
      [`/api/runs/${RUN_ID}`]: async () => schedulerAt(RUN_ID, 'running'),
      [`/api/schedules/${SCHED}/runs`]: async () => ({
        runs: [
          { run_id: OLD_RUN_ID, schedule_name: SCHED, status: 'failed', created_at: '2026-09-01T10:00:00Z', completed_at: '2026-09-01T10:05:00Z' },
        ],
      }),
      [`/api/runs/${OLD_RUN_ID}/graph`]: async () => ({ nodes: [], edges: [], run_promotion_seq: 6, latest_promotion_seq: 7 }),
      [`/api/runs/${OLD_RUN_ID}/tasks`]: async () => ({
        total_count: 1,
        tasks: [{
          task_id: 'old-t1', service_name: 'svc1', schema_name: 'public', table_name: 'old_orders',
          job_name: '', status: 'failed', attempt: 0, max_retries: 0, created_at: null,
        }],
      }),
      [`/api/runs/${OLD_RUN_ID}/attempts`]: async () => ({ attempts: [], total_count: 0 }),
    });
    vi.stubGlobal('fetch', fetchMock);
    try {
      const { container } = render(withRouter({ last_run_id: RUN_ID }));
      expect(await screen.findByText('orders')).toBeInTheDocument();

      // The live run heads the list; the past run follows it.
      fireEvent.click(await screen.findByRole('tab', { name: /past runs/i }));
      await waitFor(() => expect(container.querySelectorAll('.run-row')).toHaveLength(2));
      fireEvent.click(container.querySelectorAll('.run-row')[1]);
      fireEvent.click(screen.getByRole('tab', { name: /^run$/i }));
      expect(await screen.findByText('old_orders')).toBeInTheDocument();

      const rowReads = () => [
        callsTo(fetchMock, `/api/runs/${OLD_RUN_ID}/tasks`),
        callsTo(fetchMock, `/api/runs/${OLD_RUN_ID}/attempts`),
        callsTo(fetchMock, `/api/runs/${RUN_ID}/tasks`),
        callsTo(fetchMock, `/api/runs/${RUN_ID}/attempts`),
      ];
      const before = rowReads();
      expect(before.slice(0, 2)).toEqual([1, 1]);

      await vi.advanceTimersByTimeAsync(15000);
      expect(rowReads()).toEqual(before);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe('DetailPage — header drift chip', () => {
  it('shows how many releases the live run is behind the live topology', async () => {
    const routes = {
      ...failedRoutes(),
      [`/api/runs/${RUN_ID}/graph`]: async () => ({
        nodes: [
          { node_id: SAMPLE_NODE_ID, node_type: 'dbt-model', schedule_name: SCHED, status: 'failed' },
        ],
        edges: [],
        run_promotion_seq: 4,
        latest_promotion_seq: 6,
      }),
    };
    vi.stubGlobal('fetch', mockFetchSequence(routes));
    try {
      render(withRouter({ last_run_id: RUN_ID }));
      const chip = await screen.findByText('source 2 releases behind latest');
      expect(chip.closest('.info-strip--warning')).toBeInTheDocument();
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe('DetailPage — attempts pagination regression', () => {
  // The attempts endpoint pages at 200 rows. Regression: DetailPage once
  // issued a single raw fetch and silently dropped every row past 200. This
  // fixture serves 412 attempts across three pages (200/200/12) in state's
  // (task_id, attempt) order: t1's attempts 0..400 fill indices 0..400 and a
  // second task fills the rest, so t1's highest-numbered attempt — the one
  // its row reads — sits at index 400, deep into the third page. A mock that
  // ignored `offset` would defeat this test, so the handler slices the full
  // list by the requested offset/limit.
  const TOTAL_ATTEMPTS = 412;
  const LATEST_INDEX = 400;
  const LATEST_ERROR_MESSAGE = 'DISTINCT_ERROR_BEYOND_PAGE_1';
  const LATEST_LOG_S3_KEY = 'logs/task-t1-attempt-400.log';

  function buildAttempts(): TaskAttempt[] {
    const rows: TaskAttempt[] = [];
    for (let i = 0; i < TOTAL_ATTEMPTS; i++) {
      const isT1 = i <= LATEST_INDEX;
      rows.push({
        task_id: isT1 ? 't1' : 't9',
        attempt: isT1 ? i : i - LATEST_INDEX - 1,
        job_name: `job-${i}`,
        status: 'failed',
        failure_class: null,
        failure_reason: null,
        dispatched_at: null,
        queued_at: null,
        started_at: '2026-01-01T00:00:00Z',
        finished_at: '2026-01-01T00:01:00Z',
        execution_seconds: 60,
        error_message: i === LATEST_INDEX ? LATEST_ERROR_MESSAGE : `stale-error-${i}`,
        log_s3_key: i === LATEST_INDEX ? LATEST_LOG_S3_KEY : `logs/attempt-${i}.log`,
        run_results_uri: null,
      });
    }
    return rows;
  }

  function pagedAttemptsHandler(url: string) {
    const parsed = new URL(url, 'http://localhost');
    const offset = Number(parsed.searchParams.get('offset') ?? '0');
    const limit = Number(parsed.searchParams.get('limit') ?? '200');
    const all = buildAttempts();
    return Promise.resolve({
      attempts: all.slice(offset, offset + limit),
      total_count: all.length,
    });
  }

  it('renders the error message and log link of an attempt beyond page 1 (index 400 of 412)', async () => {
    const fetchMock = mockFetchSequence({
      ...failedRoutes(),
      [`/api/runs/${RUN_ID}/attempts`]: pagedAttemptsHandler,
    });
    vi.stubGlobal('fetch', fetchMock);

    render(withRouter({ last_run_id: RUN_ID }));

    // t1's highest-numbered attempt lives on page 3 (offset=400); if
    // DetailPage regressed to a single fetch, only page 1's attempts would
    // ever be grouped for t1, and this text would never appear. (ErrorCell
    // renders the message twice — a truncated span plus a full-text div
    // revealed via CSS on "more" — hence findAllByText.)
    expect((await screen.findAllByText(LATEST_ERROR_MESSAGE)).length).toBeGreaterThan(0);

    const logLink = screen.getByRole('link', { name: /logs/i });
    expect(logLink.getAttribute('href')).toContain(encodeURIComponent(LATEST_LOG_S3_KEY));

    // Guard against a mock that ignores `offset` (which would pass even
    // against a broken single-fetch implementation): assert the component
    // actually requested pages 2 and 3.
    const calls = fetchMock.mock.calls.map(c => String(c[0]));
    const attemptCalls = calls.filter(u => u.includes(`/api/runs/${RUN_ID}/attempts`));
    expect(attemptCalls.some(u => u.includes('offset=200'))).toBe(true);
    expect(attemptCalls.some(u => u.includes('offset=400'))).toBe(true);
  });
});

describe('DetailPage — historical run attempts', () => {
  const OLD_RUN_ID = 'run-old';

  function historicalRoutes() {
    return {
      ...failedRoutes(),
      [`/api/schedules/${SCHED}/runs`]: async () => ({
        runs: [
          { run_id: OLD_RUN_ID, schedule_name: SCHED, status: 'failed', created_at: '2026-09-01T10:00:00Z', completed_at: '2026-09-01T10:05:00Z' },
        ],
      }),
      [`/api/runs/${OLD_RUN_ID}/graph`]: async () => ({
        nodes: [
          { node_id: 'svc1.public.old_orders', node_type: 'dbt-model', schedule_name: SCHED, status: 'failed' },
        ],
        edges: [],
        run_promotion_seq: 6,
        latest_promotion_seq: 7,
      }),
      [`/api/runs/${OLD_RUN_ID}/tasks`]: async () => ({
        total_count: 1,
        tasks: [
          {
            task_id: 'old-t1', service_name: 'svc1', schema_name: 'public', table_name: 'old_orders',
            job_name: '', status: 'failed', attempt: 1, max_retries: 1, created_at: null,
          },
        ],
      }),
      [`/api/runs/${OLD_RUN_ID}/attempts`]: async () => ({
        total_count: 2,
        attempts: [0, 1].map((n): TaskAttempt => ({
          task_id: 'old-t1', attempt: n, job_name: `old-t1-${n}`, status: 'failed',
          failure_class: 'user_code', failure_reason: n === 0 ? 'timeout' : 'compile_error',
          dispatched_at: null, queued_at: null, started_at: null, finished_at: null,
          execution_seconds: null, error_message: `old failure ${n}`,
          log_s3_key: `logs/old-t1-${n}.log`, run_results_uri: null,
        })),
      }),
    };
  }

  it('shows the attempts of a historical run', async () => {
    const fetchMock = mockFetchSequence(historicalRoutes());
    vi.stubGlobal('fetch', fetchMock);
    try {
      const { container } = render(withRouter({ last_run_id: RUN_ID }));
      expect(await screen.findByText('orders')).toBeInTheDocument();

      fireEvent.click(await screen.findByRole('tab', { name: /past runs/i }));
      await waitFor(() => expect(container.querySelectorAll('.run-row')).toHaveLength(1));
      fireEvent.click(container.querySelector('.run-row')!);
      fireEvent.click(screen.getByRole('tab', { name: /^run$/i }));

      // The past run's rows come from state, not from its graph snapshot:
      // the task carries its own attempt, read as attempt+1 / max_retries+1.
      const nameCell = await screen.findByText('old_orders');
      const row = nameCell.closest('tr')!;
      expect(row.querySelector('.nodes-attempt')?.textContent).toBe('2 / 2');
      expect(screen.queryByText('orders')).toBeNull();

      fireEvent.click(row);
      const block = container.querySelector('.remediation-attempts') as HTMLElement;
      expect(block).toBeTruthy();
      const rows = within(block).getAllByRole('row').slice(1);
      expect(rows.map(r => r.querySelector('td')?.textContent)).toEqual(['1', '2']);
      expect(within(block).getByText('user_code · compile_error')).toBeInTheDocument();
      expect(within(rows[0]).getByRole('link', { name: 'open log ↗' }).getAttribute('href'))
        .toContain(encodeURIComponent('logs/old-t1-0.log'));

      const urls = fetchMock.mock.calls.map(c => String(c[0]));
      expect(urls.some(u => u.startsWith(`/api/runs/${OLD_RUN_ID}/tasks`))).toBe(true);
      expect(urls.some(u => u.startsWith(`/api/runs/${OLD_RUN_ID}/attempts`))).toBe(true);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('never shows the latest run\'s rows while a chosen past run is still loading', async () => {
    // The past run's tasks never arrive: the Run tab must read as loading,
    // not keep the latest run's rows under the past run's snapshot strip.
    const fetchMock = mockFetchSequence({
      ...historicalRoutes(),
      [`/api/runs/${OLD_RUN_ID}/tasks`]: () => new Promise(() => {}),
    });
    vi.stubGlobal('fetch', fetchMock);
    try {
      const { container } = render(withRouter({ last_run_id: RUN_ID }));
      expect(await screen.findByText('orders')).toBeInTheDocument();

      fireEvent.click(await screen.findByRole('tab', { name: /past runs/i }));
      await waitFor(() => expect(container.querySelectorAll('.run-row')).toHaveLength(1));
      fireEvent.click(container.querySelector('.run-row')!);
      fireEvent.click(screen.getByRole('tab', { name: /^run$/i }));

      expect(await screen.findByText('Loading run…')).toBeInTheDocument();
      expect(screen.queryByText('orders')).toBeNull();
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe('DetailPage — Run List service grouping', () => {
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
        run_promotion_seq: 7,
        latest_promotion_seq: 7,
      }),
      [`/api/runs/${RUN_ID}/tasks`]: async () => ({
        tasks: [
          {
            task_id: 't1', service_name: 'svc1', schema_name: 'public', table_name: 'orders',
            job_name: '', status: 'succeeded', attempt: 0, max_retries: 0, created_at: null,
          },
          {
            task_id: 't2', service_name: 'svc2', schema_name: 'public', table_name: 'payments',
            job_name: '', status: 'failed', attempt: 0, max_retries: 0, created_at: null,
          },
        ],
      }),
    };
  }

  // Node rows carry the table_name in `.nodes-node-name`; a service is
  // expanded when its rows are present, collapsed when only the group header
  // shows.
  function tableNodeNames(container: HTMLElement): (string | null)[] {
    return [...container.querySelectorAll('.nodes-node-name')].map(el => el.textContent);
  }

  it('defaults service groups to collapsed when several services exist', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(multiServiceRoutes()));
    try {
      const { container } = render(withRouter({ last_run_id: RUN_ID }));

      // One collapsible header per service; the node rows are hidden until a
      // group is opened.
      await waitFor(() => {
        expect(container.querySelectorAll('.nodes-group-row')).toHaveLength(2);
      });
      expect(screen.queryByText('orders')).toBeNull();
      expect(screen.queryByText('payments')).toBeNull();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('expanding a group reveals its node rows while the other stays collapsed', async () => {
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
      // The other service stays collapsed.
      expect(tableNodeNames(container)).not.toContain('payments');
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('collapsing an expanded group hides its node rows again', async () => {
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

      fireEvent.click(svc1Header); // collapse again
      await waitFor(() => {
        expect(tableNodeNames(container)).not.toContain('orders');
      });
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe('DetailPage — panel tabs keep loaded page state', () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it('keeps the loaded Past Runs list after the Past Runs tab is selected', async () => {
    vi.stubGlobal('fetch', mockFetchSequence({
      ...freshRoutes(),
      [`/api/schedules/${SCHED}/runs`]: async () => ({
        runs: [
          { run_id: 'run-a', schedule_name: SCHED, status: 'succeeded', created_at: '2026-09-01T10:00:00Z', completed_at: '2026-09-01T10:01:00Z' },
          { run_id: 'run-b', schedule_name: SCHED, status: 'failed', created_at: '2026-09-02T10:00:00Z', completed_at: '2026-09-02T10:01:00Z' },
        ],
      }),
    }));

    // Arriving from the dashboard card: the last run id travels as navigation state.
    render(withRouter({ last_run_id: RUN_ID }));

    const pastRunsTab = await screen.findByRole('tab', { name: /past runs/i });
    await waitFor(() => expect(within(pastRunsTab).getByText('2')).toBeInTheDocument());

    // Selecting a panel tab only rewrites the query string. That navigation
    // carries no state and must not reset the page.
    fireEvent.click(pastRunsTab);

    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /past runs/i })).toHaveClass('tabs__tab--active');
    });
    expect(within(screen.getByRole('tab', { name: /past runs/i })).getByText('2')).toBeInTheDocument();
    expect(screen.getAllByText(/^(succeeded|failed)$/, { selector: '.pill-sm' })).toHaveLength(2);
    expect(screen.queryByText('No runs yet.')).toBeNull();
  });
});

describe('DetailPage — Run / Past runs tabs', () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it('defaults to the Run tab with the List view, then switches to the swimlane Graph', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(failedRoutes()));
    const { container } = render(withRouter({ last_run_id: RUN_ID }));

    // Run tab is the default active tab, and its List sub-view is the default:
    // the node table row for the run's single node is visible.
    expect(await screen.findByText('orders')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: /^run$/i })).toHaveClass('tabs__tab--active');
    // List is the default view — the swimlane is not mounted yet.
    expect(container.querySelector('.swim-node')).toBeNull();

    // Switching the Run view to Graph mounts the swimlane.
    fireEvent.click(screen.getByRole('button', { name: /^graph$/i }));
    await waitFor(() => expect(container.querySelector('.swim-node')).toBeTruthy());
  });

  it('shows the run-progress header on the Run tab', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(failedRoutes()));
    render(withRouter({ last_run_id: RUN_ID }));

    // The progress header aggregates the live task set; the single failed task
    // means 100% complete (done = succeeded + failed + skipped).
    expect(await screen.findByTestId('run-progress-pct')).toHaveTextContent('100%');
    expect(screen.getByTestId('count-failed')).toHaveTextContent('1');
  });

  it('does not render the old run-view graph card or its focus legend', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(failedRoutes()));
    const { container } = render(withRouter({ last_run_id: RUN_ID }));

    await screen.findByText('orders');
    expect(container.querySelector('.detail-graph-card')).toBeNull();
    expect(container.querySelector('.dag-focus-legend')).toBeNull();
    // The Run tab defaults to the List view, so no graph is rendered there.
    expect(container.querySelector('.react-flow')).toBeNull();
  });
});

describe('DetailPage — Run tab empty/loading states', () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  // A schedule that has never run resolves lastRunId to null, fetches no tasks,
  // and has no runs. The Run tab must show "No runs yet." rather than rendering
  // RunProgressHeader over an empty task list (which reads as a fake 0%).
  function noRunsRoutes() {
    return {
      [`/api/schedules/${SCHED}/graph`]: async () => ({ nodes: [], edges: [] }),
      [`/api/schedules/${SCHED}/runs`]: async () => ({ runs: [] }),
      '/api/schedules': async () => ({
        schedules: [{ schedule_name: SCHED, last_run_id: null }],
      }),
    };
  }

  it('shows "No runs yet." (not a 0% progress bar) for a schedule with no runs/tasks', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(noRunsRoutes()));

    // No navigation state: lastRunId is resolved from /api/schedules and lands
    // on null for a schedule that has never run.
    render(withRouter(null));

    expect(await screen.findByText('No runs yet.')).toBeInTheDocument();
    // The fake progress bar must NOT be rendered for a never-run schedule.
    expect(screen.queryByTestId('run-progress-pct')).toBeNull();
  });
});

describe('DetailPage — brand header', () => {
  it('starts the header with the brand, before the back link', async () => {
    const fetchMock = mockFetchSequence(failedRoutes());
    vi.stubGlobal('fetch', fetchMock);
    const { container } = render(withRouter({ last_run_id: RUN_ID }));
    await screen.findByRole('button', { name: /^▶ Trigger run$/ });

    const header = container.querySelector('.page-header')!;
    const brand = header.querySelector('a.brand');
    expect(brand).toHaveAttribute('href', '/');
    const back = header.querySelector('.detail-back-link')!;
    expect(brand!.compareDocumentPosition(back) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });
});
