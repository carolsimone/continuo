// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router';
import DetailPage from '../../src/client/DetailPage';

// The schedule page against a backend that stops answering. Each failure is
// the answer the browser really gets: the ui server's HTTP 500 with a JSON
// error body when run-controller cannot be reached, the ingress's plain-text
// 502 when no ui pod can take the request.

const SCHED = 'daily';
const RUN_ID = 'run-1';
const NOTICE = /Can't read the latest data/;

const backendDown = () => new Response(
  JSON.stringify({ error: '14 UNAVAILABLE: No connection established' }),
  { status: 500, headers: { 'content-type': 'application/json' } },
);
const badGateway = () => new Response('Bad Gateway', { status: 502 });

const run = (status: string) => ({
  run_id: RUN_ID, schedule_name: SCHED, status,
  created_at: null, started_at: null, completed_at: null, cancelled_at: null, cancelled_by: '',
});
const graph = {
  nodes: [{ node_id: 'svc1.public.orders', node_type: 'dbt-model', schedule_name: SCHED, status: 'running' }],
  edges: [],
  run_promotion_seq: 1,
  latest_promotion_seq: 1,
};
const tasks = {
  total_count: 1,
  tasks: [{
    task_id: 't1', service_name: 'svc1', schema_name: 'public', table_name: 'orders',
    job_name: 'orders', status: 'running', attempt: 0, max_retries: 0, created_at: null,
  }],
};

// Answers every read from `body`, or with what `failure` returns while one is set.
function stubBackend(failure: () => Response | null, body: (path: string) => unknown = healthyBody) {
  vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
    const failed = failure();
    if (failed) return failed;
    const path = String(input).split('?')[0];
    return new Response(JSON.stringify(body(path)), { status: 200 });
  }));
}

function healthyBody(path: string): unknown {
  if (path === '/api/schedules') return { schedules: [{ schedule_name: SCHED, last_run_id: RUN_ID }] };
  if (path === `/api/schedules/${SCHED}/runs`) return { runs: [], total_count: 3 };
  if (path === `/api/schedules/${SCHED}/graph`) return { nodes: graph.nodes, edges: [] };
  if (path === `/api/runs/${RUN_ID}`) return run('running');
  if (path === `/api/runs/${RUN_ID}/graph`) return graph;
  if (path === `/api/runs/${RUN_ID}/tasks`) return tasks;
  if (path === `/api/runs/${RUN_ID}/attempts`) return { attempts: [], total_count: 0 };
  return {};
}

function renderPage(state: unknown = { last_run_id: RUN_ID }) {
  return render(
    <MemoryRouter initialEntries={[{ pathname: `/schedule/${SCHED}`, state }]}>
      <Routes>
        <Route path="/schedule/:name" element={<DetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

// Only the interval is faked: the page polls through it, while the mocked
// reads keep running on microtasks inside act.
const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

const triggerButton = () => screen.getByRole('button', { name: /Trigger run/ });
const statusPill = (container: HTMLElement) => container.querySelector('.page-header .pill')?.textContent;

describe('DetailPage when reads fail', () => {
  beforeEach(() => { vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] }); });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('keeps showing the live run while the backend is down, says so, and clears the notice when it returns', async () => {
    let down = false;
    stubBackend(() => (down ? backendDown() : null));
    const { container } = renderPage();
    await advance(0);
    expect(screen.getByText('orders', { exact: false })).toBeInTheDocument();
    expect(screen.queryByText(NOTICE)).toBeNull();

    down = true;
    await advance(5000);
    expect(screen.queryByText('Loading run…')).toBeNull();
    expect(screen.getByTestId('run-progress-pct')).toBeInTheDocument();
    expect(statusPill(container)).toBe('running');
    expect(triggerButton()).toBeDisabled();
    expect(screen.getByText(NOTICE)).toBeInTheDocument();

    down = false;
    await advance(5000);
    expect(screen.queryByText(NOTICE)).toBeNull();
    expect(statusPill(container)).toBe('running');
  });

  it('recovers on its own when the page was opened during a non-JSON failure', async () => {
    let down = true;
    stubBackend(() => (down ? badGateway() : null));
    const { container } = renderPage();
    await advance(0);
    expect(screen.getByText('Run unavailable')).toBeInTheDocument();
    expect(screen.queryByText('Loading run…')).toBeNull();
    expect(statusPill(container)).toBe('unavailable');
    expect(screen.getByText(NOTICE)).toBeInTheDocument();

    down = false;
    await advance(5000);
    expect(screen.queryByText('Run unavailable')).toBeNull();
    expect(screen.getByTestId('run-progress-pct')).toBeInTheDocument();
    expect(statusPill(container)).toBe('running');
    expect(screen.queryByText(NOTICE)).toBeNull();
  });

  it('does not crash when the page is opened during a backend error, and recovers on its own', async () => {
    let down = true;
    stubBackend(() => (down ? backendDown() : null));
    const { container } = renderPage();
    await advance(0);
    expect(screen.getByText('Run unavailable')).toBeInTheDocument();
    // The status is unknown, which is not a run that is active.
    expect(triggerButton()).toBeEnabled();

    down = false;
    await advance(5000);
    expect(screen.getByTestId('run-progress-pct')).toBeInTheDocument();
    expect(statusPill(container)).toBe('running');
    expect(triggerButton()).toBeDisabled();
  });

  it('says the run is unavailable within 10 s when the reads hang, and recovers on its own', async () => {
    vi.useRealTimers();
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'setTimeout', 'clearTimeout'] });
    let hanging = true;
    const healthy = vi.fn(async (input: RequestInfo | URL) => (
      new Response(JSON.stringify(healthyBody(String(input).split('?')[0])), { status: 200 })
    ));
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      if (!hanging) return healthy(input);
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')));
      });
    }));
    const { container } = renderPage();
    await advance(0);
    expect(screen.getByText('Loading run…')).toBeInTheDocument();
    expect(screen.queryByText(NOTICE)).toBeNull();

    await advance(10000);
    expect(screen.getByText('Run unavailable')).toBeInTheDocument();
    expect(screen.getByText(NOTICE)).toBeInTheDocument();

    hanging = false;
    await advance(5000);
    expect(screen.getByTestId('run-progress-pct')).toBeInTheDocument();
    expect(statusPill(container)).toBe('running');
    expect(screen.queryByText(NOTICE)).toBeNull();
  });

  it('does not say a schedule never ran when the schedule list cannot be read', async () => {
    let down = true;
    stubBackend(() => (down ? backendDown() : null));
    const { container } = renderPage(null);
    await advance(0);
    expect(screen.queryByText('No runs yet.')).toBeNull();
    expect(statusPill(container)).not.toBe('never run');
    expect(screen.getByText('Run unavailable')).toBeInTheDocument();

    down = false;
    await advance(5000);
    expect(screen.getByTestId('run-progress-pct')).toBeInTheDocument();
  });

  it('says a run has no tasks rather than loading it forever', async () => {
    stubBackend(() => null, (path) => {
      if (path === `/api/runs/${RUN_ID}`) return { ...run('failed'), failure_reason: 'empty_selection' };
      if (path === `/api/runs/${RUN_ID}/tasks`) return { tasks: [], total_count: 0 };
      if (path === `/api/runs/${RUN_ID}/graph`) return { ...graph, nodes: [] };
      return healthyBody(path);
    });
    renderPage();
    await advance(0);
    expect(screen.getByText('This run has no tasks.')).toBeInTheDocument();
    expect(screen.queryByText('Loading run…')).toBeNull();
  });
});
