// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router';
import NodeDetailPage from '../../src/client/NodeDetailPage';
import type { NodeDetailFrom } from '../../src/client/types';

const mockFetch = vi.fn();
beforeEach(() => {
  mockFetch.mockReset();
  vi.stubGlobal('fetch', mockFetch);
});

function jsonResp(body: unknown, status = 200) {
  return Promise.resolve({ ok: status >= 200 && status < 300, status, json: async () => body } as Response);
}

function renderPage(from?: NodeDetailFrom, navOperation?: string) {
  const state = from || navOperation ? { ...(from ? { from } : {}), ...(navOperation ? { operation: navOperation } : {}) } : null;
  return render(
    <MemoryRouter initialEntries={[{ pathname: '/node/svc.schema.tbl', state }]}>
      <Routes>
        <Route path="/" element={<div>NODES TAB</div>} />
        <Route path="/schedule/:name" element={<div>RUN MODE</div>} />
        <Route path="/schedule/:name/latest" element={<div>LATEST MODE</div>} />
        <Route path="/node/:fqn" element={<NodeDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

// bodyOf parses the JSON body of a recorded POST.
function bodyOf(init: RequestInit | undefined): Record<string, unknown> {
  return JSON.parse(String(init?.body));
}

const mkRun = (over: Partial<{ run_id: string; task_id: string; kind: string; task_status: string; created_at: string; started_at: string | null; completed_at: string | null; error_message: string | null; failure_class: string | null; failure_reason: string | null }>) => ({
  run_id: 'r1', schedule_name: 'daily', kind: 'cron',
  run_status: 'succeeded', task_id: 't1',
  task_status: 'succeeded', attempt: 0, failure_class: null, failure_reason: null,
  image_tag: 'v1',
  created_at: '2026-05-10T10:00:00Z',
  started_at: '2026-05-10T10:00:05Z',
  completed_at: '2026-05-10T10:01:00Z',
  error_message: null, log_s3_key: null,
  ...over,
});

describe('NodeDetailPage', () => {
  it('renders the node FQN in the header', async () => {
    mockFetch.mockImplementation(() => jsonResp({ tasks: [] }));
    renderPage();
    expect(await screen.findByText(/svc\.schema\.tbl/)).toBeInTheDocument();
  });

  it('shows the image tag column and no manifest column in the run history', async () => {
    mockFetch.mockImplementation(() => jsonResp({ tasks: [mkRun({})] }));
    renderPage();
    expect(await screen.findByRole('columnheader', { name: 'Image tag' })).toBeInTheDocument();
    expect(screen.queryByRole('columnheader', { name: /manifest/i })).not.toBeInTheDocument();
  });

  it('renders Run-this-node and Run-with-old-snapshot buttons', async () => {
    mockFetch.mockImplementation(() => jsonResp({ tasks: [] }));
    renderPage();
    expect(await screen.findByRole('button', { name: /run this node/i })).toBeInTheDocument();
    expect(await screen.findByRole('button', { name: /run with old snapshot/i })).toBeInTheDocument();
  });

  it('latest button POSTs /api/nodes/svc/schema/tbl/run with empty body', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url === '/api/nodes/svc/schema/tbl/run' && init?.method === 'POST') {
        return jsonResp({ run_id: 'new-r', schedule_name: 'single-node-run-abc12345' });
      }
      return jsonResp({ tasks: [] });
    });
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /run this node/i }));
    await waitFor(() => {
      const calls = mockFetch.mock.calls as unknown as [string, RequestInit?][];
      const postCall = calls.find(c => String(c[0]).includes('/api/nodes/svc/schema/tbl/run') && c[1]?.method === 'POST');
      expect(postCall).toBeDefined();
      expect(postCall![1]).toMatchObject({ method: 'POST' });
      expect(bodyOf(postCall![1])).toEqual({ request_id: expect.stringMatching(UUID), operation: '' });
    });
  });

  it("shows a failed run's failure class and reason under its status", async () => {
    mockFetch.mockImplementation(() => jsonResp({
      tasks: [mkRun({ run_id: 'r1', task_id: 't1', task_status: 'failed', failure_class: 'task', failure_reason: 'exit_code' })],
    }));
    renderPage();
    expect(await screen.findByText('task · exit_code')).toBeInTheDocument();
  });

  it('titles a queued run "waiting for a slot"', async () => {
    mockFetch.mockImplementation(() => jsonResp({ tasks: [mkRun({ run_id: 'r1', task_id: 't1', task_status: 'queued' })] }));
    renderPage();
    expect(await screen.findByTitle('waiting for a slot')).toHaveTextContent('queued');
  });

  it('shows node-history table with kindLabel and computed stats', async () => {
    mockFetch.mockImplementation(() => jsonResp({
      tasks: [
        mkRun({ run_id: 'r1', task_id: 't1', kind: 'cron', task_status: 'succeeded' }),
        mkRun({ run_id: 'r2', task_id: 't2', kind: 'rerun', task_status: 'failed',
                created_at: '2026-05-10T11:00:00Z',
                started_at: '2026-05-10T11:00:05Z',
                completed_at: '2026-05-10T11:02:00Z',
                error_message: 'boom' }),
      ],
    }));
    renderPage();
    expect(await screen.findByText(/scheduled/i)).toBeInTheDocument();
    expect(await screen.findByText(/manual rerun/i)).toBeInTheDocument();
    expect(await screen.findByText(/50% succeeded/i)).toBeInTheDocument();
  });

  it('"Run with old snapshot" opens the picker dialog', async () => {
    mockFetch.mockImplementation(() => jsonResp({
      tasks: [mkRun({ run_id: 'pick-me' })],
    }));
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /run with old snapshot/i }));
    expect(await screen.findByRole('dialog')).toBeInTheDocument();
    expect(await screen.findByRole('button', { name: /v1 snapshot/i })).toBeInTheDocument();
  });

  it('picking a source run POSTs with source_run_id', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url === '/api/nodes/svc/schema/tbl/run' && init?.method === 'POST') {
        return jsonResp({ run_id: 'new-r', schedule_name: 'single-node-run-abc12345' });
      }
      return jsonResp({ tasks: [mkRun({ run_id: 'pick-me' })] });
    });
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /run with old snapshot/i }));
    fireEvent.click(await screen.findByRole('button', { name: /v1 snapshot/i }));
    await waitFor(() => {
      const calls = mockFetch.mock.calls as unknown as [string, RequestInit?][];
      const postCall = calls.find(c => String(c[0]).includes('/api/nodes/svc/schema/tbl/run') && c[1]?.method === 'POST');
      expect(postCall).toBeDefined();
      expect(postCall![1]).toMatchObject({ method: 'POST' });
      expect(bodyOf(postCall![1])).toEqual({
        request_id: expect.stringMatching(UUID),
        source_run_id: 'pick-me',
        operation: '',
      });
    });
  });

  it('latest button verb tracks the operation select and POSTs the operation', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/run') && init?.method === 'POST') return jsonResp({ run_id: 'r', schedule_name: 's' });
      if (url.endsWith('/meta')) return jsonResp({ node_type: 'dbt-model', test_count: 4, test_count_known: true });
      return jsonResp({ tasks: [] });
    });
    renderPage();
    fireEvent.change(await screen.findByLabelText(/operation/i), { target: { value: 'build' } });
    const btn = await screen.findByRole('button', { name: /build this node/i });
    fireEvent.click(btn);
    await waitFor(() => {
      const calls = mockFetch.mock.calls as unknown as [string, RequestInit?][];
      const post = calls.find(c => String(c[0]).endsWith('/run') && c[1]?.method === 'POST');
      expect(bodyOf(post![1])).toEqual({ request_id: expect.stringMatching(UUID), operation: 'build' });
    });
  });

  it('gates only the latest trigger (not the option or old-snapshot) when the latest topology has known-zero tests', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/meta')) return jsonResp({ node_type: 'dbt-model', test_count: 0, test_count_known: true });
      return jsonResp({ tasks: [] });
    });
    renderPage();
    const select = await screen.findByLabelText(/operation/i) as HTMLSelectElement;
    // Test stays selectable — an old-snapshot test may still be valid.
    expect((screen.getByRole('option', { name: /test/i }) as HTMLOptionElement).disabled).toBe(false);

    fireEvent.change(select, { target: { value: 'test' } });
    // The latest "Test this node" button is disabled and a hint appears...
    await waitFor(() => expect(screen.getByRole('button', { name: /test this node/i })).toBeDisabled());
    expect(document.querySelector('.info-strip--info')).toBeInTheDocument();
    // ...but the old-snapshot trigger stays available.
    expect(screen.getByRole('button', { name: /run with old snapshot/i })).not.toBeDisabled();
  });

  it('allows an old-snapshot test even when the latest topology has no tests', async () => {
    // Regression: the latest test_count must NOT gate the snapshot_of_run path.
    // The backend validates a snapshot_of_run test against the source run's own
    // pinned test_count, so an old-snapshot test stays available (and POSTs
    // operation:test) even when the latest node version has zero tests.
    mockFetch.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/run') && init?.method === 'POST') return jsonResp({ run_id: 'r', schedule_name: 's' });
      if (url.endsWith('/meta')) return jsonResp({ node_type: 'dbt-model', test_count: 0, test_count_known: true });
      return jsonResp({ tasks: [mkRun({ run_id: 'pick-me' })] });
    });
    renderPage();
    fireEvent.change(await screen.findByLabelText(/operation/i), { target: { value: 'test' } });
    const oldSnap = await screen.findByRole('button', { name: /run with old snapshot/i });
    expect(oldSnap).not.toBeDisabled();
    fireEvent.click(oldSnap);
    fireEvent.click(await screen.findByRole('button', { name: /v1 snapshot/i }));
    await waitFor(() => {
      const calls = mockFetch.mock.calls as unknown as [string, RequestInit?][];
      const post = calls.find(c => String(c[0]).endsWith('/run') && c[1]?.method === 'POST');
      expect(bodyOf(post![1])).toEqual({ request_id: expect.stringMatching(UUID), source_run_id: 'pick-me', operation: 'test' });
    });
  });

  it('gates the latest test trigger and shows a hint when test_count is unknown', async () => {
    // An unset test_count (a node predating test_count capture) has no known
    // tests, so it gates the latest test trigger exactly like a known zero — only
    // a known, positive test_count is runnable. The old-snapshot path stays open.
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/meta')) return jsonResp({ node_type: 'dbt-model', test_count: 0, test_count_known: false });
      return jsonResp({ tasks: [] });
    });
    renderPage();
    fireEvent.change(await screen.findByLabelText(/operation/i), { target: { value: 'test' } });
    await waitFor(() => expect(screen.getByRole('button', { name: /test this node/i })).toBeDisabled());
    expect(document.querySelector('.info-strip--info')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /run with old snapshot/i })).not.toBeDisabled();
  });

  it('changing the operation select refetches history scoped to that operation', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/meta')) return jsonResp({ node_type: 'dbt-model', test_count: 4, test_count_known: true });
      return jsonResp({ tasks: [] });
    });
    renderPage();
    fireEvent.change(await screen.findByLabelText(/operation/i), { target: { value: 'test' } });
    await waitFor(() => {
      const calls = mockFetch.mock.calls as unknown as [string, RequestInit?][];
      const runsCall = calls.find(c => String(c[0]).includes('/runs?operation=test'));
      expect(runsCall).toBeDefined();
    });
  });

  it('shows the build info-strip explaining combined model+test stats when operation is build', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/meta')) return jsonResp({ node_type: 'dbt-model', test_count: 4, test_count_known: true });
      return jsonResp({ tasks: [] });
    });
    renderPage();
    expect(document.querySelector('.info-strip--info')).toBeNull();
    fireEvent.change(await screen.findByLabelText(/operation/i), { target: { value: 'build' } });
    await waitFor(() => {
      expect(screen.getByText(/materializes each model and runs its/i)).toBeInTheDocument();
    });
  });

  it('picking an old snapshot POSTs the selected operation alongside source_run_id', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/run') && init?.method === 'POST') return jsonResp({ run_id: 'r', schedule_name: 's' });
      if (url.endsWith('/meta')) return jsonResp({ node_type: 'dbt-model', test_count: 4, test_count_known: true });
      return jsonResp({ tasks: [mkRun({ run_id: 'pick-me' })] });
    });
    renderPage();
    fireEvent.change(await screen.findByLabelText(/operation/i), { target: { value: 'test' } });
    fireEvent.click(await screen.findByRole('button', { name: /run with old snapshot/i }));
    fireEvent.click(await screen.findByRole('button', { name: /v1 snapshot/i }));
    await waitFor(() => {
      const calls = mockFetch.mock.calls as unknown as [string, RequestInit?][];
      const post = calls.find(c => String(c[0]).endsWith('/run') && c[1]?.method === 'POST');
      expect(bodyOf(post![1])).toEqual({ request_id: expect.stringMatching(UUID), source_run_id: 'pick-me', operation: 'test' });
    });
  });
});

describe('NodeDetailPage — section-header alignment', () => {
  it('renders the section title in .section-header__title', async () => {
    mockFetch.mockImplementation(() => jsonResp({ tasks: [] }));
    renderPage();
    await waitFor(() => {
      const title = document.querySelector('.section-header__title');
      expect(title).not.toBeNull();
      expect(title?.textContent?.trim()).toBe('Node history');
    });
  });

  it('renders the run count in .section-header__count', async () => {
    mockFetch.mockImplementation(() => jsonResp({
      tasks: [
        mkRun({ run_id: 'r1', task_id: 't1' }),
        mkRun({ run_id: 'r2', task_id: 't2' }),
      ],
    }));
    renderPage();
    await waitFor(() => {
      const count = document.querySelector('.section-header__count');
      expect(count).not.toBeNull();
      expect(count?.textContent?.trim()).toBe('2');
    });
  });

  it('renders the metadata summary in .section-header__sub', async () => {
    mockFetch.mockImplementation(() => jsonResp({
      tasks: [
        mkRun({ run_id: 'r1', task_id: 't1', task_status: 'succeeded' }),
      ],
    }));
    renderPage();
    await waitFor(() => {
      const sub = document.querySelector('.section-header__sub');
      expect(sub).not.toBeNull();
      expect(sub?.textContent).toMatch(/succeeded|no terminal runs/);
      expect(sub?.textContent).toMatch(/avg/);
    });
  });

  it('does not render the truly orphan classes anywhere', async () => {
    mockFetch.mockImplementation(() => jsonResp({ tasks: [] }));
    const { container } = renderPage();
    await waitFor(() => {
      expect(container.querySelector('.node-stats-summary')).toBeNull();
      expect(container.querySelector('.nodes-log-link')).toBeNull();
    });
  });
});

describe('NodeDetailPage — foundations', () => {
  it('wraps in .page + .page-header (legacy .node-detail-page is gone)', async () => {
    mockFetch.mockImplementation(() => jsonResp({ tasks: [] }));
    const { container } = renderPage();
    await screen.findByText(/svc\.schema\.tbl/);
    expect(container.querySelector('.page')).toBeInTheDocument();
    expect(container.querySelector('.page-header')).toBeInTheDocument();
    expect(container.querySelector('.node-detail-page')).toBeNull();
  });

  it('both action buttons use .btn.btn--secondary (no legacy .rerun-btn)', async () => {
    mockFetch.mockImplementation(() => jsonResp({ tasks: [] }));
    renderPage();
    const runLatest = await screen.findByRole('button', { name: /run this node/i });
    const runOld = await screen.findByRole('button', { name: /run with old snapshot/i });
    [runLatest, runOld].forEach(btn => {
      expect(btn.className).toMatch(/\bbtn\b/);
      expect(btn.className).toMatch(/\bbtn--secondary\b/);
      expect(btn.className).not.toMatch(/\brerun-btn\b/);
    });
  });

  it('"Run this node" shows past-tense Triggered with .is-success after click', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url === '/api/nodes/svc/schema/tbl/run' && init?.method === 'POST') {
        return jsonResp({ run_id: 'new-r', schedule_name: 'single-node-run-abc12345' });
      }
      return jsonResp({ tasks: [] });
    });
    const user = userEvent.setup();
    renderPage();
    const btn = await screen.findByRole('button', { name: /run this node/i });
    await user.click(btn);
    await waitFor(() => {
      expect(btn).toHaveTextContent(/^Triggered$/);
      expect(btn.className).toMatch(/\bis-success\b/);
    });
  });

  it('error renders as full-width .info-strip--error after a failed POST', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url === '/api/nodes/svc/schema/tbl/run' && init?.method === 'POST') {
        return jsonResp({ error: 'server blew up' }, 500);
      }
      return jsonResp({ tasks: [] });
    });
    const user = userEvent.setup();
    const { container } = renderPage();
    await user.click(await screen.findByRole('button', { name: /run this node/i }));
    await waitFor(() => {
      const strip = container.querySelector('.info-strip--error');
      expect(strip).toBeInTheDocument();
    });
  });

  it('long error message in the runs table is truncated and expandable', async () => {
    const longError = 'A'.repeat(300) + ' this error message is very long and should be truncated';
    mockFetch.mockImplementation(() => jsonResp({
      tasks: [mkRun({ run_id: 'r-long', task_id: 't-long', task_status: 'failed', error_message: longError })],
    }));
    const user = userEvent.setup();
    renderPage();
    // Wait for the run to appear, then check truncated + toggle
    expect(await screen.findByRole('button', { name: /^more$/i })).toBeInTheDocument();
    // Click "more" to expand
    await user.click(screen.getByRole('button', { name: /^more$/i }));
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /^less$/i })).toBeInTheDocument();
    });
    // Click "less" to collapse
    await user.click(screen.getByRole('button', { name: /^less$/i }));
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /^more$/i })).toBeInTheDocument();
    });
  });
});

describe('NodeDetailPage — task timeout', () => {
  const mkSummary = (over: Partial<{ schema_name: string; timeout_seconds: number; timeout_declared: boolean }>) => ({
    service_name: 'svc', schema_name: 'schema', table_name: 'tbl',
    task_count: 1, success_rate_pct: null, avg_duration_sec: null, p95_duration_sec: null,
    last_status: null, last_run_at: null, operation: 'run', timeout_seconds: 86400, timeout_declared: false,
    ...over,
  });

  // Answers the node list search like run-controller does: the search term is a
  // case-insensitive exact match on the table name, and nothing else.
  function serveRows(rows: ReturnType<typeof mkSummary>[]) {
    serve(url => {
      const search = (new URL(url, 'http://x').searchParams.get('search') ?? '').toLowerCase();
      const nodes = rows.filter(r => r.table_name.toLowerCase() === search);
      return jsonResp({ total_count: nodes.length, nodes });
    });
  }

  function serve(summary: (url: string) => Promise<Response>) {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.startsWith('/api/nodes?')) return summary(url);
      if (url.endsWith('/meta')) return jsonResp({ node_type: 'dbt-model' });
      return jsonResp({ tasks: [] });
    });
  }

  it('shows the declared task timeout', async () => {
    serveRows([mkSummary({ timeout_seconds: 28800, timeout_declared: true })]);
    renderPage();
    expect(await screen.findByText('task timeout 8 h · declared')).toBeInTheDocument();
    const calls = mockFetch.mock.calls as unknown as [string][];
    expect(calls.some(c => c[0] === '/api/nodes?service=svc&search=tbl&operation=run')).toBe(true);
  });

  it('shows a declared timeout below one hour in minutes', async () => {
    serveRows([mkSummary({ timeout_seconds: 1800, timeout_declared: true })]);
    renderPage();
    expect(await screen.findByText('task timeout 30 min · declared')).toBeInTheDocument();
  });

  it('shows the install default, with its length, when none is declared', async () => {
    serveRows([mkSummary({ timeout_seconds: 86400, timeout_declared: false })]);
    renderPage();
    expect(await screen.findByText('task timeout 24 h · install default')).toBeInTheDocument();
  });

  it('shows no task timeout when the node has no summary', async () => {
    serveRows([]);
    renderPage();
    await screen.findByText('dbt-model');
    expect(screen.queryByText(/task timeout/)).not.toBeInTheDocument();
  });

  it('shows no task timeout, and still renders the page, when the summary fetch fails', async () => {
    serve(() => Promise.reject(new Error('network down')));
    renderPage();
    await screen.findByText('dbt-model');
    expect(screen.queryByText(/task timeout/)).not.toBeInTheDocument();
  });

  it('shows no task timeout when the summary request is rejected by the server', async () => {
    serve(() => jsonResp({ error: 'boom' }, 502));
    renderPage();
    await screen.findByText('dbt-model');
    expect(screen.queryByText(/task timeout/)).not.toBeInTheDocument();
  });

  it('ignores a summary of another schema with the same table name', async () => {
    serveRows([mkSummary({ schema_name: 'other', timeout_seconds: 28800 })]);
    renderPage();
    await screen.findByText('dbt-model');
    expect(screen.queryByText(/task timeout/)).not.toBeInTheDocument();
  });

  it('picks the matching schema when several nodes share the table name', async () => {
    serveRows([
      mkSummary({ schema_name: 'other', timeout_seconds: 28800, timeout_declared: true }),
      mkSummary({ schema_name: 'schema', timeout_seconds: 7200, timeout_declared: true }),
    ]);
    renderPage();
    expect(await screen.findByText('task timeout 2 h · declared')).toBeInTheDocument();
  });
});

describe('NodeDetailPage back link', () => {
  it('returns to the schedule (run mode) when from = schedule/run', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/runs')) return jsonResp({ tasks: [] });
      return jsonResp({});
    });
    renderPage({ type: 'schedule', name: 'daily', mode: 'run' });
    const back = await screen.findByRole('button', { name: /back to daily/i });
    fireEvent.click(back);
    await waitFor(() => expect(screen.getByText('RUN MODE')).toBeInTheDocument());
  });

  it('returns to the schedule (latest mode) when from = schedule/latest', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/runs')) return jsonResp({ tasks: [] });
      return jsonResp({});
    });
    renderPage({ type: 'schedule', name: 'daily', mode: 'latest' });
    const back = await screen.findByRole('button', { name: /back to daily/i });
    fireEvent.click(back);
    await waitFor(() => expect(screen.getByText('LATEST MODE')).toBeInTheDocument());
  });

  it('defaults to the Nodes tab on a deep link with no state', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/runs')) return jsonResp({ tasks: [] });
      return jsonResp({});
    });
    renderPage();
    const back = await screen.findByRole('button', { name: /back to nodes/i });
    fireEvent.click(back);
    await waitFor(() => expect(screen.getByText('NODES TAB')).toBeInTheDocument());
  });
});

describe('NodeDetailPage — operation carried from node list navigation', () => {
  it('initializes the operation selector from location.state and fetches operation=test first', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/meta')) return jsonResp({});
      return jsonResp({ tasks: [] });
    });
    renderPage(undefined, 'test');
    const select = await screen.findByLabelText(/operation/i) as HTMLSelectElement;
    expect(select.value).toBe('test');
    await waitFor(() => {
      const calls = mockFetch.mock.calls as unknown as [string, RequestInit?][];
      expect(calls.some(c => String(c[0]).includes('/runs?operation=test'))).toBe(true);
    });
  });

  it('falls back to run when nav state carries a value outside run|test|build', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/meta')) return jsonResp({});
      return jsonResp({ tasks: [] });
    });
    renderPage(undefined, 'bogus');
    const select = await screen.findByLabelText(/operation/i) as HTMLSelectElement;
    expect(select.value).toBe('run');
  });

  it('carries both the back-link "from" and the operation together', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/meta')) return jsonResp({});
      return jsonResp({ tasks: [] });
    });
    renderPage({ type: 'schedule', name: 'daily', mode: 'run' }, 'build');
    expect(await screen.findByRole('button', { name: /back to daily/i })).toBeInTheDocument();
    const select = await screen.findByLabelText(/operation/i) as HTMLSelectElement;
    expect(select.value).toBe('build');
  });
});

describe('NodeDetailPage — history fetch race guard', () => {
  function deferred<T>() {
    let resolve!: (v: T) => void;
    const promise = new Promise<T>(res => { resolve = res; });
    return { promise, resolve };
  }

  it('a stale slow run response does not overwrite a newer test response', async () => {
    const runDeferred = deferred<Response>();
    const testDeferred = deferred<Response>();
    const runResp = { tasks: [mkRun({ run_id: 'r-run', task_id: 't-run' })].map(r => ({ ...r, image_tag: 'run-img' })) };
    const testResp = { tasks: [mkRun({ run_id: 'r-test', task_id: 't-test' })].map(r => ({ ...r, image_tag: 'test-img' })) };

    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.includes('/runs?operation=run')) return runDeferred.promise;
      if (url.includes('/runs?operation=test')) return testDeferred.promise;
      if (url.endsWith('/meta')) return jsonResp({});
      return jsonResp({ tasks: [] });
    });

    renderPage();
    await waitFor(() => {
      expect(mockFetch.mock.calls.some(c => String(c[0]).includes('/runs?operation=run'))).toBe(true);
    });

    fireEvent.change(await screen.findByLabelText(/operation/i), { target: { value: 'test' } });
    await waitFor(() => {
      expect(mockFetch.mock.calls.some(c => String(c[0]).includes('/runs?operation=test'))).toBe(true);
    });

    // Newer (test) request resolves first...
    testDeferred.resolve({ ok: true, json: async () => testResp } as Response);
    await waitFor(() => expect(screen.getByText('test-img')).toBeInTheDocument());

    // ...then the older, now-stale (run) request resolves. Without the
    // generation guard this would clobber the correct test-img row.
    runDeferred.resolve({ ok: true, json: async () => runResp } as Response);
    await new Promise(r => setTimeout(r, 0));
    expect(screen.getByText('test-img')).toBeInTheDocument();
    expect(screen.queryByText('run-img')).toBeNull();
  });
});

describe('NodeDetailPage — brand header', () => {
  it('starts the header with the brand, before the back link', async () => {
    mockFetch.mockImplementation(() => jsonResp({ tasks: [] }));
    const { container } = renderPage();
    await screen.findByText(/svc\.schema\.tbl/);

    const header = container.querySelector('.page-header')!;
    const brand = header.querySelector('a.brand');
    expect(brand).toHaveAttribute('href', '/');
    const back = header.querySelector('.detail-back-link')!;
    expect(brand!.compareDocumentPosition(back) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });
});

function metaAndRuns(nodeType: string, runs: unknown[] = [], meta: { inactive?: boolean; test_count?: number; test_count_known?: boolean } = {}) {
  return (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString();
    if (url.endsWith('/meta')) return jsonResp({ node_type: nodeType, test_count: 1, test_count_known: true, ...meta });
    if (url.endsWith('/run') && init?.method === 'POST') return jsonResp({ run_id: 'fr', schedule_name: 'single-node-run-00ff00ff' });
    return jsonResp({ tasks: runs });
  };
}

describe('NodeDetailPage full refresh', () => {
  it.each(['dbt-model', 'dbt-seed'])('offers Full refresh for %s', async (nt) => {
    mockFetch.mockImplementation(metaAndRuns(nt));
    renderPage();
    const select = await screen.findByLabelText(/operation/i);
    await waitFor(() => expect(screen.getByRole('option', { name: /full refresh/i })).toBeInTheDocument());
    expect(select).toBeInTheDocument();
  });

  it.each(['dbt-snapshot', 'python-node', 'python-csv', 'dbt-test'])('hides Full refresh for %s', async (nt) => {
    mockFetch.mockImplementation(metaAndRuns(nt));
    renderPage();
    await screen.findByText(nt);
    expect(screen.queryByRole('option', { name: /full refresh/i })).toBeNull();
  });

  it('asks for confirmation and only POSTs full_refresh after confirming', async () => {
    mockFetch.mockImplementation(metaAndRuns('dbt-model'));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), 'full_refresh');
    await userEvent.click(screen.getByRole('button', { name: /full refresh this node/i }));

    const dialog = await screen.findByRole('dialog');
    expect(dialog).toHaveTextContent(/dropped and rebuilt/i);
    expect(screen.getByRole('button', { name: /cancel/i })).toHaveFocus();
    const posts = () => (mockFetch.mock.calls as unknown as [string, RequestInit?][])
      .filter(c => String(c[0]).endsWith('/run') && c[1]?.method === 'POST');
    expect(posts()).toHaveLength(0);

    await userEvent.click(screen.getByRole('button', { name: /^full refresh$/i }));
    await waitFor(() => expect(posts()).toHaveLength(1));
    expect(bodyOf(posts()[0][1])).toEqual({ request_id: expect.stringMatching(UUID), operation: 'full_refresh' });
  });

  it('cancelling the dialog sends nothing', async () => {
    mockFetch.mockImplementation(metaAndRuns('dbt-seed'));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), 'full_refresh');
    await userEvent.click(screen.getByRole('button', { name: /full refresh this node/i }));
    await userEvent.click(await screen.findByRole('button', { name: /cancel/i }));
    expect(screen.queryByRole('dialog')).toBeNull();
    const posted = (mockFetch.mock.calls as unknown as [string, RequestInit?][]).some(c => c[1]?.method === 'POST');
    expect(posted).toBe(false);
  });

  it('fetches run history while Full refresh is selected and badges full-refresh rows', async () => {
    mockFetch.mockImplementation(metaAndRuns('dbt-model', [mkRun({ task_id: 'fr1', kind: 'single_node_run' }) , { ...mkRun({ task_id: 'fr2' }), operation: 'full_refresh' }]));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    const runsFetchesBefore = (mockFetch.mock.calls as unknown as [string][])
      .filter(c => String(c[0]).includes('/runs?')).length;
    expect(runsFetchesBefore).toBeGreaterThan(0);
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), 'full_refresh');
    const urls = (mockFetch.mock.calls as unknown as [string][]).map(c => String(c[0]));
    expect(urls.some(u => u.includes('/runs?operation=full_refresh'))).toBe(false);
    // Full refresh keeps the history on the run dimension, so selecting it
    // triggers no further history fetch.
    expect(urls.filter(u => u.includes('/runs?')).slice(runsFetchesBefore).length).toBe(0);
    expect(await screen.findByText(/^full refresh$/i, { selector: '.pill-sm' })).toBeInTheDocument();
  });

  it('resets to Run when navigated in with full_refresh onto an unsupported node', async () => {
    mockFetch.mockImplementation(metaAndRuns('dbt-snapshot'));
    renderPage(undefined, 'full_refresh');
    await screen.findByText('dbt-snapshot');
    await waitFor(() => expect((screen.getByLabelText(/operation/i) as HTMLSelectElement).value).toBe('run'));
    expect(screen.getByRole('button', { name: /run this node/i })).toBeInTheDocument();
  });

  const runPosts = () => (mockFetch.mock.calls as unknown as [string, RequestInit?][])
    .filter(c => String(c[0]).endsWith('/run') && c[1]?.method === 'POST');

  it('confirms a full refresh of an old snapshot before POSTing source_run_id', async () => {
    mockFetch.mockImplementation(metaAndRuns('dbt-model', [mkRun({ run_id: 'pick-me' })]));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), 'full_refresh');
    await userEvent.click(screen.getByRole('button', { name: /run with old snapshot/i }));
    await userEvent.click(await screen.findByRole('button', { name: /v1 snapshot/i }));

    expect(await screen.findByRole('dialog')).toHaveTextContent(/full refresh svc\.schema\.tbl\?/i);
    expect(runPosts()).toHaveLength(0);

    await userEvent.click(screen.getByRole('button', { name: /^full refresh$/i }));
    await waitFor(() => expect(runPosts()).toHaveLength(1));
    expect(JSON.parse(String(runPosts()[0][1]!.body))).toEqual({ request_id: expect.stringMatching(UUID), source_run_id: 'pick-me', operation: 'full_refresh' });
  });

  it('names the picked snapshot in the full refresh confirmation', async () => {
    const picked = { ...mkRun({ run_id: 'pick-me', created_at: '2026-04-02T08:30:00Z' }), image_tag: 'v7' };
    mockFetch.mockImplementation(metaAndRuns('dbt-model', [picked]));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), 'full_refresh');
    await userEvent.click(screen.getByRole('button', { name: /run with old snapshot/i }));
    await userEvent.click(await screen.findByRole('button', { name: /v7 snapshot/i }));

    const dialog = await screen.findByRole('dialog');
    const when = new Date('2026-04-02T08:30:00Z').toLocaleString();
    expect(dialog).toHaveTextContent(`Rebuilds from the snapshot of the run on ${when} (image v7).`);
    expect(screen.getByRole('button', { name: /^cancel$/i })).toHaveFocus();
  });

  it('shows no snapshot line when the full refresh runs the latest version', async () => {
    mockFetch.mockImplementation(metaAndRuns('dbt-model', [mkRun({ run_id: 'pick-me' })]));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), 'full_refresh');
    await userEvent.click(screen.getByRole('button', { name: /full refresh this node/i }));

    const dialog = await screen.findByRole('dialog');
    expect(dialog).toHaveTextContent(/full refresh svc\.schema\.tbl\?/i);
    expect(dialog).not.toHaveTextContent(/rebuilds from the snapshot/i);
  });

  it('POSTs full_refresh even if the operation select changes while the dialog is open', async () => {
    mockFetch.mockImplementation(metaAndRuns('dbt-model'));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), 'full_refresh');
    await userEvent.click(screen.getByRole('button', { name: /full refresh this node/i }));
    await screen.findByRole('dialog');

    fireEvent.change(screen.getByLabelText(/operation/i), { target: { value: 'run' } });
    await userEvent.click(screen.getByRole('button', { name: /^full refresh$/i }));
    await waitFor(() => expect(runPosts()).toHaveLength(1));
    expect(JSON.parse(String(runPosts()[0][1]!.body))).toEqual({ request_id: expect.stringMatching(UUID), operation: 'full_refresh' });
  });

  it.each([
    ['meta never resolves', () => new Promise<Response>(() => {})],
    ['meta fails', () => jsonResp({ error: 'boom' }, 500)],
  ])('disables both triggers for a navigated-in full_refresh while the node type is unknown (%s)', async (_n, metaResp) => {
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url.endsWith('/meta')) return metaResp();
      return jsonResp({ tasks: [] });
    });
    renderPage(undefined, 'full_refresh');
    const trigger = await screen.findByRole('button', { name: /full refresh this node/i });
    expect(trigger).toBeDisabled();
    expect(screen.getByRole('button', { name: /run with old snapshot/i })).toBeDisabled();
    fireEvent.click(trigger);
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(runPosts()).toHaveLength(0);
  });
  it('offers a full refresh of an inactive node from an old snapshot only', async () => {
    mockFetch.mockImplementation(metaAndRuns('dbt-model', [mkRun({ run_id: 'old-run' })], { inactive: true }));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), 'full_refresh');

    const latest = screen.getByRole('button', { name: /full refresh this node/i });
    expect(latest).toBeDisabled();
    expect(screen.getByText(/no longer active in the topology/i)).toBeInTheDocument();
    fireEvent.click(latest);
    expect(screen.queryByRole('dialog')).toBeNull();

    const oldSnapshot = screen.getByRole('button', { name: /run with old snapshot/i });
    expect(oldSnapshot).toBeEnabled();
    await userEvent.click(oldSnapshot);
    await userEvent.click(await screen.findByRole('button', { name: /v1 snapshot/i }));
    expect(await screen.findByRole('dialog')).toHaveTextContent(/full refresh svc\.schema\.tbl\?/i);
    expect(runPosts()).toHaveLength(0);

    await userEvent.click(screen.getByRole('button', { name: /^full refresh$/i }));
    await waitFor(() => expect(runPosts()).toHaveLength(1));
    expect(JSON.parse(String(runPosts()[0][1]!.body))).toEqual({ request_id: expect.stringMatching(UUID), source_run_id: 'old-run', operation: 'full_refresh' });
  });

  it.each([
    ['run', /run this node/i],
    ['test', /test this node/i],
    ['build', /build this node/i],
    ['full_refresh', /full refresh this node/i],
  ])('disables the latest %s action for an inactive node and keeps the old-snapshot trigger', async (op, name) => {
    mockFetch.mockImplementation(metaAndRuns('dbt-model', [mkRun({ run_id: 'old-run' })], { inactive: true }));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), op);

    expect(screen.getByRole('button', { name })).toBeDisabled();
    expect(screen.getByRole('button', { name: /run with old snapshot/i })).toBeEnabled();
    expect(screen.getAllByText(/no longer active in the topology/i)).toHaveLength(1);
  });

  it('shows only the inactive strip, not the no-tests strip, for an inactive node with no tests', async () => {
    mockFetch.mockImplementation(metaAndRuns('dbt-model', [], { inactive: true, test_count: 0, test_count_known: true }));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), 'test');

    expect(screen.getByRole('button', { name: /test this node/i })).toBeDisabled();
    expect(screen.queryByText(/has no tests/i)).toBeNull();
    expect(screen.getAllByText(/no longer active in the topology/i)).toHaveLength(1);
  });

  it('keeps the latest full refresh trigger enabled for an active node', async () => {
    mockFetch.mockImplementation(metaAndRuns('dbt-seed', [], { inactive: false }));
    renderPage();
    await waitFor(() => screen.getByRole('option', { name: /full refresh/i }));
    await userEvent.selectOptions(screen.getByLabelText(/operation/i), 'full_refresh');
    expect(screen.getByRole('button', { name: /full refresh this node/i })).toBeEnabled();
    expect(screen.queryByText(/no longer active in the topology/i)).toBeNull();
  });
});

describe('NodeDetailPage — request id', () => {
  // Answers each run POST with the next reply (a rejection for a request that
  // got no answer) and returns the request ids the POSTs carried.
  // readHistory, when given, answers the node's history reads in place of
  // history.
  function installRun(
    replies: Array<'timeout' | { status: number; body: unknown }>,
    history: unknown[] = [],
    readHistory?: () => Promise<Response>,
  ) {
    const ids: string[] = [];
    let n = 0;
    mockFetch.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url === '/api/nodes/svc/schema/tbl/run' && init?.method === 'POST') {
        ids.push(JSON.parse(String(init.body)).request_id);
        const reply = replies[n++];
        if (reply === 'timeout') return Promise.reject(new Error('timeout'));
        return jsonResp(reply.body, reply.status);
      }
      if (readHistory && url.includes('/runs?operation=')) return readHistory();
      return jsonResp({ tasks: history });
    });
    return ids;
  }

  it('NodeDetailPage run: the body carries a request id', async () => {
    const ids = installRun([{ status: 200, body: { run_id: 'new-r', schedule_name: 's' } }]);
    renderPage();

    fireEvent.click(await screen.findByRole('button', { name: /run this node/i }));

    await waitFor(() => expect(ids).toHaveLength(1));
    expect(ids[0]).toMatch(UUID);
  });

  it('requestId: a press after a timeout is a retry and sends the same id', async () => {
    const ids = installRun(['timeout', { status: 200, body: { run_id: 'new-r', schedule_name: 's' } }]);
    renderPage();

    fireEvent.click(await screen.findByRole('button', { name: /run this node/i }));
    await screen.findByText('Request failed — please try again');
    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toBe(ids[0]);
  });

  it('requestId: a press that asks for another snapshot after a timeout mints a new id', async () => {
    const ids = installRun(
      ['timeout', { status: 200, body: { run_id: 'new-r', schedule_name: 's' } }],
      [mkRun({ run_id: 'pick-me' })],
    );
    renderPage();

    fireEvent.click(await screen.findByRole('button', { name: /run this node/i }));
    await screen.findByText('Request failed — please try again');
    fireEvent.click(screen.getByRole('button', { name: /run with old snapshot/i }));
    fireEvent.click(await screen.findByRole('button', { name: /v1 snapshot/i }));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toMatch(UUID);
    expect(ids[1]).not.toBe(ids[0]);
  });

  it('requestId: a press hours after an unanswered one, with no newer node run in the history, is a retry with the same id', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    try {
      const ids = installRun(
        ['timeout', { status: 200, body: { run_id: 'new-r', schedule_name: 's' } }],
        [mkRun({ run_id: 'r1', kind: 'single_node_run' })],
      );
      renderPage();

      fireEvent.click(await screen.findByRole('button', { name: /run this node/i }));
      await screen.findByText('Request failed — please try again');
      vi.setSystemTime(Date.now() + 6 * 60 * 60 * 1000);
      fireEvent.click(screen.getByRole('button', { name: /run this node/i }));

      await waitFor(() => expect(ids).toHaveLength(2));
      expect(ids[1]).toBe(ids[0]);
    } finally {
      vi.useRealTimers();
    }
  });

  // Lets the reads in flight land.
  const settle = () => act(async () => { await new Promise(r => setTimeout(r, 0)); });

  // The history is read again when the operation changes; a round trip
  // through another operation reads it as it now is.
  async function roundTripOperation() {
    const reads = () => mockFetch.mock.calls.filter(([u]) => String(u).includes('/runs?operation=run')).length;
    const before = reads();
    fireEvent.change(screen.getByLabelText(/operation/i), { target: { value: 'test' } });
    fireEvent.change(screen.getByLabelText(/operation/i), { target: { value: 'run' } });
    await waitFor(() => expect(reads()).toBeGreaterThan(before));
    await settle();
    await screen.findByRole('button', { name: /run this node/i });
  }

  // The history read again shows run as the newest.
  async function rereadHistory(newestFirst: unknown[], run: unknown) {
    newestFirst.unshift(run);
    await roundTripOperation();
  }

  it('requestId: a press after the history shows a node run newer than the unanswered press saw is a new action', async () => {
    const history = [mkRun({ run_id: 'r1', kind: 'single_node_run' })];
    const ids = installRun(['timeout', { status: 200, body: { run_id: 'new-r', schedule_name: 's' } }], history);
    renderPage();
    await screen.findByRole('button', { name: /run this node/i });
    await settle();

    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));
    await screen.findByText('Request failed — please try again');
    await rereadHistory(history, mkRun({ run_id: 'r2', kind: 'single_node_run' }));
    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toMatch(UUID);
    expect(ids[1]).not.toBe(ids[0]);
  });

  it('requestId: a newer scheduled run in the history is not the outcome of the press, so its retry keeps the id', async () => {
    const history = [mkRun({ run_id: 'r1', kind: 'single_node_run' })];
    const ids = installRun(['timeout', { status: 200, body: { run_id: 'new-r', schedule_name: 's' } }], history);
    renderPage();
    await screen.findByRole('button', { name: /run this node/i });
    await settle();

    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));
    await screen.findByText('Request failed — please try again');
    await rereadHistory(history, mkRun({ run_id: 'cron-2', kind: 'cron' }));
    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toBe(ids[0]);
  });

  it('requestId: a history read that fails after an unanswered press is not a newer run, so its retry keeps the id', async () => {
    let history: Promise<Response> = jsonResp({ tasks: [mkRun({ run_id: 'r1', kind: 'single_node_run' })] });
    const ids = installRun(
      ['timeout', 'timeout', { status: 200, body: { run_id: 'new-r', schedule_name: 's' } }],
      [],
      () => history,
    );
    renderPage();
    await screen.findByRole('button', { name: /run this node/i });
    await settle();

    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));
    await screen.findByText('Request failed — please try again');
    history = Promise.reject(new Error('network down'));
    history.catch(() => {});
    await roundTripOperation();
    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));
    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toBe(ids[0]);

    // The history is readable again and shows a node run newer than r1.
    await screen.findByText('Request failed — please try again');
    history = jsonResp({ tasks: [mkRun({ run_id: 'r2', kind: 'single_node_run' }), mkRun({ run_id: 'r1', kind: 'single_node_run' })] });
    await roundTripOperation();
    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));
    await waitFor(() => expect(ids).toHaveLength(3));
    expect(ids[2]).toMatch(UUID);
    expect(ids[2]).not.toBe(ids[0]);
  });

  it('requestId: a history read answered with an error is not a newer run, so its retry keeps the id', async () => {
    let history: Promise<Response> = jsonResp({ tasks: [mkRun({ run_id: 'r1', kind: 'single_node_run' })] });
    const ids = installRun(['timeout', { status: 200, body: { run_id: 'new-r', schedule_name: 's' } }], [], () => history);
    renderPage();
    await screen.findByRole('button', { name: /run this node/i });
    await settle();

    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));
    await screen.findByText('Request failed — please try again');
    history = jsonResp({ error: 'run-controller unavailable' }, 503);
    await roundTripOperation();
    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toBe(ids[0]);
  });

  it('requestId: a node run pushed out of the history window by scheduled runs is not a newer run', async () => {
    const history: unknown[] = [mkRun({ run_id: 'r1', kind: 'single_node_run' })];
    const ids = installRun(['timeout', { status: 200, body: { run_id: 'new-r', schedule_name: 's' } }], history);
    renderPage();
    await screen.findByRole('button', { name: /run this node/i });
    await settle();

    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));
    await screen.findByText('Request failed — please try again');
    history.splice(0, history.length, mkRun({ run_id: 'cron-2', kind: 'cron' }), mkRun({ run_id: 'cron-1', kind: 'cron' }));
    await roundTripOperation();
    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toBe(ids[0]);
  });

  it('requestId: a press after the server answered mints a new id', async () => {
    const ids = installRun([
      { status: 409, body: { error: 'schedule already has an active run' } },
      { status: 200, body: { run_id: 'new-r', schedule_name: 's' } },
    ]);
    renderPage();

    fireEvent.click(await screen.findByRole('button', { name: /run this node/i }));
    await screen.findByText('schedule already has an active run');
    fireEvent.click(screen.getByRole('button', { name: /run this node/i }));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toMatch(UUID);
    expect(ids[1]).not.toBe(ids[0]);
  });
});
