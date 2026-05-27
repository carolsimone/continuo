// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import NodeDetailPage from '../../src/client/NodeDetailPage';

const mockFetch = vi.fn();
beforeEach(() => {
  mockFetch.mockReset();
  vi.stubGlobal('fetch', mockFetch);
});

function jsonResp(body: unknown, status = 200) {
  return Promise.resolve({ ok: status >= 200 && status < 300, status, json: async () => body } as Response);
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/schedule/daily/node/svc.schema.tbl']}>
      <Routes>
        <Route path="/schedule/:name/node/:fqn" element={<NodeDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

const mkRun = (over: Partial<{ run_id: string; task_id: string; kind: string; task_status: string; created_at: string; started_at: string | null; completed_at: string | null; error_message: string | null }>) => ({
  run_id: 'r1', schedule_name: 'daily', kind: 'cron',
  terminal_status: 'succeeded', task_id: 't1',
  task_status: 'succeeded', retry_count: 0,
  image_tag: 'v1', manifest_version: 'm1',
  created_at: '2026-05-10T10:00:00Z',
  started_at: '2026-05-10T10:00:05Z',
  completed_at: '2026-05-10T10:01:00Z',
  error_message: null, log_s3_key: null,
  ...over,
});

describe('NodeDetailPage', () => {
  it('renders the node FQN in the header', async () => {
    mockFetch.mockImplementation(() => jsonResp({ runs: [] }));
    renderPage();
    expect(await screen.findByText(/svc\.schema\.tbl/)).toBeInTheDocument();
  });

  it('renders Run-this-node and Run-with-old-snapshot buttons', async () => {
    mockFetch.mockImplementation(() => jsonResp({ runs: [] }));
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
      return jsonResp({ runs: [] });
    });
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /run this node/i }));
    await waitFor(() => {
      const calls = mockFetch.mock.calls as unknown as [string, RequestInit?][];
      const postCall = calls.find(c => String(c[0]).includes('/api/nodes/svc/schema/tbl/run') && c[1]?.method === 'POST');
      expect(postCall).toBeDefined();
      expect(postCall![1]).toMatchObject({ method: 'POST', body: '{}' });
    });
  });

  it('shows node-history table with kindLabel and computed stats', async () => {
    mockFetch.mockImplementation(() => jsonResp({
      runs: [
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
      runs: [mkRun({ run_id: 'pick-me' })],
    }));
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /run with old snapshot/i }));
    expect(await screen.findByRole('dialog')).toBeInTheDocument();
    expect(await screen.findByRole('button', { name: /pick-me/ })).toBeInTheDocument();
  });

  it('picking a source run POSTs with source_run_id', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString();
      if (url === '/api/nodes/svc/schema/tbl/run' && init?.method === 'POST') {
        return jsonResp({ run_id: 'new-r', schedule_name: 'single-node-run-abc12345' });
      }
      return jsonResp({ runs: [mkRun({ run_id: 'pick-me' })] });
    });
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /run with old snapshot/i }));
    fireEvent.click(await screen.findByRole('button', { name: /pick-me/ }));
    await waitFor(() => {
      const calls = mockFetch.mock.calls as unknown as [string, RequestInit?][];
      const postCall = calls.find(c => String(c[0]).includes('/api/nodes/svc/schema/tbl/run') && c[1]?.method === 'POST');
      expect(postCall).toBeDefined();
      expect(postCall![1]).toMatchObject({
        method: 'POST',
        body: JSON.stringify({ source_run_id: 'pick-me' }),
      });
    });
  });
});

describe('NodeDetailPage — section-header alignment', () => {
  it('renders the section title in .section-header__title', async () => {
    mockFetch.mockImplementation(() => jsonResp({ runs: [] }));
    renderPage();
    await waitFor(() => {
      const title = document.querySelector('.section-header__title');
      expect(title).not.toBeNull();
      expect(title?.textContent?.trim()).toBe('Node history');
    });
  });

  it('renders the run count in .section-header__count', async () => {
    mockFetch.mockImplementation(() => jsonResp({
      runs: [
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
      runs: [
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
    mockFetch.mockImplementation(() => jsonResp({ runs: [] }));
    const { container } = renderPage();
    await waitFor(() => {
      expect(container.querySelector('.node-stats-summary')).toBeNull();
      expect(container.querySelector('.nodes-log-link')).toBeNull();
    });
  });
});

describe('NodeDetailPage — foundations', () => {
  it('wraps in .page + .page-header (legacy .node-detail-page is gone)', async () => {
    mockFetch.mockImplementation(() => jsonResp({ runs: [] }));
    const { container } = renderPage();
    await screen.findByText(/svc\.schema\.tbl/);
    expect(container.querySelector('.page')).toBeInTheDocument();
    expect(container.querySelector('.page-header')).toBeInTheDocument();
    expect(container.querySelector('.node-detail-page')).toBeNull();
  });

  it('both action buttons use .btn.btn--secondary (no legacy .rerun-btn)', async () => {
    mockFetch.mockImplementation(() => jsonResp({ runs: [] }));
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
      return jsonResp({ runs: [] });
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
      return jsonResp({ runs: [] });
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
      runs: [mkRun({ run_id: 'r-long', task_id: 't-long', task_status: 'failed', error_message: longError })],
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
