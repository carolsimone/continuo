// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
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

const mkRun = (over: Partial<{ run_id: string; kind: string; task_status: string; created_at: string; started_at: string | null; completed_at: string | null; error_message: string | null }>) => ({
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
    expect(await screen.findByRole('button', { name: /run this node \(latest\)/i })).toBeInTheDocument();
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
    fireEvent.click(await screen.findByRole('button', { name: /run this node \(latest\)/i }));
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
        mkRun({ run_id: 'r1', kind: 'cron', task_status: 'succeeded' }),
        mkRun({ run_id: 'r2', kind: 'rerun', task_status: 'failed',
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
