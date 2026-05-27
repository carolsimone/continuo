// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import DashboardPage from '../../src/client/DashboardPage';

const fetchMock = vi.fn();

beforeEach(() => {
  fetchMock.mockReset();
  fetchMock.mockResolvedValue({
    ok: true,
    json: async () => ({ schedules: [] }),
  });
  vi.stubGlobal('fetch', fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function renderPage() {
  return render(
    <MemoryRouter>
      <DashboardPage />
    </MemoryRouter>,
  );
}

describe('DashboardPage — shell + Update Graph button', () => {
  it('renders inside the .page / .page-header foundation', async () => {
    const { container } = renderPage();
    await act(async () => { await Promise.resolve(); });

    expect(container.querySelector('.page')).toBeInTheDocument();
    expect(container.querySelector('.page-header')).toBeInTheDocument();
    expect(container.querySelector('.page-content--readable')).toBeInTheDocument();
    expect(container.querySelector('.app')).toBeNull();
    expect(container.querySelector('.app-header')).toBeNull();
  });

  it('Update Graph button uses .btn.btn--secondary', async () => {
    renderPage();
    await act(async () => { await Promise.resolve(); });
    const btn = screen.getByRole('button', { name: /update graph/i });
    expect(btn.className).toMatch(/\bbtn\b/);
    expect(btn.className).toMatch(/\bbtn--secondary\b/);
    expect(btn.className).not.toMatch(/\bupdate-graph-btn\b/);
  });

  it('shows past-tense "Updated" with .is-success after a successful click', async () => {
    fetchMock.mockImplementation((url: string) => {
      if (url === '/api/dashboard/graph-update') {
        return Promise.resolve({ ok: true, json: async () => ({}) });
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({ schedules: [] }),
      });
    });

    const user = userEvent.setup();
    renderPage();
    await act(async () => { await Promise.resolve(); });

    const btn = screen.getByRole('button', { name: /update graph/i });
    await user.click(btn);
    await waitFor(() => {
      expect(btn).toHaveTextContent(/^Updated$/);
      expect(btn.className).toMatch(/\bis-success\b/);
    });
  });

  it('renders graph error as .info-strip--error (not .error-banner)', async () => {
    fetchMock.mockImplementation((url: string) => {
      if (url === '/api/dashboard/graph-update') {
        return Promise.resolve({
          ok: false,
          json: async () => ({ error: 'boom' }),
        });
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({ schedules: [] }),
      });
    });
    const user = userEvent.setup();
    const { container } = renderPage();
    await act(async () => { await Promise.resolve(); });

    await user.click(screen.getByRole('button', { name: /update graph/i }));
    await waitFor(() => {
      const strip = container.querySelector('.info-strip--error');
      expect(strip).toBeInTheDocument();
      expect(strip?.textContent).toMatch(/boom/);
    });
    expect(container.querySelector('.error-banner')).toBeNull();
  });
});

function renderAt(entries: string[]) {
  return render(
    <MemoryRouter initialEntries={entries}>
      <DashboardPage />
    </MemoryRouter>,
  );
}

describe('DashboardPage — tabs', () => {
  beforeEach(() => {
    fetchMock.mockImplementation((url: string) => {
      if (url === '/api/schedules') {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            schedules: [
              {
                schedule_name: 'a',
                cron_expression: '0 * * * *',
                description: '',
                timezone: 'UTC',
                is_running: false,
                last_run_at: null,
                last_run_status: 'succeeded',
                last_run_id: 'r1',
              },
              {
                schedule_name: 'b',
                cron_expression: '0 * * * *',
                description: '',
                timezone: 'UTC',
                is_running: false,
                last_run_at: null,
                last_run_status: 'succeeded',
                last_run_id: 'r2',
              },
            ],
          }),
        });
      }
      if (url === '/api/topology/schedules') {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            schedules: [
              { schedule_name: 'a', node_count: 5, last_updated_at: new Date().toISOString() },
            ],
          }),
        });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    });
  });

  it('defaults to the Runs tab when no query param is present', async () => {
    renderAt(['/']);
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /runs/i })).toHaveClass('tabs__tab--active');
    });
    expect(screen.getByRole('tab', { name: /topology/i })).not.toHaveClass('tabs__tab--active');
  });

  it('selects the Topology tab when the URL has ?tab=topology', async () => {
    renderAt(['/?tab=topology']);
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /topology/i })).toHaveClass('tabs__tab--active');
    });
  });

  it('falls back to Runs when ?tab is unknown', async () => {
    renderAt(['/?tab=garbage']);
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /runs/i })).toHaveClass('tabs__tab--active');
    });
  });

  it('count pills reflect the loaded data on both tabs', async () => {
    renderAt(['/']);
    await waitFor(() => {
      const counts = document.querySelectorAll('.tabs__count');
      expect(counts).toHaveLength(2);
      expect(counts[0].textContent).toBe('2');
      expect(counts[1].textContent).toBe('1');
    });
  });

  it('no longer renders the standalone "DAG Latest Snapshot" h2 heading', async () => {
    renderAt(['/']);
    await act(async () => { await Promise.resolve(); });
    expect(screen.queryByRole('heading', { name: /dag latest snapshot/i })).toBeNull();
  });
});
