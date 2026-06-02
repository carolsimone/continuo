// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act, waitFor } from '@testing-library/react';
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

describe('DashboardPage — shell', () => {
  it('renders inside the .page / .page-header foundation', async () => {
    const { container } = renderPage();
    await act(async () => { await Promise.resolve(); });

    expect(container.querySelector('.page')).toBeInTheDocument();
    expect(container.querySelector('.page-header')).toBeInTheDocument();
    expect(container.querySelector('.page-content--readable')).toBeInTheDocument();
    expect(container.querySelector('.app')).toBeNull();
    expect(container.querySelector('.app-header')).toBeNull();
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
      expect(document.querySelector('.snapshot-tile-grid')).toBeInTheDocument();
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
