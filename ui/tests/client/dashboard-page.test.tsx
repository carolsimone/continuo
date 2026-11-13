// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
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

  it('renders the brand as a home link inside the heading', async () => {
    const { container } = renderPage();
    await act(async () => { await Promise.resolve(); });

    const brand = container.querySelector('.page-header h1 a.brand');
    expect(brand).toHaveAttribute('href', '/');
    expect(brand?.textContent?.trim()).toBe('continuo');
    expect(brand?.querySelector('img.brand__mark')).toHaveAttribute('src', '/mark-light.svg');
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
                cron: '0 * * * *',
                paused: false,
                next_fire_at: null,
                version: 1,
                updated_by: '',
                updated_at: null,
                model_count: 1,
                cron_words: '',
                description: '',
                timezone: 'UTC',
                has_active_run: false,
                last_run_at: null,
                last_run_status: 'succeeded',
                last_run_id: 'r1',
              },
              {
                schedule_name: 'b',
                cron: '0 * * * *',
                paused: false,
                next_fire_at: null,
                version: 1,
                updated_by: '',
                updated_at: null,
                model_count: 1,
                cron_words: '',
                description: '',
                timezone: 'UTC',
                has_active_run: false,
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
      if (url.startsWith('/api/nodes')) {
        return Promise.resolve({ ok: true, json: async () => ({ total_count: 3 }) });
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

  it('renders the tabs in the order Runs, Nodes, Topology, Releases, Remediation', async () => {
    renderAt(['/']);
    await waitFor(() => {
      const labels = Array.from(document.querySelectorAll('[role="tab"]'))
        .map(t => t.textContent?.replace(/\d+$/, '').trim());
      expect(labels).toEqual(['Runs', 'Nodes', 'Topology', 'Releases', 'Remediation']);
    });
  });

  it('count pills reflect the loaded data on both tabs', async () => {
    renderAt(['/']);
    await waitFor(() => {
      const counts = document.querySelectorAll('.tabs__count');
      expect(counts).toHaveLength(3);
      expect(counts[0].textContent).toBe('2');  // Runs
      expect(counts[1].textContent).toBe('3');  // Nodes
      expect(counts[2].textContent).toBe('1');  // Topology
    });
  });

  it('shows the Dormant tile only when dormant_node_count > 0 and opens the Nodes tab filtered to dormant', async () => {
    const serveTopology = (dormantNodeCount: string | undefined) => {
      fetchMock.mockImplementation((url: string) => {
        if (url === '/api/topology/schedules') {
          return Promise.resolve({
            ok: true,
            json: async () => ({
              schedules: [{ schedule_name: 'a', node_count: 5, last_updated_at: new Date().toISOString() }],
              ...(dormantNodeCount === undefined ? {} : { dormant_node_count: Number(dormantNodeCount) }),
            }),
          });
        }
        if (url.startsWith('/api/nodes?')) {
          return Promise.resolve({ ok: true, json: async () => ({ total_count: 0, nodes: [] }) });
        }
        return Promise.resolve({ ok: true, json: async () => ({ schedules: [], names: [] }) });
      });
    };

    serveTopology('0');
    const none = renderAt(['/?tab=topology']);
    await screen.findByText('a');
    expect(screen.queryByText('Dormant')).toBeNull();
    none.unmount();

    serveTopology(undefined);
    const absent = renderAt(['/?tab=topology']);
    await screen.findByText('a');
    expect(screen.queryByText('Dormant')).toBeNull();
    absent.unmount();

    serveTopology('3');
    renderAt(['/?tab=topology']);
    const tile = (await screen.findByText('3 dormant nodes · run only by hand')).closest('button') as HTMLElement;
    // The Dormant tile comes after the schedule tiles.
    const tiles = Array.from(document.querySelectorAll('.snapshot-tile'));
    expect(tiles.map(t => t.querySelector('.snapshot-tile__name')?.textContent)).toEqual(['a', 'Dormant']);

    fireEvent.click(tile);
    await waitFor(() => {
      expect(screen.getByRole('tab', { name: /nodes/i })).toHaveClass('tabs__tab--active');
    });
    expect(screen.getByLabelText('Dormant only')).toBeChecked();
    await waitFor(() => {
      const listReads = fetchMock.mock.calls.map(([u]) => String(u)).filter(u => u.startsWith('/api/nodes?operation='));
      expect(listReads.some(u => new URLSearchParams(u.split('?')[1]).get('dormant') === '1')).toBe(true);
    });
  });

  it('shows the Dormant tile, and no empty-topology strip, when every node of the live release is dormant', async () => {
    fetchMock.mockImplementation((url: string) => {
      if (url === '/api/topology/schedules') {
        return Promise.resolve({ ok: true, json: async () => ({ schedules: [], dormant_node_count: 2 }) });
      }
      return Promise.resolve({ ok: true, json: async () => ({ schedules: [], total_count: 0 }) });
    });
    renderAt(['/?tab=topology']);
    expect(await screen.findByText('2 dormant nodes · run only by hand')).toBeInTheDocument();
    expect(screen.queryByText(/No topology loaded yet/)).toBeNull();
  });

  it('no longer renders the standalone "DAG Latest Snapshot" h2 heading', async () => {
    renderAt(['/']);
    await act(async () => { await Promise.resolve(); });
    expect(screen.queryByRole('heading', { name: /dag latest snapshot/i })).toBeNull();
  });
});
