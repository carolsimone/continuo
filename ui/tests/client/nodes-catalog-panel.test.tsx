// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router';
import NodesCatalogPanel from '../../src/client/NodesCatalogPanel';

const page = {
  total_count: 2,
  nodes: [
    { service_name: 'service-1', schema_name: 'an', table_name: 'fct_orders',
      task_count: 51, success_rate_pct: 82, avg_duration_sec: 34, p95_duration_sec: 69,
      last_status: 'failed', last_run_at: '2026-06-08T11:48:00Z' },
    { service_name: 'service-2', schema_name: 'an', table_name: 'dim_products',
      task_count: 40, success_rate_pct: 91, avg_duration_sec: 11, p95_duration_sec: 28,
      last_status: 'succeeded', last_run_at: '2026-06-08T09:00:00Z' },
  ],
};

const fetchMock = vi.fn();
beforeEach(() => {
  fetchMock.mockReset();
  fetchMock.mockImplementation((url: any) => {
    if (String(url).includes('/api/nodes/names')) {
      return Promise.resolve({ ok: true, json: async () => ({ names: ['dim_products', 'fct_orders', 'rpt_revenue'] }) });
    }
    return Promise.resolve({ ok: true, json: async () => page });
  });
  vi.stubGlobal('fetch', fetchMock);
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('NodesCatalogPanel', () => {
  it('renders rows from /api/nodes with node identity', async () => {
    render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
    expect(await screen.findByText('fct_orders')).toBeInTheDocument();
    expect(screen.getByText('dim_products')).toBeInTheDocument();
    expect(screen.getByText(/service-1 · an/)).toBeInTheDocument();
  });

  it('offers the complete distinct-name list as datalist options (independent of the paged catalog)', async () => {
    const { container } = render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
    await screen.findByText('fct_orders');
    const opts = Array.from(container.querySelectorAll('#node-name-options option'))
      .map(o => (o as HTMLOptionElement).value);
    expect(opts).toContain('fct_orders');
    expect(opts).toContain('dim_products');
    expect(opts).toContain('rpt_revenue'); // from /names, NOT present in the catalog page
  });

  it('navigates to /node/:fqn on row click', async () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<NodesCatalogPanel />} />
          <Route path="/node/:fqn" element={<div>NODE DETAIL</div>} />
        </Routes>
      </MemoryRouter>,
    );
    fireEvent.click(await screen.findByText('fct_orders'));
    expect(await screen.findByText('NODE DETAIL')).toBeInTheDocument();
  });

  it('shows empty state when there are no nodes', async () => {
    fetchMock.mockImplementation((url: any) => {
      if (String(url).includes('/api/nodes/names')) {
        return Promise.resolve({ ok: true, json: async () => ({ names: [] }) });
      }
      return Promise.resolve({ ok: true, json: async () => ({ total_count: 0, nodes: [] }) });
    });
    render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
    expect(await screen.findByText(/No node runs yet/i)).toBeInTheDocument();
  });

  it('shows a loading strip, not the empty state, until the first page arrives', async () => {
    let resolvePage!: (v: any) => void;
    fetchMock.mockImplementation((url: any) => {
      if (String(url).includes('/api/nodes/names')) {
        return Promise.resolve({ ok: true, json: async () => ({ names: [] }) });
      }
      return new Promise(r => { resolvePage = r; });
    });
    render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
    expect(screen.getByText(/Loading nodes/i)).toBeInTheDocument();
    expect(screen.queryByText(/No node runs yet/i)).not.toBeInTheDocument();

    await vi.waitFor(() => expect(resolvePage).toBeDefined());
    await act(async () => { resolvePage({ ok: true, json: async () => ({ total_count: 0, nodes: [] }) }); });
    expect(await screen.findByText(/No node runs yet/i)).toBeInTheDocument();
    expect(screen.queryByText(/Loading nodes/i)).not.toBeInTheDocument();
  });

  it('shows no node count until the first page arrives', async () => {
    let resolvePage!: (v: any) => void;
    fetchMock.mockImplementation((url: any) => {
      if (String(url).includes('/api/nodes/names')) {
        return Promise.resolve({ ok: true, json: async () => ({ names: [] }) });
      }
      return new Promise(r => { resolvePage = r; });
    });
    const { container } = render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
    expect(container.querySelector('.section-header__count')).not.toBeInTheDocument();

    await vi.waitFor(() => expect(resolvePage).toBeDefined());
    await act(async () => { resolvePage({ ok: true, json: async () => page }); });
    await screen.findByText('fct_orders');
    expect(container.querySelector('.section-header__count')).toHaveTextContent('2');
  });

  it('says no node matches when a filter returns nothing', async () => {
    fetchMock.mockImplementation((url: any) => {
      if (String(url).includes('/api/nodes/names')) {
        return Promise.resolve({ ok: true, json: async () => ({ names: [] }) });
      }
      if (String(url).includes('search=nope')) {
        return Promise.resolve({ ok: true, json: async () => ({ total_count: 0, nodes: [] }) });
      }
      return Promise.resolve({ ok: true, json: async () => page });
    });
    render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
    await screen.findByText('fct_orders');
    fireEvent.change(screen.getByPlaceholderText(/table name/i), { target: { value: 'nope' } });
    expect(await screen.findByText(/No node matches/i)).toBeInTheDocument();
    expect(screen.queryByText(/No node runs yet/i)).not.toBeInTheDocument();
  });

  it('does not refetch the node list on a timer', async () => {
    vi.useFakeTimers();
    const catalogCalls = () =>
      fetchMock.mock.calls.filter(([url]) => /\/api\/nodes\?/.test(String(url))).length;
    render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
    await act(async () => { await vi.advanceTimersByTimeAsync(250); });
    expect(catalogCalls()).toBe(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
    expect(catalogCalls()).toBe(1);
  });

  it('renders an error strip when the fetch fails', async () => {
    fetchMock.mockImplementation((url: any) => {
      if (String(url).includes('/api/nodes/names')) {
        return Promise.resolve({ ok: true, json: async () => ({ names: [] }) });
      }
      return Promise.resolve({ ok: false, json: async () => ({ error: 'boom' }) });
    });
    const { container } = render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
    expect(await screen.findByText(/Failed to load nodes/i)).toBeInTheDocument();
    expect(container.querySelector('.info-strip--error')).toBeInTheDocument();
    // empty-state must NOT show alongside an error
    expect(screen.queryByText(/No node runs yet/i)).not.toBeInTheDocument();
  });

  it('hides Load more when all rows are loaded', async () => {
    render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
    await screen.findByText('fct_orders');
    expect(screen.queryByRole('button', { name: /load more/i })).not.toBeInTheDocument();
  });

  it('ignores a stale response that resolves after a newer one', async () => {
    vi.useFakeTimers();
    let resolveStale!: (v: any) => void;
    const stalePromise = new Promise(r => { resolveStale = r; });
    const pageNew = { total_count: 1, nodes: [
      { service_name: 'svc', schema_name: 'an', table_name: 'NEW_NODE',
        task_count: 1, success_rate_pct: 100, avg_duration_sec: 1, p95_duration_sec: 1,
        last_status: 'succeeded', last_run_at: '2026-06-08T11:00:00Z' } ] };
    const pageStale = { total_count: 99, nodes: [
      { service_name: 'svc', schema_name: 'an', table_name: 'STALE_NODE',
        task_count: 1, success_rate_pct: 0, avg_duration_sec: 1, p95_duration_sec: 1,
        last_status: 'failed', last_run_at: '2026-06-08T10:00:00Z' } ] };
    let catalogCall = 0;
    fetchMock.mockReset();
    fetchMock.mockImplementation((url: any) => {
      if (String(url).includes('/api/nodes/names')) {
        return Promise.resolve({ ok: true, json: async () => ({ names: [] }) });
      }
      catalogCall += 1;
      if (catalogCall === 1) return stalePromise;                       // mount catalog page — deferred
      return Promise.resolve({ ok: true, json: async () => pageNew });  // post-search catalog page
    });
    render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
    await act(async () => { await vi.advanceTimersByTimeAsync(250); });         // fire mount debounce → call 1 (pending)

    fireEvent.change(screen.getByPlaceholderText(/table name/i), { target: { value: 'x' } });
    await act(async () => { await vi.advanceTimersByTimeAsync(250); });         // fire call 2 → resolves pageNew
    expect(screen.getByText('NEW_NODE')).toBeInTheDocument();

    resolveStale({ ok: true, json: async () => pageStale });                    // late stale response arrives
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });

    expect(screen.queryByText('STALE_NODE')).not.toBeInTheDocument();           // stale must be ignored
    expect(screen.getByText('NEW_NODE')).toBeInTheDocument();
    vi.useRealTimers();
  });
});
