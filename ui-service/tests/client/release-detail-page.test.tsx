// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router';
import ReleaseDetailPage from '../../src/client/ReleaseDetailPage';

const DETAIL = {
  release_id: 'rel_abc',
  status: 'rejected',
  transitions: [{ to: 'received', at: '2026-06-01T10:00:00Z' }],
  validation_node_ids: null,
  reject_reason: 'schema drift',
  failing_nodes: null,
  per_node_results: [
    { node_id: 'm.dim_x', status: 'failed', duration_ms: 1200, dbt_log_uri: 's3://logs/x' },
  ],
  image_tags: { 'service-1': 'sha123' },
};

// Release with a failed node eligible for a remediation proposal.
const DETAIL_FAILED = {
  release_id: 'rel_abc',
  status: 'rejected',
  transitions: [{ to: 'rejected', at: '2026-06-25T14:30:29Z' }],
  validation_node_ids: null,
  reject_reason: 'node failed',
  failing_nodes: null,
  per_node_results: [
    { node_id: 'analytics.table_a', status: 'ok', duration_ms: 10, dbt_log_uri: 's3://logs/a' },
    { node_id: 'analytics.table_g', status: 'failed', duration_ms: 0, dbt_log_uri: null },
  ],
  image_tags: {},
};

// Healthy release: no failed nodes, so no node is eligible for a proposal.
const DETAIL_OK = {
  ...DETAIL_FAILED,
  status: 'promoted',
  reject_reason: null,
  per_node_results: [
    { node_id: 'analytics.table_a', status: 'ok', duration_ms: 10, dbt_log_uri: 's3://logs/a' },
    { node_id: 'analytics.table_i', status: 'skipped', duration_ms: 0, dbt_log_uri: null },
  ],
};

// A ready proposal for the failed node: the FIX cell is status-aware, so only a
// 'proposed' row surfaces the "Proposed fix available →" link.
const PROPOSAL_G = { release_id: 'rel_abc', node_id: 'analytics.table_g', status: 'proposed' };

function renderDetail() {
  return render(
    <MemoryRouter initialEntries={['/releases/rel_abc']}>
      <Routes>
        <Route path="/releases/:id" element={<ReleaseDetailPage />} />
      </Routes>
    </MemoryRouter>
  );
}

// fetch mock: release detail for /api/releases/:id, and the current proposal set
// (read live from proposalsRef) for /api/remediation/proposals.
function mockFetch(detail: any, proposalsRef: { current: any[] }) {
  return vi.fn((url: any) => {
    const u = String(url);
    if (u.includes('/api/remediation/proposals')) {
      return Promise.resolve({ ok: true, json: async () => ({ proposals: proposalsRef.current }) });
    }
    return Promise.resolve({ ok: true, json: async () => detail });
  });
}

function proposalCallCount(fetchMock: ReturnType<typeof vi.fn>): number {
  return fetchMock.mock.calls.filter(c => String(c[0]).includes('/api/remediation/proposals')).length;
}

// Flush pending microtasks + zero-delay timers so React applies state updates.
async function flush() {
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
}

afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

describe('ReleaseDetailPage', () => {
  it('renders a compliant header, section-headers, nodes-table, and reject-reason strip', async () => {
    vi.stubGlobal('fetch', vi.fn(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve(DETAIL) })));
    const { container } = renderDetail();

    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    expect(container.querySelector('.detail-back-link')).toBeTruthy();
    expect(container.querySelector('.detail-page-title')).toBeTruthy();
    expect(container.querySelector('.page-header .pill')).toBeTruthy();
    expect(container.querySelectorAll('.section-header').length).toBe(2);
    expect(container.querySelector('table.nodes-table')).toBeTruthy();
    expect(container.querySelector('.info-strip--error')?.textContent).toContain('schema drift');

    expect(container.querySelector('.release-table')).toBeNull();
    expect(container.querySelector('.log-view')).toBeNull();
    expect(container.querySelector('.btn--small')).toBeNull();
  });

  it('toggles the log block and always shows a link-out', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/api/releases/log')) {
        return Promise.resolve({ ok: true, text: () => Promise.resolve('LOG CONTENT') });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve(DETAIL) });
    });
    vi.stubGlobal('fetch', fetchMock);
    const { container } = renderDetail();

    await waitFor(() => expect(screen.getByText('m.dim_x')).toBeInTheDocument());

    expect(container.querySelector('a[href*="/api/releases/log"]')).toBeTruthy();
    expect(container.querySelector('.log-block')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'view' }));
    await waitFor(() =>
      expect(container.querySelector('.log-block')?.textContent).toContain('LOG CONTENT'));
  });

  it('renders a log fetch error as an info-strip, not as log text', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/api/releases/log')) {
        return Promise.resolve({ ok: false, status: 500, text: () => Promise.resolve('') });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve(DETAIL) });
    });
    vi.stubGlobal('fetch', fetchMock);
    const { container } = renderDetail();
    await waitFor(() => expect(screen.getByText('m.dim_x')).toBeInTheDocument());

    fireEvent.click(screen.getByRole('button', { name: 'view' }));
    await waitFor(() => {
      const strips = Array.from(container.querySelectorAll('.info-strip--error'));
      expect(strips.some(s => s.textContent?.includes('HTTP 500'))).toBe(true);
    });
    expect(container.querySelector('.log-block')).toBeNull();
  });

  it('renders a full-page error strip when the release fetch fails', async () => {
    vi.stubGlobal('fetch', vi.fn(() =>
      Promise.resolve({ ok: false, status: 404, json: () => Promise.resolve({}) })));
    const { container } = renderDetail();
    await waitFor(() =>
      expect(container.querySelector('.info-strip--error')?.textContent).toContain('HTTP 404'));
  });
});

describe('ReleaseDetailPage — proposal link polling', () => {
  it('surfaces the fix link on a later poll without a manual refresh', async () => {
    vi.useFakeTimers();
    const proposals = { current: [] as any[] };
    const fetchMock = mockFetch(DETAIL_FAILED, proposals);
    vi.stubGlobal('fetch', fetchMock);

    renderDetail();
    await flush();
    expect(screen.queryByText(/Proposed fix available/)).not.toBeInTheDocument();

    // Proposal lands after the page is already open.
    proposals.current = [PROPOSAL_G];
    await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
    await flush();

    expect(screen.getByText(/Proposed fix available/)).toBeInTheDocument();
  });

  it('stops polling once every failed node has a proposal', async () => {
    vi.useFakeTimers();
    const proposals = { current: [PROPOSAL_G] };
    const fetchMock = mockFetch(DETAIL_FAILED, proposals);
    vi.stubGlobal('fetch', fetchMock);

    renderDetail();
    await flush();
    expect(screen.getByText(/Proposed fix available/)).toBeInTheDocument();
    const settled = proposalCallCount(fetchMock);

    await act(async () => { await vi.advanceTimersByTimeAsync(30000); }); // 6 poll intervals
    expect(proposalCallCount(fetchMock)).toBe(settled); // no further polling
  });

  it('stops polling at the cap when the failed node never gets a proposal', async () => {
    vi.useFakeTimers();
    const proposals = { current: [] as any[] };
    const fetchMock = mockFetch(DETAIL_FAILED, proposals);
    vi.stubGlobal('fetch', fetchMock);

    renderDetail();
    await flush();
    await act(async () => { await vi.advanceTimersByTimeAsync(190000); }); // past the 180s cap
    const capped = proposalCallCount(fetchMock);
    expect(capped).toBeGreaterThanOrEqual(36); // interval delivered the full cap, not a short-circuit

    await act(async () => { await vi.advanceTimersByTimeAsync(60000); });
    expect(proposalCallCount(fetchMock)).toBe(capped); // capped, no more polling
  });

  it('does not poll when the release has no failed nodes', async () => {
    vi.useFakeTimers();
    const proposals = { current: [] as any[] };
    const fetchMock = mockFetch(DETAIL_OK, proposals);
    vi.stubGlobal('fetch', fetchMock);

    renderDetail();
    await flush();
    const afterMount = proposalCallCount(fetchMock);
    expect(afterMount).toBeLessThanOrEqual(1); // single best-effort fetch, no interval

    await act(async () => { await vi.advanceTimersByTimeAsync(30000); });
    expect(proposalCallCount(fetchMock)).toBe(afterMount);
  });

  it('stops polling after the page unmounts', async () => {
    vi.useFakeTimers();
    const proposals = { current: [] as any[] };
    const fetchMock = mockFetch(DETAIL_FAILED, proposals);
    vi.stubGlobal('fetch', fetchMock);

    const { unmount } = renderDetail();
    await flush();
    const beforeUnmount = proposalCallCount(fetchMock);

    unmount();
    await act(async () => { await vi.advanceTimersByTimeAsync(30000); });
    expect(proposalCallCount(fetchMock)).toBe(beforeUnmount); // no polling after unmount
  });

  it('discards a stale proposal response that resolves after a newer poll stopped polling', async () => {
    vi.useFakeTimers();
    // Manually-resolvable responses for the proposals endpoint, so overlapping
    // polls can be made to resolve out of order.
    const deferreds: Array<(proposals: any[]) => void> = [];
    const fetchMock = vi.fn((url: any) => {
      if (String(url).includes('/api/remediation/proposals')) {
        let resolveResponse!: (v: any) => void;
        const p = new Promise<any>(res => { resolveResponse = res; });
        deferreds.push((proposals: any[]) =>
          resolveResponse({ ok: true, json: async () => ({ proposals }) }));
        return p;
      }
      return Promise.resolve({ ok: true, json: async () => DETAIL_FAILED });
    });
    vi.stubGlobal('fetch', fetchMock);

    renderDetail();
    await flush();

    // Resolve the initial fetch with no proposal yet — link hidden, polling continues.
    deferreds[deferreds.length - 1]([]);
    await flush();
    expect(screen.queryByText(/Proposed fix available/)).not.toBeInTheDocument();

    // Two polls fire while both responses are still in flight (overlapping).
    await act(async () => { await vi.advanceTimersByTimeAsync(5000); }); // poll A (older)
    const pollA = deferreds[deferreds.length - 1];
    await act(async () => { await vi.advanceTimersByTimeAsync(5000); }); // poll B (newer)
    const pollB = deferreds[deferreds.length - 1];

    // Newer poll B resolves first: finds the proposal, shows the link, stops polling.
    pollB([PROPOSAL_G]);
    await flush();
    expect(screen.getByText(/Proposed fix available/)).toBeInTheDocument();

    // Older poll A resolves last with a stale empty set — it must be discarded,
    // not overwrite proposedNodeIds and hide the link.
    pollA([]);
    await flush();
    expect(screen.getByText(/Proposed fix available/)).toBeInTheDocument();
  });
});
