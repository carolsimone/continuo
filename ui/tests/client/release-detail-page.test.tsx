// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest';
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

// A fix being verified: the FIX cell splits its 'verifying' chip on the phase
// agent-remediation recorded for the run judging it — 'Queued for
// verification' while it waits its turn in the pipeline's global queue,
// 'Verifying fix…' once it starts.
function proposalVerifying(phase: 'queued' | 'running') {
  return {
    release_id: 'rel_abc', node_id: 'analytics.table_g', status: 'verifying',
    verifications: [{ service: '', kind: '', run_id: 'verify-1', phase, activated_at: '', error: '' }],
  };
}

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
    // Services, Timeline, and the one per-node stage section.
    expect(container.querySelectorAll('.section-header').length).toBe(3);
    expect(container.querySelector('.service-tile')).toBeTruthy();
    expect(container.querySelector('.release-timeline')).toBeTruthy();
    expect(container.textContent).not.toContain('Image tags:');
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

  it('keeps polling past the cap while a verification run is still judging a fix', async () => {
    // Backend verification runs a whole release — parse, candidate schema,
    // validation — behind a global release queue, so it routinely outlasts the
    // three-minute cap that exists for failures which will never be healed. A
    // node whose fix is 'verifying' is not one of those: a verdict IS coming,
    // and stopping early leaves the page stuck on "Verifying fix…" with the
    // proposal link only a manual reload away.
    vi.useFakeTimers();
    const proposals = { current: [{ ...PROPOSAL_G, status: 'verifying' }] };
    const fetchMock = mockFetch(DETAIL_FAILED, proposals);
    vi.stubGlobal('fetch', fetchMock);

    renderDetail();
    await flush();
    await act(async () => { await vi.advanceTimersByTimeAsync(190000); }); // past the 180s cap
    const atCap = proposalCallCount(fetchMock);

    await act(async () => { await vi.advanceTimersByTimeAsync(60000); });
    expect(proposalCallCount(fetchMock)).toBeGreaterThan(atCap); // still polling

    // The verdict lands: the link appears without a reload, and polling stops.
    proposals.current = [PROPOSAL_G];
    await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
    await flush();
    expect(screen.getByText(/Proposed fix available/)).toBeInTheDocument();
    const settled = proposalCallCount(fetchMock);

    await act(async () => { await vi.advanceTimersByTimeAsync(60000); });
    expect(proposalCallCount(fetchMock)).toBe(settled);
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

describe('ReleaseDetailPage — verify chip: queued vs running', () => {
  it('reads a phase recorded as queued as "Queued for verification"', async () => {
    vi.useFakeTimers();
    const proposals = { current: [proposalVerifying('queued')] };
    vi.stubGlobal('fetch', mockFetch(DETAIL_FAILED, proposals));

    renderDetail();
    await flush();

    expect(screen.getByText('Queued for verification')).toBeInTheDocument();
    expect(screen.queryByText('Verifying fix…')).not.toBeInTheDocument();
  });

  it('reads a phase recorded as running as "Verifying fix…"', async () => {
    vi.useFakeTimers();
    const proposals = { current: [proposalVerifying('running')] };
    vi.stubGlobal('fetch', mockFetch(DETAIL_FAILED, proposals));

    renderDetail();
    await flush();

    expect(screen.getByText('Verifying fix…')).toBeInTheDocument();
    expect(screen.queryByText('Queued for verification')).not.toBeInTheDocument();
  });

  it('flips from queued to running once the next proposal poll observes the updated phase', async () => {
    vi.useFakeTimers();
    const proposals = { current: [proposalVerifying('queued')] };
    vi.stubGlobal('fetch', mockFetch(DETAIL_FAILED, proposals));

    renderDetail();
    await flush();
    expect(screen.getByText('Queued for verification')).toBeInTheDocument();

    // The run leaves the queue and starts; the next proposal poll observes
    // the updated phase and the chip flips without a reload.
    proposals.current = [proposalVerifying('running')];
    await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
    await flush();

    expect(screen.getByText('Verifying fix…')).toBeInTheDocument();
    expect(screen.queryByText('Queued for verification')).not.toBeInTheDocument();
  });

  it('falls back to "Verifying fix…" when the attempt carries no recorded verification phase', async () => {
    vi.useFakeTimers();
    const proposals = { current: [{ release_id: 'rel_abc', node_id: 'analytics.table_g', status: 'verifying' }] };
    vi.stubGlobal('fetch', mockFetch(DETAIL_FAILED, proposals));

    renderDetail();
    await flush();

    // No phase recorded yet, so the chip stays the existing flat one rather
    // than claiming a queue wait it cannot substantiate.
    expect(screen.getByText('Verifying fix…')).toBeInTheDocument();
    expect(screen.queryByText('Queued for verification')).not.toBeInTheDocument();
  });
});

describe('ReleaseDetailPage — brand header', () => {
  it('starts the header with the brand, before the back link', async () => {
    vi.stubGlobal('fetch', vi.fn(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve(DETAIL) })));
    const { container } = renderDetail();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    const header = container.querySelector('.page-header')!;
    const brand = header.querySelector('a.brand');
    expect(brand).toHaveAttribute('href', '/');
    const back = header.querySelector('.detail-back-link')!;
    expect(brand!.compareDocumentPosition(back) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(header.querySelector('.page-header__divider')).toBeTruthy();
  });
});

describe('ReleaseDetailPage — provenance line', () => {
  it('says which service the release changes and links its commit on GitHub', async () => {
    const detail = {
      ...DETAIL, changed_service: 'service-1', repo: 'acme/demo', commit_sha: 'abcdef1234567',
      commit_url: 'https://ghe.example.com/acme/demo/commit/abcdef1234567',
    };
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(detail) })));
    const { container } = renderDetail();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    const sub = container.querySelector('.page-sub')!;
    expect(sub.textContent).toContain('Changes');
    expect(sub.querySelector('strong')?.textContent).toBe('service-1');
    const link = sub.querySelector('a')!;
    expect(link).toHaveAttribute('href', 'https://ghe.example.com/acme/demo/commit/abcdef1234567');
    expect(link.textContent).toContain('acme/demo @ abcdef1');
  });

  it('omits the commit link when the server attached no commit URL', async () => {
    const detail = { ...DETAIL, changed_service: 'service-1', repo: 'acme/demo', commit_sha: 'abcdef1234567' };
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(detail) })));
    const { container } = renderDetail();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());
    const sub = container.querySelector('.page-sub')!;
    expect(sub.querySelector('strong')?.textContent).toBe('service-1');
    expect(sub.querySelector('a')).toBeNull();
  });

  it('renders no provenance line when the release names no changed service', async () => {
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(DETAIL) })));
    const { container } = renderDetail();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());
    expect(container.querySelector('.page-sub')).toBeNull();
  });
});

describe('ReleaseDetailPage — timeline preview follows the release kind', () => {
  it('shows no compile leg ahead of a python release', async () => {
    const detail = { ...DETAIL, status: 'received', transitions: [{ to: 'received', at: '2026-06-01T10:00:00Z' }], manifest_kind: 'python' };
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(detail) })));
    const { container } = renderDetail();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());
    const ghosts = Array.from(container.querySelectorAll('.release-timeline__step--upcoming .pill-sm'))
      .map(p => p.firstChild?.textContent);
    expect(ghosts).toEqual(['parsing', 'validating']);
  });

  it('shows no validation ahead of a bootstrap release', async () => {
    const detail = { ...DETAIL, status: 'received', transitions: [{ to: 'received', at: '2026-06-01T10:00:00Z' }], manifest_kind: 'dbt', bootstrap: true };
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve(detail) })));
    const { container } = renderDetail();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());
    const ghosts = Array.from(container.querySelectorAll('.release-timeline__step--upcoming .pill-sm'))
      .map(p => p.firstChild?.textContent);
    expect(ghosts).toEqual(['compiling', 'parsing']);
  });
});

// The notice of the design's example: a seed that gains a column, two views
// the run rebuilds, one table that refreshes on its own schedule and one view
// outside continuo that stops the load.
const designNotice = {
  kind: 'seed_reload', node_id: 'analytics.seed_fx_rates_eur', service: 'finance',
  engine_family: 'schema_bound', change: 'columns',
  columns: { added: ['source'], removed: [], retyped: [] },
  load: { operation: 'run', drops_table: true, reason: 'columns changed; +full_refresh: true re-creates the table on every load' },
  rebuilds: [
    { node_id: 'analytics.fx_rates_current', materialized: 'view', operation: 'run', schedule: 'hourly' },
    { node_id: 'analytics.fx_rates_latest_eur', materialized: 'view', operation: 'run', schedule: 'hourly' },
  ],
  refresh_on_schedule: [{ node_id: 'analytics.fx_transactions_eur', materialized: 'table', schedule: 'daily' }],
  warehouse: {
    checked: true, completeness: 'exact',
    outside_continuo: [{ object: 'bi.fx_rates_report', kind: 'view', owner: 'bi' }],
  },
  prediction: 'stops', stop_reason: 'unowned_dependents', unvalidated: false,
};

const promotedRelease = {
  ...DETAIL_OK,
  release_id: 'rel-a1b2c3d-201',
  transitions: [
    { to: 'received', at: '2026-06-01T10:00:00Z' },
    { to: 'seed_building', at: '2026-06-01T10:01:00Z' },
    { to: 'promoted', at: '2026-06-01T10:05:00Z' },
  ],
  per_node_results: [],
};

let runsResponse: any[] = [];
function mockRuns(runs: any[]) { runsResponse = runs; }

function mockRelease(detail: any) {
  vi.stubGlobal('fetch', vi.fn((url: any) => {
    const u = String(url);
    if (u.startsWith('/api/runs')) {
      return Promise.resolve({ ok: true, json: async () => ({ total_count: runsResponse.length, runs: runsResponse }) });
    }
    return Promise.resolve({ ok: true, json: async () => detail });
  }));
}

function renderPage(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/releases/:id" element={<ReleaseDetailPage />} />
      </Routes>
    </MemoryRouter>
  );
}

function strips(container: HTMLElement): string[] {
  return Array.from(container.querySelectorAll('.info-strip--warning')).map(s => s.textContent ?? '');
}

describe('ReleaseDetailPage — seed reload notices', () => {
  beforeEach(() => { runsResponse = []; });

  it('shows a warning strip for each seed reload notice', async () => {
    mockRelease({ ...promotedRelease, notices: [designNotice] });
    renderPage('/releases/rel-a1b2c3d-201');
    const strip = await screen.findByText(/Seed reload: analytics.seed_fx_rates_eur adds the column source/);
    expect(strip.closest('.info-strip--warning')).not.toBeNull();
    expect(strip.textContent).toContain('rebuilds the 2 views that read it');
    expect(strip.textContent).toContain('bi.fx_rates_report');
    expect(strip.textContent).toContain('The reload will stop before the drop');
  });

  it('words a notice in the order of the design', async () => {
    mockRelease({ ...promotedRelease, notices: [designNotice] });
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await screen.findByText(/Seed reload: /);
    expect(strips(container)).toEqual([
      '⚠Seed reload: analytics.seed_fx_rates_eur adds the column source '
      + '(columns changed; +full_refresh: true re-creates the table on every load) '
      + 'and, in the same run, rebuilds the 2 views that read it: analytics.fx_rates_current, analytics.fx_rates_latest_eur. '
      + 'analytics.fx_transactions_eur (table) refreshes on its own schedule, daily. '
      + '1 object outside continuo reads the seed: bi.fx_rates_report (view, owner bi). '
      + 'The reload will stop before the drop until an operator names them in a full refresh of the seed, '
      + 'or removes them and runs the seed again.',
    ]);
  });

  it('draws one strip per notice and words a row change, a removal and a type change', async () => {
    mockRelease({
      ...promotedRelease,
      notices: [
        { ...designNotice, node_id: 'a.rows', change: 'rows', rebuilds: [], refresh_on_schedule: [], prediction: 'reloads',
          warehouse: { checked: true, completeness: 'exact', outside_continuo: [] } },
        { ...designNotice, node_id: 'a.cols', columns: { added: [], removed: ['x', 'y'], retyped: ['z'] },
          rebuilds: [designNotice.rebuilds[0]], refresh_on_schedule: [], prediction: 'reloads',
          warehouse: { checked: true, completeness: 'exact', outside_continuo: [] } },
      ],
    });
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await screen.findByText(/Seed reload: a.rows reloads new rows/);
    const texts = strips(container);
    expect(texts).toHaveLength(2);
    expect(texts[0]).toContain('Seed reload: a.rows reloads new rows (columns changed;');
    expect(texts[0]).not.toContain('rebuilds');
    expect(texts[1]).toContain('Seed reload: a.cols removes the columns x, y and changes the type of the column z');
    expect(texts[1]).toContain('rebuilds the 1 view that reads it: analytics.fx_rates_current.');
  });

  it('names the stop reason of a refused reload', async () => {
    mockRelease({
      ...promotedRelease,
      notices: [{ ...designNotice, prediction: 'refused', stop_reason: 'seed_full_refresh_disabled',
        warehouse: { checked: true, completeness: 'exact', outside_continuo: [] } }],
    });
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await screen.findByText(/Seed reload: /);
    expect(strips(container)[0]).toContain('The reload is refused: seed_full_refresh_disabled.');
    expect(strips(container)[0]).not.toContain('will stop before the drop');
  });

  it('warns that objects outside continuo may go stale on a late binding warehouse', async () => {
    mockRelease({ ...promotedRelease, notices: [{ ...designNotice, engine_family: 'late_binding', prediction: 'reloads' }] });
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await screen.findByText(/Seed reload: /);
    expect(strips(container)[0]).toContain('They may go stale; recreate them after the reload.');
  });

  it('says a release skipped validation', async () => {
    mockRelease({ ...promotedRelease, notices: [{ ...designNotice, unvalidated: true }] });
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await screen.findByText(/Seed reload: /);
    expect(strips(container)[0]).toContain('unvalidated: the release skipped validation');
  });

  it('says the warehouse was not checked only once the seed-build stage has run', async () => {
    const unchecked = { ...designNotice, warehouse: { checked: false, completeness: '', outside_continuo: [] }, prediction: 'reloads' };
    mockRelease({ ...promotedRelease, notices: [unchecked] });
    const done = renderPage('/releases/rel-a1b2c3d-201');
    await screen.findByText(/Seed reload: /);
    expect(strips(done.container)[0]).toContain('The warehouse was not checked.');
    done.unmount();

    const building = { ...promotedRelease, status: 'seed_building', transitions: promotedRelease.transitions.slice(0, 2) };
    mockRelease({ ...building, notices: [unchecked] });
    const waiting = renderPage('/releases/rel-a1b2c3d-201');
    await screen.findByText(/Seed reload: /);
    expect(strips(waiting.container)[0]).not.toContain('not checked');
    waiting.unmount();

    const parsing = { ...promotedRelease, status: 'parsing', transitions: promotedRelease.transitions.slice(0, 1) };
    mockRelease({ ...parsing, notices: [unchecked] });
    const early = renderPage('/releases/rel-a1b2c3d-201');
    await screen.findByText(/Seed reload: /);
    expect(strips(early.container)[0]).not.toContain('not checked');
  });

  it('does not say the warehouse was not checked when it was', async () => {
    mockRelease({ ...promotedRelease, notices: [designNotice] });
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await screen.findByText(/Seed reload: /);
    expect(strips(container)[0]).not.toContain('not checked');
  });

  it('draws no strip for a release without notices', async () => {
    mockRelease(promotedRelease);
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await waitFor(() => expect(screen.getByText('rel-a1b2c3d-201')).toBeInTheDocument());
    expect(strips(container)).toEqual([]);
  });

  it('draws no strip for a release whose notices list is empty', async () => {
    mockRelease({ ...promotedRelease, notices: [] });
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await waitFor(() => expect(screen.getByText('rel-a1b2c3d-201')).toBeInTheDocument());
    expect(strips(container)).toEqual([]);
  });

  it('draws a notice that lacks its lists and skips one that names no node', async () => {
    mockRelease({
      ...promotedRelease,
      notices: [
        { kind: 'seed_reload', node_id: 'a.bare', change: 'columns', prediction: 'reloads' },
        { kind: 'seed_reload', change: 'rows', prediction: 'reloads' },
      ],
    });
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await screen.findByText(/Seed reload: a.bare changes its columns\./);
    expect(strips(container)).toHaveLength(1);
  });
});

describe('ReleaseDetailPage — the runs a promotion started', () => {
  beforeEach(() => { runsResponse = []; });

  it('lists the runs the promotion started', async () => {
    mockRuns([{ run_id: 'r1', kind: 'promote_seed', status: 'succeeded', schedule_name: 'promote-seed-5b2e0a1c' }]);
    mockRelease(promotedRelease);
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    const link = await screen.findByRole('link', { name: /Seed reload/ });
    expect(link).toHaveAttribute('href', '/run/r1');
    const row = link.closest('tr')!;
    expect(row.querySelector('.pill-sm--succeeded')?.textContent).toBe('succeeded');
    expect(row.textContent).toContain('promote-seed-5b2e0a1c');
    expect(vi.mocked(fetch)).toHaveBeenCalledWith(
      '/api/runs?release_id=rel-a1b2c3d-201&kind=promote_seed', expect.anything());
    expect(container.textContent).toContain('Seed reload runs');
  });

  it('draws nothing when the promotion started no run', async () => {
    mockRelease(promotedRelease);
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await waitFor(() => expect(screen.getByText('rel-a1b2c3d-201')).toBeInTheDocument());
    await waitFor(() => expect(vi.mocked(fetch)).toHaveBeenCalledWith(expect.stringContaining('/api/runs?'), expect.anything()));
    expect(container.textContent).not.toContain('Seed reload runs');
  });

  it('does not look for runs of a release that is not promoted', async () => {
    mockRuns([{ run_id: 'r1', kind: 'promote_seed', status: 'running', schedule_name: 'promote-seed-5b2e0a1c' }]);
    mockRelease({ ...promotedRelease, status: 'validating' });
    renderPage('/releases/rel-a1b2c3d-201');
    await waitFor(() => expect(screen.getByText('rel-a1b2c3d-201')).toBeInTheDocument());
    expect(vi.mocked(fetch)).not.toHaveBeenCalledWith(expect.stringContaining('/api/runs?'), expect.anything());
  });
});

describe('ReleaseRunsPanel — polling', () => {
  const run = (status: string) => ({ run_id: 'r1', kind: 'promote_seed', status, schedule_name: 'promote-seed-5b2e0a1c' });

  // answers[i] is the runs list the i-th read returns; the last repeats.
  function mockReads(answers: any[][]) {
    let i = 0;
    vi.stubGlobal('fetch', vi.fn((url: any) => {
      const u = String(url);
      if (u.startsWith('/api/runs')) {
        const runs = answers[Math.min(i++, answers.length - 1)];
        return Promise.resolve({ ok: true, json: async () => ({ total_count: runs.length, runs }) });
      }
      return Promise.resolve({ ok: true, json: async () => promotedRelease });
    }));
  }
  const reads = () => vi.mocked(fetch).mock.calls.filter(c => String(c[0]).startsWith('/api/runs')).length;
  const tickFor = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

  beforeEach(() => { vi.useFakeTimers(); });

  it('reads again while the list is empty and shows a run once it exists', async () => {
    mockReads([[], [run('pending')]]);
    renderPage('/releases/rel-a1b2c3d-201');
    await tickFor(0);
    expect(reads()).toBe(1);
    expect(screen.queryByRole('link', { name: /Seed reload/ })).toBeNull();
    await tickFor(5000);
    expect(screen.getByRole('link', { name: /Seed reload/ })).toHaveAttribute('href', '/run/r1');
  });

  it('keeps reading while a run is active and stops once every run is terminal', async () => {
    mockReads([[run('running')], [run('running')], [run('succeeded')]]);
    const { container } = renderPage('/releases/rel-a1b2c3d-201');
    await tickFor(0);
    expect(container.querySelector('tbody .pill-sm--running')).not.toBeNull();
    await tickFor(5000);
    expect(reads()).toBe(2);
    await tickFor(5000);
    expect(reads()).toBe(3);
    expect(container.querySelector('tbody .pill-sm--succeeded')).not.toBeNull();
    expect(container.querySelector('tbody .pill-sm--running')).toBeNull();
    await tickFor(60000);
    expect(reads()).toBe(3);
  });

  it('stops reading after a bounded window while the list stays empty', async () => {
    mockReads([[]]);
    renderPage('/releases/rel-a1b2c3d-201');
    await tickFor(0);
    await tickFor(60000);
    expect(reads()).toBe(6);
  });

  it('stops reading when the page goes away', async () => {
    mockReads([[run('running')]]);
    const { unmount } = renderPage('/releases/rel-a1b2c3d-201');
    await tickFor(0);
    unmount();
    const before = reads();
    await tickFor(30000);
    expect(reads()).toBe(before);
  });
});
