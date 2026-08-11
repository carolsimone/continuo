// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router';
import ReleaseDetailPage from './ReleaseDetailPage';
import { NodeValidationResult, ReleaseDetail, ProposalDTO } from './types';
import { reasonLabel } from './release-helpers';

vi.mock('./remediation-api', () => ({ fetchProposals: vi.fn() }));
import { fetchProposals } from './remediation-api';
const mockFetchProposals = fetchProposals as ReturnType<typeof vi.fn>;

const node = (o: Partial<NodeValidationResult> & { stage: string; node_id: string }): NodeValidationResult => ({
  status: 'failed', ...o,
});

const makeRelease = (perNode: NodeValidationResult[], reject_reason = 'compile_failed'): ReleaseDetail => ({
  release_id: 'rel-1', status: 'rejected', transitions: [], validation_node_ids: null,
  reject_reason, failing_nodes: null, per_node_results: perNode, image_tags: {},
  manifests_uri: '', bootstrap: false,
});

const proposal = (o: Partial<ProposalDTO> & { source: string; node_id: string }): ProposalDTO => ({
  id: 'p', release_id: 'rel-1', error_signature: 's', attempt: 1, status: 'proposed',
  confidence: 'high', rationale: '', proposed_sql_uri: '', diff_uri: '', candidate_fix_sql_uri: '',
  candidate_fix_diff_uri: '', source_resolved: true, repo: '', commit_sha: '', file_path: '',
  model: '', created_at: '', pr_url: '', pr_number: 0, pr_state: '', pr_opened_at: '', pr_opened_by: '',
  pr_closed_at: '',
  ...o,
});

function renderPage(rel: ReleaseDetail) {
  global.fetch = vi.fn((url: string) => {
    if (String(url).startsWith('/api/releases/rel-1')) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve(rel) });
    }
    return Promise.resolve({ ok: true, text: () => Promise.resolve(''), json: () => Promise.resolve({}) });
  }) as unknown as typeof fetch;
  return render(
    <MemoryRouter initialEntries={['/releases/rel-1']}>
      <Routes><Route path="/releases/:id" element={<ReleaseDetailPage />} /></Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => { vi.clearAllMocks(); mockFetchProposals.mockResolvedValue([]); });

describe('ReleaseDetailPage — stage sections', () => {
  it('renders a Compilation section with the offending file_path, no Seed/Validation sections', async () => {
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'service-1', file_path: 'models/x.sql', dbt_log_uri: 's3://c.log' })]));
    expect(await screen.findByText('models/x.sql')).toBeInTheDocument();
    const sectionTitles = Array.from(document.querySelectorAll('.section-header__title')).map(el => el.textContent);
    expect(sectionTitles).toContain('Compilation');
    expect(screen.queryByText('Seed')).toBeNull();
    expect(screen.queryByText('Validation')).toBeNull();
  });

  it('attaches a proposal link only to its own stage row (no cross-stage leak)', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'svc' })]);
    renderPage(makeRelease([
      node({ stage: 'compile', node_id: 'svc', file_path: 'models/x.sql' }),
      node({ stage: 'validation', node_id: 'svc' }),
    ]));
    await screen.findByText('models/x.sql');
    await waitFor(() => expect(screen.getAllByText(/Proposed fix available/).length).toBe(1));
    // The one link lives in the Compilation section's table, not Validation's.
    const links = screen.getAllByText(/Proposed fix available/);
    expect(links).toHaveLength(1);
  });

  it('renders only a Validation section for a validation-only release', async () => {
    renderPage(makeRelease([node({ stage: 'validation', node_id: 'analytics.x' })], 'validation_failed'));
    expect(await screen.findByText('analytics.x')).toBeInTheDocument();
    const sectionTitles = Array.from(document.querySelectorAll('.section-header__title')).map(el => el.textContent);
    expect(sectionTitles).toContain('Validation');
    expect(screen.queryByText('Compilation')).toBeNull();
    expect(screen.queryByText('Seed')).toBeNull();
  });

  it('shows the humanized reason in the rejection banner, not the raw token', async () => {
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'service-1', file_path: 'models/x.sql' })]));
    await screen.findByText('models/x.sql');
    const banner = document.querySelector('.info-strip--error');
    expect(banner).not.toBeNull();
    expect(banner!.textContent).toContain('Compilation');
    expect(screen.queryByText('compile_failed')).toBeNull();
  });
});

describe('ReleaseDetailPage — FIX cell is status-aware', () => {
  it('shows a disabled "Generating fix…" chip while a proposal is in flight', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'svc', status: 'generating' })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'svc', file_path: 'models/x.sql' })]));
    await screen.findByText('models/x.sql');
    const chip = await screen.findByText(/Generating fix/);
    expect(chip).toHaveAttribute('aria-disabled', 'true');
    // Not a link: the in-flight chip must not be actionable.
    expect(screen.queryByText(/Proposed fix available/)).toBeNull();
  });

  it('shows the "Proposed fix available →" link once the proposal is proposed', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'svc', status: 'proposed' })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'svc', file_path: 'models/x.sql' })]));
    await screen.findByText('models/x.sql');
    await waitFor(() => expect(screen.getByText(/Proposed fix available/)).toBeInTheDocument());
    expect(screen.queryByText(/Generating fix/)).toBeNull();
  });

  it('prefers the proposed link over a generating chip when both exist for a node', async () => {
    mockFetchProposals.mockResolvedValue([
      proposal({ source: 'compile', node_id: 'svc', attempt: 1, status: 'generating' }),
      proposal({ source: 'compile', node_id: 'svc', attempt: 2, status: 'proposed' }),
    ]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'svc', file_path: 'models/x.sql' })]));
    await screen.findByText('models/x.sql');
    await waitFor(() => expect(screen.getByText(/Proposed fix available/)).toBeInTheDocument());
    expect(screen.queryByText(/Generating fix/)).toBeNull();
  });

  it('renders nothing in the FIX cell for a skipped proposal', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'svc', status: 'skipped' })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'svc', file_path: 'models/x.sql' })]));
    await screen.findByText('models/x.sql');
    await waitFor(() => expect(mockFetchProposals).toHaveBeenCalled());
    expect(screen.queryByText(/Proposed fix available/)).toBeNull();
    expect(screen.queryByText(/Generating fix/)).toBeNull();
  });

  it('renders nothing in the FIX cell when no proposal exists', async () => {
    mockFetchProposals.mockResolvedValue([]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'svc', file_path: 'models/x.sql' })]));
    await screen.findByText('models/x.sql');
    await waitFor(() => expect(mockFetchProposals).toHaveBeenCalled());
    expect(screen.queryByText(/Proposed fix available/)).toBeNull();
    expect(screen.queryByText(/Generating fix/)).toBeNull();
  });
});

describe('ReleaseDetailPage — live polling while non-terminal', () => {
  it('polls while validating and stops once the release reaches a terminal status', async () => {
    vi.useFakeTimers();
    try {
      const base = {
        release_id: 'rel-1', transitions: [], validation_node_ids: null, reject_reason: '',
        failing_nodes: null, image_tags: {}, manifests_uri: '', bootstrap: false,
      };
      const responses: ReleaseDetail[] = [
        { ...base, status: 'validating', per_node_results: [node({ stage: 'validation', node_id: 'a', status: 'ok' })] },
        {
          ...base, status: 'validating',
          per_node_results: [
            node({ stage: 'validation', node_id: 'a', status: 'ok' }),
            node({ stage: 'validation', node_id: 'b', status: 'ok' }),
          ],
        },
        {
          ...base, status: 'promoted',
          per_node_results: [
            node({ stage: 'validation', node_id: 'a', status: 'ok' }),
            node({ stage: 'validation', node_id: 'b', status: 'ok' }),
          ],
        },
      ];
      let i = 0;
      global.fetch = vi.fn(() => Promise.resolve({
        ok: true,
        json: () => Promise.resolve(responses[Math.min(i++, responses.length - 1)]),
      })) as unknown as typeof fetch;

      render(
        <MemoryRouter initialEntries={['/releases/rel-1']}>
          <Routes><Route path="/releases/:id" element={<ReleaseDetailPage />} /></Routes>
        </MemoryRouter>,
      );

      // Flush the initial fetch (no timer advance needed — it fires on mount).
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(screen.getByText('a')).toBeInTheDocument();
      expect(screen.queryByText('b')).toBeNull();

      // Still validating: the next poll tick (~5s) picks up the newly-added node.
      await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
      expect(screen.getByText('b')).toBeInTheDocument();

      // This tick returns a terminal status; polling must stop.
      await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
      expect(screen.getByText('promoted')).toBeInTheDocument();
      const callsAtTerminal = (global.fetch as ReturnType<typeof vi.fn>).mock.calls.length;
      expect(callsAtTerminal).toBe(3);

      // No further fetches are scheduled once terminal.
      await act(async () => { await vi.advanceTimersByTimeAsync(20000); });
      expect((global.fetch as ReturnType<typeof vi.fn>).mock.calls.length).toBe(callsAtTerminal);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('ReleaseDetailPage — resilient polling on transient errors', () => {
  it('keeps the last-good view and keeps polling through a mid-poll fetch failure, then recovers', async () => {
    vi.useFakeTimers();
    try {
      const base = {
        release_id: 'rel-1', transitions: [], validation_node_ids: null, reject_reason: '',
        failing_nodes: null, image_tags: {}, manifests_uri: '', bootstrap: false,
      };
      const good1: ReleaseDetail = {
        ...base, status: 'validating',
        per_node_results: [node({ stage: 'validation', node_id: 'a', status: 'ok' })],
      };
      const good2: ReleaseDetail = {
        ...base, status: 'validating',
        per_node_results: [
          node({ stage: 'validation', node_id: 'a', status: 'ok' }),
          node({ stage: 'validation', node_id: 'b', status: 'ok' }),
        ],
      };

      let call = 0;
      global.fetch = vi.fn((url: string) => {
        if (String(url).startsWith('/api/releases/rel-1')) {
          call += 1;
          if (call === 2) return Promise.reject(new Error('network blip'));
          return Promise.resolve({ ok: true, json: () => Promise.resolve(call === 1 ? good1 : good2) });
        }
        return Promise.resolve({ ok: true, text: () => Promise.resolve(''), json: () => Promise.resolve({}) });
      }) as unknown as typeof fetch;

      render(
        <MemoryRouter initialEntries={['/releases/rel-1']}>
          <Routes><Route path="/releases/:id" element={<ReleaseDetailPage />} /></Routes>
        </MemoryRouter>,
      );

      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(screen.getByText('a')).toBeInTheDocument();

      // Second tick fails transiently: the page must keep showing the last-good
      // data instead of blanking to the hard error page, and a non-blocking
      // indicator surfaces the transient failure.
      await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
      expect(screen.getByText('a')).toBeInTheDocument();
      expect(document.querySelector('.info-strip--error')).toBeNull();
      expect(document.querySelector('.info-strip--warning')).toBeInTheDocument();

      // Third tick succeeds again: recovers, picks up node 'b', and clears the
      // transient indicator. Three fetches total proves the failed tick still
      // scheduled the retry.
      await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
      expect(screen.getByText('b')).toBeInTheDocument();
      expect(document.querySelector('.info-strip--warning')).toBeNull();
      expect((global.fetch as ReturnType<typeof vi.fn>).mock.calls.length).toBe(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it('shows the error page when the initial load fails (no last-good data to fall back on)', async () => {
    global.fetch = vi.fn(() => Promise.reject(new Error('boom'))) as unknown as typeof fetch;
    render(
      <MemoryRouter initialEntries={['/releases/rel-1']}>
        <Routes><Route path="/releases/:id" element={<ReleaseDetailPage />} /></Routes>
      </MemoryRouter>,
    );
    expect(await screen.findByText('boom')).toBeInTheDocument();
    expect(document.querySelector('.info-strip--error')).toBeInTheDocument();
  });
});

describe('ReleaseDetailPage — reject detail', () => {
  it('names both claimants when a release is rejected for a duplicated relation', async () => {
    renderPage({
      release_id: 'rel-1',
      status: 'rejected',
      reject_reason: 'duplicate_table',
      reject_detail:
        'analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql); ' +
        'a relation may be produced by exactly one node — rename one of them',
      transitions: [],
      validation_node_ids: null,
      failing_nodes: ['analytics.orders'],
      per_node_results: null,
      image_tags: {},
      manifests_uri: '',
      bootstrap: false,
    });

    expect(await screen.findByText(/finance \(models\/orders\.sql\)/)).toBeInTheDocument();
    expect(screen.getByText(/marketing \(models\/orders\.sql\)/)).toBeInTheDocument();
  });

  // Guards the empty-detail path against a dangling separator: an unconditional
  // " — " (or a stray empty element) that a future change might introduce would
  // still pass a loose "does not contain a dash" check, so this pins the
  // strip's text to exactly the icon plus the reason label and nothing else.
  it('shows the reason alone when a rejection carries no detail', async () => {
    renderPage({
      release_id: 'rel-2',
      status: 'rejected',
      reject_reason: 'validation_failed',
      reject_detail: '',
      transitions: [],
      validation_node_ids: null,
      failing_nodes: null,
      per_node_results: null,
      image_tags: {},
      manifests_uri: '',
      bootstrap: false,
    });

    await screen.findByText(/validation/i);
    const strip = document.querySelector('.info-strip--error');
    expect(strip).not.toBeNull();
    expect(strip!.textContent).toBe(`⚠${reasonLabel('validation_failed')}`);
  });
});

describe('ReleaseDetailPage — proposal polling gated on rejected', () => {
  it('does not poll proposals while validating even with a failed node, then starts fresh once the release is rejected', async () => {
    vi.useFakeTimers();
    try {
      const base = {
        release_id: 'rel-1', transitions: [], validation_node_ids: null, reject_reason: '',
        failing_nodes: null, image_tags: {}, manifests_uri: '', bootstrap: false,
      };
      const validatingWithFailure: ReleaseDetail = {
        ...base, status: 'validating',
        per_node_results: [node({ stage: 'validation', node_id: 'svc', status: 'failed' })],
      };
      const rejected: ReleaseDetail = {
        ...base, status: 'rejected', reject_reason: 'validation_failed',
        per_node_results: [node({ stage: 'validation', node_id: 'svc', status: 'failed' })],
      };

      let call = 0;
      global.fetch = vi.fn((url: string) => {
        if (String(url).startsWith('/api/releases/rel-1')) {
          call += 1;
          return Promise.resolve({ ok: true, json: () => Promise.resolve(call === 1 ? validatingWithFailure : rejected) });
        }
        return Promise.resolve({ ok: true, text: () => Promise.resolve(''), json: () => Promise.resolve({}) });
      }) as unknown as typeof fetch;

      mockFetchProposals.mockResolvedValue([proposal({ source: 'validation', node_id: 'svc', status: 'proposed' })]);

      render(
        <MemoryRouter initialEntries={['/releases/rel-1']}>
          <Routes><Route path="/releases/:id" element={<ReleaseDetailPage />} /></Routes>
        </MemoryRouter>,
      );

      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(screen.getByText('svc')).toBeInTheDocument();
      expect(mockFetchProposals).not.toHaveBeenCalled();

      // The next poll transitions the release to rejected. Proposal polling
      // must start now (not have been running all along) and surface the
      // already-ready proposal without a manual reload. Fake timers are active,
      // so flush the effect's immediate refresh() call directly instead of
      // waitFor (which polls on real timers and would hang).
      await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockFetchProposals).toHaveBeenCalled();
      expect(screen.getByText(/Proposed fix available/)).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });
});
