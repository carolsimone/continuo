// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, act, fireEvent } from '@testing-library/react';
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

const makeRelease = (
  perNode: NodeValidationResult[],
  reject_reason = 'compile_failed',
  remediation_round = 1,
): ReleaseDetail => ({
  release_id: 'rel-1', status: 'rejected', transitions: [], validation_node_ids: null,
  reject_reason, failing_nodes: null, per_node_results: perNode, image_tags: {},
  bootstrap: false, remediation_round,
});

const proposal = (o: Partial<ProposalDTO> & { source: string; node_id: string }): ProposalDTO => ({
  id: 'p', release_id: 'rel-1', error_signature: 's', attempt: 1, status: 'proposed',
  confidence: 'high', rationale: '', proposed_sql_uri: '', diff_uri: '', candidate_fix_sql_uri: '',
  candidate_fix_diff_uri: '', source_resolved: true, repo: '', commit_sha: '', file_path: '',
  model: '', created_at: '', pr_url: '', pr_number: 0, pr_state: '', pr_opened_at: '', pr_opened_by: '',
  pr_closed_at: '', verification_run_id: '', verify_error: '',
  ...o,
});

// mockPost answers every POST fetch (currently only the retry-remediation
// route); tests configure its resolved {status, json} shape per case.
const mockPost = vi.fn();

// verificationRuns backs the /api/releases/:id/verifications route every
// renderPage-driven test's fetch mock answers; a test that cares about the
// "Verification runs" section overrides it with mockVerificationRuns, and
// every other test keeps seeing the default empty list.
let verificationRunsResponse: any[] = [];
function mockVerificationRuns(runs: any[]) {
  verificationRunsResponse = runs;
}

function renderPage(rel: ReleaseDetail) {
  global.fetch = vi.fn((url: string, init?: RequestInit) => {
    if (init?.method === 'POST') return mockPost(url, init);
    // Checked before the plain release-detail route below: that route's
    // startsWith('/api/releases/rel-1') would otherwise also swallow this
    // more specific path.
    if (String(url).startsWith('/api/releases/rel-1/verifications')) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ runs: verificationRunsResponse }) });
    }
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

beforeEach(() => {
  vi.clearAllMocks();
  mockFetchProposals.mockResolvedValue([]);
  mockPost.mockResolvedValue({ ok: true, status: 200, json: async () => ({}) });
  verificationRunsResponse = [];
});

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

  it('shows a disabled "Verifying fix…" chip while a verification run is judging the fix', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'validation', node_id: 'svc', status: 'verifying' })]);
    renderPage(makeRelease([node({ stage: 'validation', node_id: 'svc', file_path: 'contracts/svc.yaml' })]));
    await screen.findByText('contracts/svc.yaml');
    const chip = await screen.findByText(/Verifying fix/);
    expect(chip).toHaveAttribute('aria-disabled', 'true');
    // Not a link, and not the earlier phase's chip: verification follows generation.
    expect(screen.queryByText(/Proposed fix available/)).toBeNull();
    expect(screen.queryByText(/Generating fix/)).toBeNull();
  });

  it('prefers the proposed link over a verifying chip when both exist for a node', async () => {
    mockFetchProposals.mockResolvedValue([
      proposal({ source: 'validation', node_id: 'svc', attempt: 1, status: 'verifying' }),
      proposal({ source: 'validation', node_id: 'svc', attempt: 2, status: 'proposed' }),
    ]);
    renderPage(makeRelease([node({ stage: 'validation', node_id: 'svc', file_path: 'contracts/svc.yaml' })]));
    await screen.findByText('contracts/svc.yaml');
    await waitFor(() => expect(screen.getByText(/Proposed fix available/)).toBeInTheDocument());
    expect(screen.queryByText(/Verifying fix/)).toBeNull();
  });

  it('prefers the verifying chip over a generating chip when both exist for a node', async () => {
    mockFetchProposals.mockResolvedValue([
      proposal({ source: 'validation', node_id: 'svc', attempt: 1, status: 'generating' }),
      proposal({ source: 'validation', node_id: 'svc', attempt: 2, status: 'verifying' }),
    ]);
    renderPage(makeRelease([node({ stage: 'validation', node_id: 'svc', file_path: 'contracts/svc.yaml' })]));
    await screen.findByText('contracts/svc.yaml');
    await waitFor(() => expect(screen.getByText(/Verifying fix/)).toBeInTheDocument());
    expect(screen.queryByText(/Generating fix/)).toBeNull();
  });

  it('shows why a skipped attempt stopped in the FIX cell instead of leaving it empty', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'svc', status: 'skipped' })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'svc', file_path: 'models/x.sql' })]));
    await screen.findByText('models/x.sql');
    expect(await screen.findByText(/No source to fix at this commit\. Fix it in the repository\./)).toBeInTheDocument();
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

  it('lights every node a batched proposal resolves and stops polling once all are proposed', async () => {
    mockFetchProposals.mockResolvedValue([proposal({
      source: 'validation', node_id: 's.a', status: 'proposed',
      resolved_node_ids: ['s.a', 's.b'],
      node_outcomes: { 's.a': { status: 'proposed', reason: '' }, 's.b': { status: 'proposed', reason: '' } },
    })]);
    renderPage(makeRelease([
      node({ stage: 'validation', node_id: 's.a' }),
      node({ stage: 'validation', node_id: 's.b' }),
    ], 'validation_failed'));
    await screen.findByText('s.b');
    await waitFor(() => expect(screen.getAllByText(/Proposed fix available/)).toHaveLength(2));
    expect(screen.queryByText(/Try again/)).toBeNull();
  });

  it('shows a per-node note for a member the batched attempt skipped while another verifies', async () => {
    mockFetchProposals.mockResolvedValue([proposal({
      source: 'validation', node_id: 's.a', status: 'verifying',
      resolved_node_ids: ['s.a', 's.b'],
      node_outcomes: { 's.a': { status: 'verifying', reason: '' }, 's.b': { status: 'skipped', reason: 'No source to fix at this commit.' } },
    })]);
    renderPage(makeRelease([
      node({ stage: 'validation', node_id: 's.a' }),
      node({ stage: 'validation', node_id: 's.b' }),
    ], 'validation_failed'));
    await screen.findByText('s.b');
    expect(await screen.findByText(/Verifying fix/)).toBeInTheDocument();
    expect(await screen.findByText(/No source to fix at this commit\. Fix it in the repository\./)).toBeInTheDocument();
  });
});

describe('ReleaseDetailPage — live polling while non-terminal', () => {
  it('polls while validating and stops once the release reaches a terminal status', async () => {
    vi.useFakeTimers();
    try {
      const base = {
        release_id: 'rel-1', transitions: [], validation_node_ids: null, reject_reason: '',
        failing_nodes: null, image_tags: {}, bootstrap: false, remediation_round: 1,
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
        failing_nodes: null, image_tags: {}, bootstrap: false, remediation_round: 1,
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
      bootstrap: false,
      remediation_round: 1,
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
      bootstrap: false,
      remediation_round: 1,
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
        failing_nodes: null, image_tags: {}, bootstrap: false, remediation_round: 1,
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

describe('ReleaseDetailPage — proposal polling settles per node on a batched attempt', () => {
  it('stops after the first refresh once every failed node is terminal (one proposed, one skipped)', async () => {
    vi.useFakeTimers();
    try {
      mockFetchProposals.mockResolvedValue([proposal({
        source: 'validation', node_id: 's.a', status: 'proposed',
        resolved_node_ids: ['s.a', 's.b'],
        node_outcomes: { 's.a': { status: 'proposed', reason: '' }, 's.b': { status: 'skipped', reason: 'No source to fix at this commit.' } },
      })]);
      renderPage(makeRelease([
        node({ stage: 'validation', node_id: 's.a' }),
        node({ stage: 'validation', node_id: 's.b' }),
      ], 'validation_failed'));

      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockFetchProposals).toHaveBeenCalledTimes(1);
      expect(screen.getByText(/Proposed fix available/)).toBeInTheDocument();
      expect(screen.getByText(/No source to fix at this commit\. Fix it in the repository\./)).toBeInTheDocument();

      // Both failed nodes settled on the first refresh (proposed / skipped),
      // so the interval must have been cleared — advancing several more poll
      // intervals issues no further fetch.
      await act(async () => { await vi.advanceTimersByTimeAsync(5000 * 4); });
      expect(mockFetchProposals).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps polling while one resolved node is still verifying, even though another is already proposed', async () => {
    vi.useFakeTimers();
    try {
      mockFetchProposals.mockResolvedValue([proposal({
        source: 'validation', node_id: 's.a', status: 'proposed',
        resolved_node_ids: ['s.a', 's.b'],
        node_outcomes: { 's.a': { status: 'proposed', reason: '' }, 's.b': { status: 'verifying', reason: '' } },
      })]);
      renderPage(makeRelease([
        node({ stage: 'validation', node_id: 's.a' }),
        node({ stage: 'validation', node_id: 's.b' }),
      ], 'validation_failed'));

      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(mockFetchProposals).toHaveBeenCalledTimes(1);
      expect(screen.getByText(/Verifying fix/)).toBeInTheDocument();

      // s.b is still 'verifying' — not settled — so the next tick must still
      // issue a fetch (the cap is also suspended while any node verifies).
      await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
      expect(mockFetchProposals).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('ReleaseDetailPage — retry a dead-end rejected release', () => {
  it('shows Try again for a rejected release at a dead end and posts the retry', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3, rationale: '' })]);
    mockPost.mockResolvedValue({ ok: true, status: 202, json: async () => ({ release_id: 'rel-1', remediation_round: 2 }) });
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    const btn = await screen.findByRole('button', { name: 'Try again (round 1 of 3)' });
    fireEvent.click(btn);
    await waitFor(() => expect(mockPost).toHaveBeenCalledWith('/api/releases/rel-1/retry-remediation', expect.objectContaining({ method: 'POST' })));
  });

  it('presents the retry as a titled action banner, not a bare button', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3, rationale: '' })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    const btn = await screen.findByRole('button', { name: 'Try again (round 1 of 3)' });
    // The action must read as the next thing to do: a banner that says what
    // happened, with the retry as its primary call-to-action.
    expect(screen.getByText(/No fix was produced for this release/i)).toBeTruthy();
    expect(btn.closest('.action-banner')).not.toBeNull();
  });

  it('bumps the round shown in the header and restarts proposal polling after a successful retry (202)', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3 })]);
    mockPost.mockResolvedValue({ ok: true, status: 202, json: async () => ({ release_id: 'rel-1', remediation_round: 2 }) });
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    const btn = await screen.findByRole('button', { name: 'Try again (round 1 of 3)' });
    const callsBeforeRetry = mockFetchProposals.mock.calls.length;

    fireEvent.click(btn);

    await waitFor(() => {
      const pill = document.querySelector('.pill');
      expect(pill!.textContent).toBe('rejected · round 2');
    });
    await waitFor(() => expect(mockFetchProposals.mock.calls.length).toBeGreaterThan(callsBeforeRetry));
  });

  it('hides Try again once the FIX cell already shows a proposed fix', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'proposed' })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    await screen.findByText(/Proposed fix available/);
    expect(screen.queryByRole('button', { name: /Try again/ })).toBeNull();
  });

  it('hides Try again when the current round has no proposals yet (a retry is already in progress)', async () => {
    mockFetchProposals.mockResolvedValue([]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    await screen.findByText('finance');
    await waitFor(() => expect(mockFetchProposals).toHaveBeenCalled());
    expect(screen.queryByRole('button', { name: /Try again/ })).toBeNull();
  });

  it('shows Try again when the only round-1 proposal is proposed but its PR was rejected', async () => {
    mockFetchProposals.mockResolvedValue([proposal({
      source: 'compile', node_id: 'finance', status: 'proposed', pr_state: 'rejected', remediation_round: 1,
    })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    expect(await screen.findByRole('button', { name: 'Try again (round 1 of 3)' })).toBeInTheDocument();
  });

  it('hides Try again when a batched proposal has one owning service rejected but another still open', async () => {
    // The server mirrors the singular pr_state from pull_requests[0] — here
    // service 'a' (alphabetically first, and rejected) — so a reader that
    // only looks at the singular field would misjudge this attempt as a dead
    // end even though service 'b' still has a PR open for review.
    mockFetchProposals.mockResolvedValue([proposal({
      source: 'compile', node_id: 'finance', status: 'proposed', pr_state: 'rejected', remediation_round: 1,
      pr_services: ['a', 'b'],
      pull_requests: [
        {
          service: 'a', repo: 'demo', branch: 'fix/a', pr_url: 'https://x/pr/1', pr_number: 1,
          pr_state: 'rejected', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '',
        },
        {
          service: 'b', repo: 'demo', branch: 'fix/b', pr_url: 'https://x/pr/2', pr_number: 2,
          pr_state: 'open', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '',
        },
      ],
    })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    await screen.findByText('finance');
    await waitFor(() => expect(mockFetchProposals).toHaveBeenCalled());
    expect(screen.queryByRole('button', { name: /Try again/ })).toBeNull();
  });

  it('hides Try again when the only round-1 proposal is proposed with no PR yet', async () => {
    mockFetchProposals.mockResolvedValue([proposal({
      source: 'compile', node_id: 'finance', status: 'proposed', pr_state: '', remediation_round: 1,
    })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    await screen.findByText('finance');
    await waitFor(() => expect(mockFetchProposals).toHaveBeenCalled());
    expect(screen.queryByRole('button', { name: /Try again/ })).toBeNull();
  });

  it('hides Try again when the release has no failed nodes', async () => {
    mockFetchProposals.mockResolvedValue([]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'ok' })]));
    await screen.findByText('finance');
    await waitFor(() => expect(mockFetchProposals).toHaveBeenCalled());
    expect(screen.queryByRole('button', { name: /Try again/ })).toBeNull();
  });

  it('shows the retry-in-progress message on a 409 retry_in_progress', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3 })]);
    mockPost.mockResolvedValue({ ok: false, status: 409, json: async () => ({ error: 'retry_in_progress' }) });
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    const btn = await screen.findByRole('button', { name: 'Try again (round 1 of 3)' });
    fireEvent.click(btn);
    expect(await screen.findByText(/A retry is already in progress — wait for the new round to start\./)).toBeInTheDocument();
    expect(document.querySelector('[role="alert"]')).not.toBeNull();
  });

  it('shows a link to the open PR when retry answers 409 proposal_open', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3 })]);
    mockPost.mockResolvedValue({ ok: false, status: 409, json: async () => ({ error: 'proposal_open', pr_url: 'https://x/pr/7' }) });
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    const btn = await screen.findByRole('button', { name: 'Try again (round 1 of 3)' });
    fireEvent.click(btn);
    expect(await screen.findByText(/A fix is already proposed: https:\/\/x\/pr\/7/)).toBeInTheDocument();
  });

  it('says a fix is already proposed when 409 proposal_open carries no pr_url', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3 })]);
    mockPost.mockResolvedValue({ ok: false, status: 409, json: async () => ({ error: 'proposal_open' }) });
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    const btn = await screen.findByRole('button', { name: 'Try again (round 1 of 3)' });
    fireEvent.click(btn);
    expect(await screen.findByText(/A fix is already proposed — review it on the Remediation tab\./)).toBeInTheDocument();
  });

  it('maps a release-controller refusal reason to its message', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3 })]);
    mockPost.mockResolvedValue({ ok: false, status: 409, json: async () => ({ error: 'not_healable' }) });
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    const btn = await screen.findByRole('button', { name: 'Try again (round 1 of 3)' });
    fireEvent.click(btn);
    expect(await screen.findByText(/This rejection is not something the agent can fix\./)).toBeInTheDocument();
  });

  it('maps a 502 proposal_reader_unavailable refusal to its message', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3 })]);
    mockPost.mockResolvedValue({ ok: false, status: 502, json: async () => ({ error: 'proposal_reader_unavailable' }) });
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    const btn = await screen.findByRole('button', { name: 'Try again (round 1 of 3)' });
    fireEvent.click(btn);
    expect(await screen.findByText('The remediation service is unreachable — try again in a moment.')).toBeInTheDocument();
  });

  it('treats a proposal with remediation_round 0 as round 1 (Try again shows on a round-1 dead end)', async () => {
    // A missing remediation_round arrives over the wire as 0, not undefined —
    // ui/src/server/remediation-client.ts loads the proto with `defaults:
    // true`. The proposal must still be read as belonging to round 1.
    mockFetchProposals.mockResolvedValue([proposal({
      source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3, remediation_round: 0,
    })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    expect(await screen.findByRole('button', { name: 'Try again (round 1 of 3)' })).toBeInTheDocument();
  });

  it('says push a new commit when the release is on round 3 at a dead end', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3, remediation_round: 3 })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })], 'compile_failed', 3));
    expect(await screen.findByText(/Retried 3 times — push a new commit to start over\./)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Try again/ })).toBeNull();
  });

  it('explains a skipped attempt in the FIX cell', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'skipped', rationale: 'offending file not found at commit' })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    expect(await screen.findByText(/offending file not found at commit/)).toBeInTheDocument();
    expect(screen.getByText(/fix it in the repository/i)).toBeInTheDocument();
  });

  it('explains an escalated attempt in the FIX cell', async () => {
    mockFetchProposals.mockResolvedValue([proposal({ source: 'compile', node_id: 'finance', status: 'escalated' })]);
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    expect(await screen.findByText(/Attempt budget spent\./)).toBeInTheDocument();
  });

  it('shows the remediation round next to status once retried', async () => {
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })], 'compile_failed', 2));
    await screen.findByText('finance');
    const pill = document.querySelector('.pill');
    expect(pill!.textContent).toBe('rejected · round 2');
  });

  it('does not show a round suffix on the first remediation round', async () => {
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'svc', file_path: 'models/x.sql' })]));
    await screen.findByText('models/x.sql');
    const pill = document.querySelector('.pill');
    expect(pill!.textContent).toBe('rejected');
  });

  it('keeps Try again hidden right after a 202 even though only the old round\'s proposal has been refetched, then reflects the new round live', async () => {
    vi.useFakeTimers();
    try {
      // Round 1's only proposal is a terminal, non-actionable escalation — the
      // release-controller would not have accepted a retry otherwise.
      const round1Escalated = proposal({ source: 'compile', node_id: 'finance', status: 'escalated', attempt: 3, remediation_round: 1 });
      mockFetchProposals.mockResolvedValue([round1Escalated]);
      mockPost.mockResolvedValue({ ok: true, status: 202, json: async () => ({ release_id: 'rel-1', remediation_round: 2 }) });

      renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      expect(screen.getByRole('button', { name: 'Try again (round 1 of 3)' })).toBeInTheDocument();

      fireEvent.click(screen.getByRole('button', { name: 'Try again (round 1 of 3)' }));
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });

      const pill = document.querySelector('.pill');
      expect(pill!.textContent).toBe('rejected · round 2');
      // The wire still only carries round 1's terminal proposal — the button
      // must not reappear off that stale, already-spent round.
      expect(screen.queryByRole('button', { name: /Try again/ })).toBeNull();

      // Round 2's first attempt lands on the next poll tick.
      mockFetchProposals.mockResolvedValue([
        round1Escalated,
        proposal({ source: 'compile', node_id: 'finance', status: 'generating', attempt: 4, remediation_round: 2 }),
      ]);
      await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
      expect(screen.getByText(/Generating fix/)).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: /Try again/ })).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('ReleaseDetailPage — verification runs section', () => {
  it('lists the verification runs that judged fixes for this release', async () => {
    mockVerificationRuns([
      {
        run_id: 'verify-rel-1-core-a1', status: 'failed', service: 'core', attempt: 1,
        created_at: '2026-09-02T10:00:00Z', activated_at: '2026-09-02T10:01:00Z',
        finished_at: '2026-09-02T10:05:00Z', fail_reason: 'validation_failed',
      },
      {
        run_id: 'verify-rel-1-core-a2', status: 'validating', service: 'core', attempt: 2,
        created_at: '2026-09-02T10:06:00Z', activated_at: '2026-09-02T10:07:00Z', finished_at: '',
      },
    ]);
    // The per-node result's own status is 'ok' (not 'failed'), so the pill
    // text asserted below unambiguously belongs to the verification-runs
    // table, not the per-node results table above it.
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'ok' })]));
    expect(await screen.findByText('Verification runs')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'verify-rel-1-core-a2' })).toHaveAttribute('href', '/verifications/verify-rel-1-core-a2');
    expect(screen.getByText('running')).toBeInTheDocument();
    expect(screen.getByText('failed')).toBeInTheDocument();
  });

  it('renders no section when the release has no verification runs', async () => {
    renderPage(makeRelease([node({ stage: 'compile', node_id: 'finance', status: 'failed' })]));
    await screen.findByText('finance');
    await waitFor(() => expect(mockFetchProposals).toHaveBeenCalled());
    expect(screen.queryByText('Verification runs')).toBeNull();
  });
});
