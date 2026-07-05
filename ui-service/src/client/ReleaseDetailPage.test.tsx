// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import ReleaseDetailPage from './ReleaseDetailPage';
import { NodeValidationResult, ReleaseDetail, ProposalDTO } from './types';

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
