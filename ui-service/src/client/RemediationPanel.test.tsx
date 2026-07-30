// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import RemediationPanel from './RemediationPanel';
import { ProposalDTO } from './types';

vi.mock('./remediation-api', () => ({
  fetchProposals: vi.fn(),
}));

import { fetchProposals } from './remediation-api';
const mockFetchProposals = fetchProposals as ReturnType<typeof vi.fn>;

const makeProposal = (overrides: Partial<ProposalDTO> = {}): ProposalDTO => ({
  id: 'prop-1',
  source: 's3://bucket/key.sql',
  release_id: 'rel-abc',
  node_id: 'svc.schema.my_model',
  error_signature: 'syntax error',
  attempt: 1,
  status: 'open',
  confidence: 'high',
  rationale: 'The fix addresses the syntax issue in the model.',
  proposed_sql_uri: 's3://bucket/proposed.sql',
  diff_uri: 's3://bucket/diff.patch',
  candidate_fix_sql_uri: '',
  candidate_fix_diff_uri: '',
  source_resolved: true,
  repo: 'org/repo',
  commit_sha: 'abc123',
  file_path: 'models/my_model.sql',
  model: 'my_model',
  created_at: '2026-06-24T10:00:00Z',
  pr_url: '',
  pr_number: 0,
  pr_state: '',
  pr_opened_at: '',
  pr_opened_by: '',
  pr_closed_at: '',
  ...overrides,
});

function renderPanel() {
  return render(
    <MemoryRouter>
      <RemediationPanel />
    </MemoryRouter>
  );
}

describe('RemediationPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders a row per proposal returned from fetchProposals', async () => {
    const proposals = [
      makeProposal({ id: 'p1', node_id: 'svc.schema.model_a', release_id: 'rel-1', confidence: 'high' }),
      makeProposal({ id: 'p2', node_id: 'svc.schema.model_b', release_id: 'rel-2', confidence: 'medium' }),
    ];
    mockFetchProposals.mockResolvedValue(proposals);

    renderPanel();

    await waitFor(() => {
      expect(screen.getByText('svc.schema.model_a')).toBeInTheDocument();
      expect(screen.getByText('svc.schema.model_b')).toBeInTheDocument();
    });

    expect(screen.getByText('rel-1')).toBeInTheDocument();
    expect(screen.getByText('rel-2')).toBeInTheDocument();
    expect(screen.getAllByText('high')).toHaveLength(1);
    expect(screen.getByText('medium')).toBeInTheDocument();
  });

  it('clicking a row reveals the proposal rationale', async () => {
    const proposal = makeProposal({
      rationale: 'Fixes the JOIN clause that was missing a condition.',
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));

    // Rationale not yet visible
    expect(screen.queryByText('Fixes the JOIN clause that was missing a condition.')).toBeNull();

    fireEvent.click(screen.getByText('svc.schema.my_model'));

    expect(screen.getByText('Fixes the JOIN clause that was missing a condition.')).toBeInTheDocument();
  });

  it('shows the warning info-strip when source_resolved is false', async () => {
    const proposal = makeProposal({ source_resolved: false });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    expect(
      screen.getByText(/No real-source fix — a PR cannot be opened for this proposal/)
    ).toBeInTheDocument();
  });

  it('shows the neutral info-strip when there are no proposals', async () => {
    mockFetchProposals.mockResolvedValue([]);

    renderPanel();

    await waitFor(() => {
      expect(screen.getByText('No proposals yet.')).toBeInTheDocument();
    });
  });

  it('shows open PR link when proposal has a pr_url', async () => {
    const proposal = makeProposal({ pr_url: 'https://github.com/org/repo/pull/42', pr_number: 42 });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    const link = screen.getByRole('link', { name: /open PR ↗/i });
    expect(link).toHaveAttribute('href', 'https://github.com/org/repo/pull/42');
  });

  it('shows diff view/hide toggle button when a proposal with diff_uri is selected', async () => {
    const proposal = makeProposal({ diff_uri: 's3://bucket/my.patch' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    expect(screen.getByRole('button', { name: /view/i })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /open full ↗/i })).toBeInTheDocument();
  });

  it('renders a merged chip for a proposal whose PR was merged', async () => {
    const proposal = makeProposal({
      pr_state: 'merged',
      pr_url: 'https://github.com/org/repo/pull/7',
      pr_number: 7,
      pr_closed_at: '2026-07-03T10:00:00Z',
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    const chip = await screen.findByText('merged');
    expect(chip).toHaveClass('pr-chip', 'pr-chip--merged');
  });

  it('renders a rejected chip for a proposal whose PR was closed without merge', async () => {
    const proposal = makeProposal({
      pr_state: 'rejected',
      pr_url: 'https://github.com/org/repo/pull/7',
      pr_number: 7,
      pr_closed_at: '2026-07-03T10:00:00Z',
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    const chip = await screen.findByText('rejected');
    expect(chip).toHaveClass('pr-chip', 'pr-chip--rejected');
  });
});
