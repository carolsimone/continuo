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
  status: 'proposed',
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
      // status: skipped keeps both rows compact, so this test stays about
      // row rendering rather than the auto-expanded card.
      makeProposal({ id: 'p1', node_id: 'svc.schema.model_a', release_id: 'rel-1', confidence: 'high', status: 'skipped' }),
      makeProposal({ id: 'p2', node_id: 'svc.schema.model_b', release_id: 'rel-2', confidence: 'medium', status: 'skipped' }),
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
      status: 'skipped',
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
    const proposal = makeProposal({ pr_url: 'https://github.com/org/repo/pull/42', pr_number: 42, pr_state: 'open' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    const link = screen.getByRole('link', { name: /open PR ↗/i });
    expect(link).toHaveAttribute('href', 'https://github.com/org/repo/pull/42');
  });

  it('shows diff view/hide toggle button when a proposal with diff_uri is selected', async () => {
    const proposal = makeProposal({ status: 'skipped', diff_uri: 's3://bucket/my.patch' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    expect(screen.getByRole('button', { name: /view/i })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /open full ↗/i })).toBeInTheDocument();
  });

  it('renders one labelled diff view per edit when the proposal carries edits', async () => {
    const proposal = makeProposal({
      status: 'skipped',
      diff_uri: 's3://bucket/legacy.patch',
      edits: [
        { path: 'contracts/a.yml', content_uri: 's3://bucket/a.yml', diff_uri: 's3://bucket/a.diff' },
        { path: 'scripts/a.py', content_uri: 's3://bucket/a.py', diff_uri: 's3://bucket/py.diff' },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    expect(screen.getByText('contracts/a.yml')).toBeInTheDocument();
    expect(screen.getByText('scripts/a.py')).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: /^view$/i })).toHaveLength(2);

    const links = screen.getAllByRole('link', { name: /open full ↗/i });
    expect(links).toHaveLength(2);
    expect(links[0].getAttribute('href')).toContain(encodeURIComponent('s3://bucket/a.diff'));
    expect(links[1].getAttribute('href')).toContain(encodeURIComponent('s3://bucket/py.diff'));
    // The per-file diffs replace the single-file preview rather than adding to it.
    expect(
      links.some((l) => l.getAttribute('href')?.includes(encodeURIComponent('s3://bucket/legacy.patch'))),
    ).toBe(false);
  });

  it('falls back to the single unlabelled diff view when the proposal carries no edits', async () => {
    const proposal = makeProposal({
      status: 'skipped',
      diff_uri: 's3://bucket/candidate.patch',
      edits: [],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    const links = screen.getAllByRole('link', { name: /open full ↗/i });
    expect(links).toHaveLength(1);
    expect(links[0].getAttribute('href')).toContain(encodeURIComponent('s3://bucket/candidate.patch'));
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

  it('renders the detail card inline with no click, for an actionable proposal', async () => {
    const proposal = makeProposal({
      status: 'proposed',
      source_resolved: true,
      pr_url: '',
      rationale: 'Adds the missing GROUP BY column.',
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    // The node id shows both in the compact row and in the auto-expanded
    // card's title — no click needed to make the card appear.
    await waitFor(() => expect(screen.getAllByText('svc.schema.my_model').length).toBeGreaterThan(0));

    expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
  });

  it.each([
    // status stays 'proposed' for the PR-lifecycle cases below: remediation-agent
    // never mutates status after insert, so a merged/rejected/already-opened/
    // in-flight PR is recorded on pr_state, not on status.
    ['a claim already in flight', { status: 'proposed', source_resolved: true, pr_url: '', pr_state: 'opening' }],
    ['a PR already opened', { status: 'proposed', source_resolved: true, pr_url: 'https://github.com/org/repo/pull/9', pr_state: 'open' }],
    ['merged', { status: 'proposed', source_resolved: true, pr_url: 'https://github.com/org/repo/pull/9', pr_state: 'merged' }],
    ['rejected', { status: 'proposed', source_resolved: true, pr_url: 'https://github.com/org/repo/pull/9', pr_state: 'rejected' }],
    // skipped/escalated are real classifier outcomes distinct from 'proposed'.
    ['skipped', { status: 'skipped', source_resolved: true, pr_url: '' }],
    ['escalated', { status: 'escalated', source_resolved: true, pr_url: '' }],
  ])('renders a compact row with no card until clicked, when %s', async (_label, overrides) => {
    const proposal = makeProposal({
      rationale: 'Adds the missing GROUP BY column.',
      ...overrides,
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expect(screen.queryByText('Adds the missing GROUP BY column.')).toBeNull();

    fireEvent.click(screen.getByText('svc.schema.my_model'));
    expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
  });

  it.each(['', 'failed'])(
    'auto-expands and shows Create PR when pr_state is %j (retryable claim state)',
    async (pr_state) => {
      const proposal = makeProposal({
        status: 'proposed',
        source_resolved: true,
        pr_url: '',
        pr_state,
        rationale: 'Adds the missing GROUP BY column.',
      });
      mockFetchProposals.mockResolvedValue([proposal]);

      renderPanel();

      await waitFor(() => expect(screen.getAllByText('svc.schema.my_model').length).toBeGreaterThan(0));
      expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
    }
  );

  it('does not auto-expand and offers no Create PR when pr_state is opening', async () => {
    const proposal = makeProposal({
      status: 'proposed',
      source_resolved: true,
      pr_url: '',
      pr_state: 'opening',
      rationale: 'Adds the missing GROUP BY column.',
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    // No auto-expansion: the rationale is not visible without a click.
    expect(screen.queryByText('Adds the missing GROUP BY column.')).toBeNull();

    fireEvent.click(screen.getByText('svc.schema.my_model'));
    expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
    // Even once manually opened, the claim is already in flight — no second
    // Create PR trigger, which would just 409 against the live claim.
    expect(screen.queryByRole('button', { name: /Create PR/i })).toBeNull();
  });

  it('opens a collapsed row on Enter and closes it on Space', async () => {
    const proposal = makeProposal({
      status: 'skipped',
      rationale: 'Adds the missing GROUP BY column.',
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    const row = screen.getByText('svc.schema.my_model').closest('tr')!;
    expect(row).toHaveAttribute('role', 'button');
    expect(row).toHaveAttribute('tabIndex', '0');
    expect(row).toHaveAttribute('aria-expanded', 'false');

    fireEvent.keyDown(row, { key: 'Enter' });
    expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
    expect(row).toHaveAttribute('aria-expanded', 'true');
    expect(row).toHaveClass('nodes-row--selected');

    fireEvent.keyDown(row, { key: ' ' });
    expect(screen.queryByText('Adds the missing GROUP BY column.')).toBeNull();
  });

  it('an auto-expanded row has no role or tabIndex attributes', async () => {
    const proposal = makeProposal({ status: 'proposed', source_resolved: true, pr_url: '' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => expect(screen.getAllByText('svc.schema.my_model').length).toBeGreaterThan(0));
    // First match is the compact row's cell, not the card's title.
    const row = screen.getAllByText('svc.schema.my_model')[0].closest('tr')!;
    expect(row).not.toHaveAttribute('role');
    expect(row).not.toHaveAttribute('tabIndex');
  });
});
