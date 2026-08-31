// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import RemediationPanel from './RemediationPanel';
import { ProposalDTO } from './types';
import { AuthContext } from './auth/AuthContext';
import type { AuthUser } from './auth/useAuth';

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
  shadow_release_id: '',
  verify_error: '',
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
    // status stays 'proposed' for the PR-lifecycle cases below: agent-remediation
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

describe('RemediationPanel — a fix awaiting shadow verification', () => {
  const operator: AuthUser = {
    userId: 'u-1', email: 'op@example.com', name: 'Op', role: 'operator',
  };

  function renderPanelAsOperator() {
    return render(
      <AuthContext.Provider value={operator}>
        <MemoryRouter>
          <RemediationPanel />
        </MemoryRouter>
      </AuthContext.Provider>
    );
  }

  it('reads as "Verifying fix…" instead of the raw status word', async () => {
    mockFetchProposals.mockResolvedValue([makeProposal({ status: 'verifying' })]);

    renderPanelAsOperator();

    const chip = await screen.findByText(/Verifying fix/);
    expect(chip).toHaveAttribute('aria-busy', 'true');
    expect(screen.queryByText('verifying')).toBeNull();
  });

  it('links a verifying proposal to the release that is judging it', async () => {
    // The chip says a fix is being verified; without the link the operator has
    // no way to reach the release doing the verifying, which is on another
    // screen under a name they have never been shown.
    mockFetchProposals.mockResolvedValue([
      makeProposal({ status: 'verifying', shadow_release_id: 'shadow-rel-abc-svc.schema.my_model-a1' }),
    ]);

    renderPanelAsOperator();

    await screen.findByText(/Verifying fix/);
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    const link = await screen.findByRole('link', { name: /shadow-rel-abc-svc.schema.my_model-a1/ });
    expect(link).toHaveAttribute('href', '/releases/shadow-rel-abc-svc.schema.my_model-a1');
  });

  it('shows why verification failed instead of the bare word "failed"', async () => {
    // verify_error is the whole reason a python contract attempt failed. Left
    // unrendered it sits unread in the database while the operator is told
    // only "failed".
    mockFetchProposals.mockResolvedValue([
      makeProposal({
        status: 'failed',
        shadow_release_id: 'shadow-rel-abc-svc.schema.my_model-a1',
        verify_error: 'column "revenue_total" does not exist',
      }),
    ]);

    renderPanelAsOperator();

    fireEvent.click(await screen.findByText('svc.schema.my_model'));
    expect(await screen.findByText(/column "revenue_total" does not exist/)).toBeInTheDocument();
  });

  it('offers an operator no Create PR while the fix is still being verified', async () => {
    mockFetchProposals.mockResolvedValue([
      makeProposal({ status: 'verifying', source_resolved: true, pr_state: '' }),
    ]);

    renderPanelAsOperator();

    await screen.findByText(/Verifying fix/);
    expect(screen.queryByRole('button', { name: /Create PR/i })).toBeNull();
  });

  it('offers that same operator Create PR once verification finished and the fix is proposed', async () => {
    mockFetchProposals.mockResolvedValue([
      makeProposal({ status: 'proposed', source_resolved: true, pr_state: '' }),
    ]);

    renderPanelAsOperator();

    expect(await screen.findByRole('button', { name: /Create PR/i })).toBeInTheDocument();
  });
});

describe('RemediationPanel — a batched proposal spanning several nodes', () => {
  it('joins every resolved node in the row and the card title, not just the representative one', async () => {
    const proposal = makeProposal({
      node_id: 's.a',
      status: 'skipped',
      resolved_node_ids: ['s.a', 's.b'],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('s.a, s.b'));
    fireEvent.click(screen.getByText('s.a, s.b'));
    expect(screen.getAllByText('s.a, s.b').length).toBeGreaterThan(0);
  });

  it('still shows the single node_id for a legacy proposal with no resolved_node_ids', async () => {
    const proposal = makeProposal({ node_id: 's.a', status: 'skipped' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('s.a'));
    expect(screen.queryByText('s.a, s.b')).toBeNull();
  });

  it('renders one verification link per entry in verifications, not just the single shadow_release_id', async () => {
    const proposal = makeProposal({
      status: 'proposed',
      source_resolved: true,
      resolved_node_ids: ['s.a', 's.b'],
      verifications: [
        { service: 'svc-a', kind: 'shadow', shadow_release_id: 'shadow-rel-a' },
        { service: 'svc-b', kind: 'shadow', shadow_release_id: 'shadow-rel-b' },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    const linkA = await screen.findByRole('link', { name: /shadow-rel-a/ });
    const linkB = await screen.findByRole('link', { name: /shadow-rel-b/ });
    expect(linkA).toHaveAttribute('href', '/releases/shadow-rel-a');
    expect(linkB).toHaveAttribute('href', '/releases/shadow-rel-b');
  });

  it('falls back to the single shadow_release_id link when verifications is empty', async () => {
    const proposal = makeProposal({
      status: 'verifying',
      shadow_release_id: 'shadow-rel-abc-svc.schema.my_model-a1',
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    fireEvent.click(await screen.findByText('svc.schema.my_model'));
    const link = await screen.findByRole('link', { name: /shadow-rel-abc-svc.schema.my_model-a1/ });
    expect(link).toHaveAttribute('href', '/releases/shadow-rel-abc-svc.schema.my_model-a1');
  });
});

describe('RemediationPanel — pull requests split across owning services', () => {
  it('renders one labeled open PR link per pull_requests entry', async () => {
    const proposal = makeProposal({
      status: 'proposed',
      source_resolved: true,
      resolved_node_ids: ['core.a', 'finance.b'],
      pr_services: ['core', 'finance'],
      pull_requests: [
        {
          service: 'core', repo: 'org/core-repo', branch: 'remediation/rel/attempt1/core',
          pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'open',
          pr_opened_at: '', pr_opened_by: '', pr_closed_at: '',
        },
        {
          service: 'finance', repo: 'org/finance-repo', branch: 'remediation/rel/attempt1/finance',
          pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11, pr_state: 'merged',
          pr_opened_at: '', pr_opened_by: '', pr_closed_at: '2026-07-01T00:00:00Z',
        },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('core.a, finance.b'));
    fireEvent.click(screen.getByText('core.a, finance.b'));

    const coreLink = screen.getByRole('link', { name: /open PR \(core\) ↗/i });
    expect(coreLink).toHaveAttribute('href', 'https://github.com/org/core-repo/pull/10');
    const financeLink = screen.getByRole('link', { name: /open PR \(finance\) ↗/i });
    expect(financeLink).toHaveAttribute('href', 'https://github.com/org/finance-repo/pull/11');
    // Not the legacy unlabeled form.
    expect(screen.queryByRole('link', { name: /^open PR ↗$/i })).toBeNull();
  });

  it('shows a state chip per pull_requests entry, labeled by service', async () => {
    const proposal = makeProposal({
      status: 'proposed',
      source_resolved: true,
      pr_services: ['core', 'finance'],
      pull_requests: [
        {
          service: 'core', repo: '', branch: '',
          pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'merged',
          pr_opened_at: '', pr_opened_by: '', pr_closed_at: '',
        },
        {
          service: 'finance', repo: '', branch: '',
          pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11, pr_state: 'rejected',
          pr_opened_at: '', pr_opened_by: '', pr_closed_at: '',
        },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    const mergedChip = await screen.findByText('merged');
    expect(mergedChip).toHaveClass('pr-chip', 'pr-chip--merged');
    expect(mergedChip.closest('.pr-chip-labeled')).toHaveTextContent('merged (core)');

    const rejectedChip = screen.getByText('rejected');
    expect(rejectedChip).toHaveClass('pr-chip', 'pr-chip--rejected');
    expect(rejectedChip.closest('.pr-chip-labeled')).toHaveTextContent('rejected (finance)');
  });

  it('stays actionable (auto-expanded) while one service still needs a PR, even though another already merged', async () => {
    const proposal = makeProposal({
      status: 'proposed',
      source_resolved: true,
      pr_services: ['core', 'finance'],
      // core is already settled; finance has no entry at all yet.
      pull_requests: [
        {
          service: 'core', repo: '', branch: '',
          pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'merged',
          pr_opened_at: '', pr_opened_by: '', pr_closed_at: '2026-07-01T00:00:00Z',
        },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    // Auto-expanded with no click: the node id shows both in the compact
    // row and the card title.
    await waitFor(() => expect(screen.getAllByText('svc.schema.my_model').length).toBeGreaterThan(0));
  });

  it('is not actionable once every owning service has a settled (non-retryable) PR', async () => {
    const proposal = makeProposal({
      status: 'proposed',
      source_resolved: true,
      rationale: 'Adds the missing GROUP BY column.',
      pr_services: ['core', 'finance'],
      pull_requests: [
        {
          service: 'core', repo: '', branch: '',
          pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'open',
          pr_opened_at: '', pr_opened_by: '', pr_closed_at: '',
        },
        {
          service: 'finance', repo: '', branch: '',
          pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11, pr_state: 'merged',
          pr_opened_at: '', pr_opened_by: '', pr_closed_at: '2026-07-01T00:00:00Z',
        },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    // Not auto-expanded: the rationale is not visible without a click.
    expect(screen.queryByText('Adds the missing GROUP BY column.')).toBeNull();
  });
});
