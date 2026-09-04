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
  fetchNodeServices: vi.fn(),
}));

import { fetchProposals, fetchNodeServices } from './remediation-api';
const mockFetchProposals = fetchProposals as ReturnType<typeof vi.fn>;
const mockFetchNodeServices = fetchNodeServices as ReturnType<typeof vi.fn>;

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
  verification_run_id: '',
  verify_error: '',
  ...overrides,
});

const operator: AuthUser = { userId: 'u-1', email: 'op@example.com', name: 'Op', role: 'operator' };

function renderPanel() {
  return render(
    <MemoryRouter>
      <RemediationPanel />
    </MemoryRouter>
  );
}

function renderPanelAsOperator() {
  return render(
    <AuthContext.Provider value={operator}>
      <MemoryRouter>
        <RemediationPanel />
      </MemoryRouter>
    </AuthContext.Provider>
  );
}

// The Nodes cell of a group row carries the union of the group's node ids; it
// is the group row's clickable handle in these tests. clicking it toggles the
// (non-actionable) group open.
const expandGroupByNodes = (nodesText: string) => fireEvent.click(screen.getByText(nodesText));

// Confidence is rendered only in an attempt row, never the group row, so it is
// an unambiguous handle for opening a single non-actionable attempt.
const openAttemptByConfidence = (confidence = 'high') => fireEvent.click(screen.getByText(confidence));

describe('RemediationPanel — group list', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeServices.mockResolvedValue([]);
  });

  it('renders one group row per (release, round), newest group first', async () => {
    const proposals = [
      makeProposal({ id: 'p1', node_id: 'svc.schema.model_a', release_id: 'rel-1', status: 'skipped', created_at: '2026-06-24T10:00:00Z' }),
      makeProposal({ id: 'p2', node_id: 'svc.schema.model_b', release_id: 'rel-2', status: 'skipped', created_at: '2026-06-25T10:00:00Z' }),
    ];
    mockFetchProposals.mockResolvedValue(proposals);

    renderPanel();

    await waitFor(() => {
      expect(screen.getByText('svc.schema.model_a')).toBeInTheDocument();
      expect(screen.getByText('svc.schema.model_b')).toBeInTheDocument();
    });
    expect(screen.getByText('rel-1')).toBeInTheDocument();
    expect(screen.getByText('rel-2')).toBeInTheDocument();

    // Newest group (rel-2, created later) sorts first.
    const releaseCells = screen.getAllByText(/^rel-\d$/).map(el => el.textContent);
    expect(releaseCells).toEqual(['rel-2', 'rel-1']);
  });

  it('collapses several attempts of one (release, round) into a single group row', async () => {
    const proposals = [
      makeProposal({ id: 'a1', release_id: 'rel-x', remediation_round: 1, attempt: 1, status: 'failed', created_at: '2026-06-24T10:00:00Z' }),
      makeProposal({ id: 'a2', release_id: 'rel-x', remediation_round: 1, attempt: 2, status: 'skipped', created_at: '2026-06-24T11:00:00Z' }),
    ];
    mockFetchProposals.mockResolvedValue(proposals);

    renderPanel();

    // One group row, and its Attempts cell reads 2.
    await waitFor(() => screen.getByText('rel-x'));
    expect(screen.getAllByText('rel-x')).toHaveLength(1);
    const groupRow = screen.getByText('rel-x').closest('tr')!;
    expect(groupRow).toHaveTextContent('2'); // attempts count
  });

  it('shows the union of services and nodes for a group', async () => {
    const proposal = makeProposal({
      status: 'skipped',
      node_id: 'core.a',
      resolved_node_ids: ['core.a', 'finance.b'],
      pr_services: ['finance', 'core'],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('core.a, finance.b'));
    // Services union is sorted.
    expect(screen.getByText('core, finance')).toBeInTheDocument();
  });

  it('derives the Services column from the nodes an unsplit attempt resolves', async () => {
    // No pr_services: a legacy or single-service attempt. The services still
    // come from the resolved node ids — the first dotted segment of a
    // "{service}.{schema}.{table}" id, or the whole id for a compile-stage
    // node, which is the bare service name.
    const proposal = makeProposal({
      status: 'skipped',
      node_id: 'analytics.public.orders',
      resolved_node_ids: ['analytics.public.orders', 'analytics.public.customers', 'billing'],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('analytics.public.customers, analytics.public.orders, billing'));
    expect(screen.getByText('analytics, billing')).toBeInTheDocument();
  });

  it('shows the neutral info-strip when there are no proposals', async () => {
    mockFetchProposals.mockResolvedValue([]);
    renderPanel();
    await waitFor(() => expect(screen.getByText('No proposals yet.')).toBeInTheDocument());
  });

  it('renders the merged chip in the group PR column without expanding', async () => {
    const proposal = makeProposal({ status: 'skipped', pr_state: 'merged', pr_url: 'https://github.com/org/repo/pull/7', pr_number: 7 });
    mockFetchProposals.mockResolvedValue([proposal]);
    renderPanel();
    const chip = await screen.findByText('merged');
    expect(chip).toHaveClass('pr-chip', 'pr-chip--merged');
  });

  it('renders the rejected chip in the group PR column without expanding', async () => {
    const proposal = makeProposal({ status: 'skipped', pr_state: 'rejected', pr_url: 'https://github.com/org/repo/pull/7', pr_number: 7 });
    mockFetchProposals.mockResolvedValue([proposal]);
    renderPanel();
    const chip = await screen.findByText('rejected');
    expect(chip).toHaveClass('pr-chip', 'pr-chip--rejected');
  });

  it('reads a verifying group as an in-progress pill in the Latest status column, without expanding', async () => {
    mockFetchProposals.mockResolvedValue([makeProposal({ status: 'verifying' })]);
    renderPanel();
    const pill = await screen.findByText('verifying');
    expect(pill).toHaveClass('pill-sm', 'pill-sm--running');
    expect(pill).toHaveAttribute('aria-busy', 'true');
  });
});

describe('RemediationPanel — status pills', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeServices.mockResolvedValue([]);
  });

  it.each([
    ['proposed',   'pill-sm--succeeded'],
    ['failed',     'pill-sm--failed'],
    ['escalated',  'pill-sm--failed'],
    ['skipped',    'pill-sm--skipped'],
    ['generating', 'pill-sm--pending'],
  ])('renders the %s status as a %s pill in the Latest status column', async (status, cls) => {
    // source_resolved=false keeps a 'proposed' attempt from auto-expanding,
    // so the only status text on screen is the group row's.
    mockFetchProposals.mockResolvedValue([makeProposal({ status, source_resolved: false })]);
    renderPanel();
    const pill = await screen.findByText(status);
    expect(pill).toHaveClass('pill-sm', cls);
  });

  it('renders the attempt row status as the same pill once the group is open', async () => {
    mockFetchProposals.mockResolvedValue([makeProposal({ status: 'failed' })]);
    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');

    const pills = screen.getAllByText('failed');
    expect(pills).toHaveLength(2); // group row + attempt row
    for (const pill of pills) expect(pill).toHaveClass('pill-sm', 'pill-sm--failed');
  });
});

describe('RemediationPanel — attempts are nested inside their group', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeServices.mockResolvedValue([]);
  });

  it('renders the attempt rows and their column labels inside the group\'s contained block, not as a peer table', async () => {
    mockFetchProposals.mockResolvedValue([makeProposal({ status: 'skipped' })]);
    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');

    // The attempt row and the inner column labels both live inside the
    // .remediation-attempts block hosted by the group's expansion row…
    const block = screen.getByText('high').closest('.remediation-attempts');
    expect(block).not.toBeNull();
    expect(screen.getByText('Confidence').closest('.remediation-attempts')).toBe(block);
    expect(block!.closest('tr')).toHaveClass('remediation-group__body');
    // …while the outer header is not inside any such block.
    expect(screen.getByText('Latest status').closest('.remediation-attempts')).toBeNull();
  });

  it('reads the source column as resolved / unresolved rather than yes / no', async () => {
    mockFetchProposals.mockResolvedValue([
      makeProposal({ id: 'a', status: 'skipped', source_resolved: true, attempt: 1 }),
      makeProposal({ id: 'b', status: 'skipped', source_resolved: false, attempt: 2, created_at: '2026-06-24T11:00:00Z' }),
    ]);
    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');

    expect(screen.getByText('resolved')).toBeInTheDocument();
    expect(screen.getByText('unresolved')).toBeInTheDocument();
    expect(screen.queryByText('yes')).toBeNull();
    expect(screen.queryByText('no')).toBeNull();
  });
});

describe('RemediationPanel — expanding a group and its attempts', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeServices.mockResolvedValue([]);
  });

  it('reveals attempt rows on group click, then the rationale on attempt click', async () => {
    const proposal = makeProposal({ status: 'skipped', rationale: 'Fixes the JOIN clause that was missing a condition.' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    // Nothing from the attempt is visible until the group is opened.
    expect(screen.queryByText('high')).toBeNull();
    expect(screen.queryByText('Fixes the JOIN clause that was missing a condition.')).toBeNull();

    expandGroupByNodes('svc.schema.my_model');
    // Attempt row now visible (confidence cell).
    expect(screen.getByText('high')).toBeInTheDocument();
    // Card still requires an attempt click.
    expect(screen.queryByText('Fixes the JOIN clause that was missing a condition.')).toBeNull();

    openAttemptByConfidence();
    expect(screen.getByText('Fixes the JOIN clause that was missing a condition.')).toBeInTheDocument();
  });

  it('opens a collapsed group on Enter and closes it on Space', async () => {
    const proposal = makeProposal({ status: 'skipped' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    const groupRow = screen.getByText('svc.schema.my_model').closest('tr')!;
    expect(groupRow).toHaveAttribute('role', 'button');
    expect(groupRow).toHaveAttribute('tabIndex', '0');
    expect(groupRow).toHaveAttribute('aria-expanded', 'false');

    fireEvent.keyDown(groupRow, { key: 'Enter' });
    expect(groupRow).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByText('high')).toBeInTheDocument(); // attempt row shown

    fireEvent.keyDown(groupRow, { key: ' ' });
    expect(groupRow).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByText('high')).toBeNull();
  });

  it('shows the source warning in the attempt card', async () => {
    const proposal = makeProposal({ status: 'skipped', source_resolved: false });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openAttemptByConfidence();

    expect(screen.getByText(/No real-source fix — a PR cannot be opened for this proposal/)).toBeInTheDocument();
  });

  it('shows the diff view/hide toggle in the attempt card', async () => {
    const proposal = makeProposal({ status: 'skipped', diff_uri: 's3://bucket/my.patch' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openAttemptByConfidence();

    expect(screen.getByRole('button', { name: /^view$/i })).toBeInTheDocument();
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
    expandGroupByNodes('svc.schema.my_model');
    openAttemptByConfidence();

    expect(screen.getByText('contracts/a.yml')).toBeInTheDocument();
    expect(screen.getByText('scripts/a.py')).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: /^view$/i })).toHaveLength(2);

    const links = screen.getAllByRole('link', { name: /open full ↗/i });
    expect(links).toHaveLength(2);
    expect(links[0].getAttribute('href')).toContain(encodeURIComponent('s3://bucket/a.diff'));
    expect(links[1].getAttribute('href')).toContain(encodeURIComponent('s3://bucket/py.diff'));
    expect(links.some((l) => l.getAttribute('href')?.includes(encodeURIComponent('s3://bucket/legacy.patch')))).toBe(false);
  });

  it('falls back to the single unlabelled diff view when the proposal carries no edits', async () => {
    const proposal = makeProposal({ status: 'skipped', diff_uri: 's3://bucket/candidate.patch', edits: [] });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openAttemptByConfidence();

    const links = screen.getAllByRole('link', { name: /open full ↗/i });
    expect(links).toHaveLength(1);
    expect(links[0].getAttribute('href')).toContain(encodeURIComponent('s3://bucket/candidate.patch'));
  });

  it('the attempt row within an expanded group carries the button role, the group row above it too', async () => {
    const proposal = makeProposal({ status: 'skipped' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');

    const attemptRow = screen.getByText('high').closest('tr')!;
    expect(attemptRow).toHaveAttribute('role', 'button');
    expect(attemptRow).toHaveAttribute('tabIndex', '0');
  });
});

describe('RemediationPanel — an actionable proposal', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeServices.mockResolvedValue([]);
  });

  it('auto-expands the group and its card with no click', async () => {
    const proposal = makeProposal({ status: 'proposed', source_resolved: true, pr_url: '', rationale: 'Adds the missing GROUP BY column.' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    // The card title and the group Nodes cell both carry the node id — no click.
    await waitFor(() => expect(screen.getAllByText('svc.schema.my_model').length).toBeGreaterThan(1));
    expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
  });

  it('an auto-expanded group row has no role or tabIndex', async () => {
    const proposal = makeProposal({ status: 'proposed', source_resolved: true, pr_url: '' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => expect(screen.getAllByText('svc.schema.my_model').length).toBeGreaterThan(0));
    const groupRow = screen.getAllByText('svc.schema.my_model')[0].closest('tr')!;
    expect(groupRow).not.toHaveAttribute('role');
    expect(groupRow).not.toHaveAttribute('tabIndex');
  });

  it.each(['', 'failed'])(
    'auto-expands and offers Create PR to an operator when pr_state is %j (retryable claim state)',
    async (pr_state) => {
      const proposal = makeProposal({ status: 'proposed', source_resolved: true, pr_url: '', pr_state, rationale: 'Adds the missing GROUP BY column.' });
      mockFetchProposals.mockResolvedValue([proposal]);

      renderPanelAsOperator();

      expect(await screen.findByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /Create PR/i })).toBeInTheDocument();
    }
  );

  it('does not auto-expand the card and offers no Create PR when pr_state is opening', async () => {
    const proposal = makeProposal({ status: 'proposed', source_resolved: true, pr_url: '', pr_state: 'opening', rationale: 'Adds the missing GROUP BY column.' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanelAsOperator();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    // Not actionable: nothing from the attempt is shown until the group opens.
    expect(screen.queryByText('Adds the missing GROUP BY column.')).toBeNull();

    expandGroupByNodes('svc.schema.my_model');
    openAttemptByConfidence();
    expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Create PR/i })).toBeNull();
  });

  it.each([
    ['skipped', { status: 'skipped', source_resolved: true, pr_url: '' }],
    ['escalated', { status: 'escalated', source_resolved: true, pr_url: '' }],
  ])('keeps a %s group collapsed until clicked', async (_label, overrides) => {
    const proposal = makeProposal({ rationale: 'Adds the missing GROUP BY column.', ...overrides });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expect(screen.queryByText('Adds the missing GROUP BY column.')).toBeNull();

    expandGroupByNodes('svc.schema.my_model');
    openAttemptByConfidence();
    expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
  });
});

describe('RemediationPanel — verification runs on an attempt', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeServices.mockResolvedValue([]);
  });

  it('lists each verification run beneath its attempt with a link to the run', async () => {
    const proposal = makeProposal({
      status: 'verifying',
      resolved_node_ids: ['s.a', 's.b'],
      verifications: [
        { service: 'core', kind: 'dbt', run_id: 'verify-rel-1-core-a1', phase: 'running', activated_at: '2026-09-02T10:01:00Z', error: '' },
        { service: 'ops', kind: 'python', run_id: 'verify-rel-1-ops-a1', phase: 'queued', activated_at: '', error: '' },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('s.a, s.b'));
    expandGroupByNodes('s.a, s.b');

    expect(screen.getByText(/core · dbt · Verifying fix… · since 2026-09-02T10:01:00Z/)).toBeInTheDocument();
    expect(screen.getAllByRole('link', { name: /open run →/ })[0]).toHaveAttribute('href', '/verifications/verify-rel-1-core-a1');
    // A queued run with no activation time omits the "since" clause.
    expect(screen.getByText(/ops · python · Queued for verification$/)).toBeInTheDocument();
    expect(screen.getByText(/ops · python · Queued for verification/).closest('.remediation-verif__run')!
      .querySelector('a')).toHaveAttribute('href', '/verifications/verify-rel-1-ops-a1');
    // Both runs sit in the attempt's verification sub-row, inside the group's block.
    expect(screen.getByText(/core · dbt · Verifying fix/).closest('.remediation-verif')).not.toBeNull();
    expect(screen.getByText(/core · dbt · Verifying fix/).closest('.remediation-attempts')).not.toBeNull();
  });

  it('falls back to the single verification_run_id link when verifications is empty', async () => {
    const proposal = makeProposal({ status: 'verifying', verification_run_id: 'verify-rel-abc-a1' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');

    const line = await screen.findByText('verification run verify-rel-abc-a1');
    const link = line.closest('.remediation-verif__run')!.querySelector('a');
    expect(link).toHaveTextContent('open run →');
    expect(link).toHaveAttribute('href', '/verifications/verify-rel-abc-a1');
  });

  it('shows why verification failed in the attempt card', async () => {
    const proposal = makeProposal({
      status: 'failed',
      verification_run_id: 'verify-rel-abc-a1',
      verify_error: 'column "revenue_total" does not exist',
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openAttemptByConfidence();

    expect(await screen.findByText(/column "revenue_total" does not exist/)).toBeInTheDocument();
  });
});

describe('RemediationPanel — pull requests split across owning services', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeServices.mockResolvedValue([]);
  });

  it('renders one labeled open PR link per pull_requests entry in the actionable card', async () => {
    const proposal = makeProposal({
      status: 'proposed',
      source_resolved: true,
      node_id: 'core.a',
      resolved_node_ids: ['core.a', 'finance.b'],
      pr_services: ['core', 'finance'],
      pull_requests: [
        { service: 'core', repo: 'org/core-repo', branch: 'b', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'open', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '' },
        { service: 'finance', repo: 'org/finance-repo', branch: 'b', pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11, pr_state: '', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '' },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    // Actionable (finance still '' → retryable), so the card is auto-shown.
    await waitFor(() => expect(screen.getAllByText('core.a, finance.b').length).toBeGreaterThan(0));

    expect(screen.getByRole('link', { name: /open PR \(core\) ↗/i })).toHaveAttribute('href', 'https://github.com/org/core-repo/pull/10');
    expect(screen.queryByRole('link', { name: /^open PR ↗$/i })).toBeNull();
  });

  it('shows a per-service state chip labeled by service in the group PR column', async () => {
    const proposal = makeProposal({
      status: 'skipped',
      pr_services: ['core', 'finance'],
      pull_requests: [
        { service: 'core', repo: '', branch: '', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'merged', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '' },
        { service: 'finance', repo: '', branch: '', pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11, pr_state: 'rejected', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '' },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    const mergedChip = await screen.findByText('merged');
    expect(mergedChip).toHaveClass('pr-chip', 'pr-chip--merged');
    expect(mergedChip.closest('.pr-chip-labeled')).toHaveTextContent('core merged');

    const rejectedChip = screen.getByText('rejected');
    expect(rejectedChip.closest('.pr-chip-labeled')).toHaveTextContent('finance rejected');

    // Both labelled chips share one wrapping row in the PR cell, so a
    // proposal split across many services stays one compact cell.
    const row = mergedChip.closest('.remediation-prs')!;
    expect(row).not.toBeNull();
    expect(rejectedChip.closest('.remediation-prs')).toBe(row);
    expect(row.querySelectorAll('.pr-chip-labeled')).toHaveLength(2);
  });

  it.each([
    ['open',    'pr-chip--open'],
    ['opening', 'pr-chip--opening'],
    ['failed',  'pr-chip--failed'],
  ])('renders a non-terminal %s pull-request state as a chip too, so the PR column reads uniformly', async (pr_state, cls) => {
    const proposal = makeProposal({ status: 'skipped', pr_state, pr_url: 'https://github.com/org/repo/pull/7', pr_number: 7 });
    mockFetchProposals.mockResolvedValue([proposal]);
    renderPanel();
    const chip = await screen.findByText(pr_state);
    expect(chip).toHaveClass('pr-chip', cls);
    // A legacy (unsplit) proposal's chip carries no service prefix.
    expect(chip.closest('.pr-chip-labeled')).toBeNull();
  });

  it('stays actionable (auto-expanded) while one service still needs a PR', async () => {
    const proposal = makeProposal({
      status: 'proposed',
      source_resolved: true,
      pr_services: ['core', 'finance'],
      pull_requests: [
        { service: 'core', repo: '', branch: '', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'merged', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '2026-07-01T00:00:00Z' },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => expect(screen.getAllByText('svc.schema.my_model').length).toBeGreaterThan(1));
  });

  it('is not actionable once every owning service has a settled PR', async () => {
    const proposal = makeProposal({
      status: 'proposed',
      source_resolved: true,
      rationale: 'Adds the missing GROUP BY column.',
      pr_services: ['core', 'finance'],
      pull_requests: [
        { service: 'core', repo: '', branch: '', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'open', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '' },
        { service: 'finance', repo: '', branch: '', pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11, pr_state: 'merged', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '2026-07-01T00:00:00Z' },
      ],
    });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    // Not auto-expanded: the rationale is not visible without opening the group.
    expect(screen.queryByText('Adds the missing GROUP BY column.')).toBeNull();
  });
});

describe('RemediationPanel — service filter', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchProposals.mockResolvedValue([makeProposal({ status: 'skipped' })]);
  });

  it('populates the Service select from fetchNodeServices and filters the fetch on change', async () => {
    mockFetchNodeServices.mockResolvedValue(['billing', 'ledger']);

    renderPanel();

    const select = await screen.findByLabelText('Service');
    await waitFor(() => expect(screen.getByRole('option', { name: 'billing' })).toBeInTheDocument());
    expect(mockFetchProposals).toHaveBeenLastCalledWith({}); // initial fetch, no filter

    fireEvent.change(select, { target: { value: 'billing' } });
    await waitFor(() => expect(mockFetchProposals).toHaveBeenLastCalledWith({ service: 'billing' }));
  });

  it('ignores a superseded (slower, earlier) proposals response so it cannot overwrite the current filter', async () => {
    mockFetchNodeServices.mockResolvedValue(['billing', 'ledger']);

    // The initial unfiltered request is held open and resolves LAST; the
    // filtered request resolves first. The stale unfiltered result must not
    // win the race and clobber the filtered list.
    let resolveInitial!: (p: ProposalDTO[]) => void;
    const initial = new Promise<ProposalDTO[]>(r => { resolveInitial = r; });
    const filtered = [makeProposal({ id: 'f1', node_id: 'billing.only', status: 'skipped' })];
    const stale = [makeProposal({ id: 's1', node_id: 'stale.everything', status: 'skipped' })];
    mockFetchProposals
      .mockReturnValueOnce(initial)                 // mount (service='')
      .mockResolvedValueOnce(filtered);             // after selecting billing

    renderPanel();
    const select = await screen.findByLabelText('Service');
    fireEvent.change(select, { target: { value: 'billing' } });

    // Filtered result lands first.
    await waitFor(() => screen.getByText('billing.only'));

    // Now the earlier unfiltered request finally resolves — it must be ignored.
    resolveInitial(stale);
    await new Promise(r => setTimeout(r, 0));
    expect(screen.queryByText('stale.everything')).toBeNull();
    expect(screen.getByText('billing.only')).toBeInTheDocument();
  });

  it('leaves the list working with only an All services option when the services fetch fails', async () => {
    mockFetchNodeServices.mockRejectedValue(new Error('nope'));

    renderPanel();

    // The proposals still render.
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    // Only the default option exists.
    const options = screen.getAllByRole('option').map(o => o.textContent);
    expect(options).toEqual(['All services']);
  });
});
