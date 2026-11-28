// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import RemediationPanel from './RemediationPanel';
import { FixDTO } from './types';
import { AuthContext } from './auth/AuthContext';
import type { AuthUser } from './auth/useAuth';

vi.mock('./remediation-api', () => ({
  fetchFixes: vi.fn(),
}));

vi.mock('./node-api', () => ({
  fetchNodeProjects: vi.fn(),
}));

import { fetchFixes } from './remediation-api';
import { fetchNodeProjects } from './node-api';
const mockFetchFixes = fetchFixes as ReturnType<typeof vi.fn>;
const mockFetchNodeProjects = fetchNodeProjects as ReturnType<typeof vi.fn>;

const makeFix = (overrides: Partial<FixDTO> = {}): FixDTO => ({
  id: 'prop-1',
  source: 's3://bucket/key.sql',
  release_id: 'rel-abc',
  node_id: 'svc.schema.my_model',
  error_signature: 'syntax error',
  try: 1,
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
  release_check_id: '',
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

// Confidence is rendered only in a try row, never the group row, so it is
// an unambiguous handle for opening a single non-actionable try.
const openTryByConfidence = (confidence = 'high') => fireEvent.click(screen.getByText(confidence));

// mockFetch answers DiffView's GET against /api/logs?key=<uri> with
// body whenever the requested url matches pattern, and an empty 200
// otherwise; DiffView is the component's only caller of the global fetch.
function mockFetch(pattern: RegExp, body: string) {
  global.fetch = vi.fn((url: string) => {
    if (pattern.test(String(url))) {
      return Promise.resolve({ ok: true, text: () => Promise.resolve(body) });
    }
    return Promise.resolve({ ok: true, text: () => Promise.resolve('') });
  }) as unknown as typeof fetch;
}

describe('RemediationPanel — group list', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeProjects.mockResolvedValue([]);
  });

  it('renders one group row per (release, round), newest group first', async () => {
    const fixes = [
      makeFix({ id: 'p1', node_id: 'svc.schema.model_a', release_id: 'rel-1', status: 'skipped', created_at: '2026-06-24T10:00:00Z' }),
      makeFix({ id: 'p2', node_id: 'svc.schema.model_b', release_id: 'rel-2', status: 'skipped', created_at: '2026-06-25T10:00:00Z' }),
    ];
    mockFetchFixes.mockResolvedValue(fixes);

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

  it('collapses several tries of one (release, round) into a single group row', async () => {
    const fixes = [
      makeFix({ id: 'a1', release_id: 'rel-x', round: 1, try: 1, status: 'failed', created_at: '2026-06-24T10:00:00Z' }),
      makeFix({ id: 'a2', release_id: 'rel-x', round: 1, try: 2, status: 'skipped', created_at: '2026-06-24T11:00:00Z' }),
    ];
    mockFetchFixes.mockResolvedValue(fixes);

    renderPanel();

    // One group row, and its Tries cell reads 2.
    await waitFor(() => screen.getByText('rel-x'));
    expect(screen.getAllByText('rel-x')).toHaveLength(1);
    const groupRow = screen.getByText('rel-x').closest('tr')!;
    expect(groupRow).toHaveTextContent('2'); // tries count
  });

  it('names each try by its round and number, the hand-over row as such, and opens a skipped try on its reason', async () => {
    mockFetchFixes.mockResolvedValue([
      makeFix({ id: 't1', release_id: 'rel-x', round: 2, try: 1, status: 'failed', confidence: 'low', created_at: '2026-06-24T10:00:00Z' }),
      makeFix({
        id: 't2', release_id: 'rel-x', round: 2, try: 2, status: 'skipped', confidence: '',
        rationale: 'maintenance mode is on', created_at: '2026-06-24T11:00:00Z',
      }),
      makeFix({ id: 'h', release_id: 'rel-x', round: 2, try: 0, status: 'handed_over', confidence: '', created_at: '2026-06-24T11:00:00Z' }),
    ]);
    renderPanel();
    await waitFor(() => screen.getByText('rel-x'));
    expect(screen.getByText('rel-x').closest('tr')).toHaveTextContent('2'); // the hand-over row is no try
    expandGroupByNodes('svc.schema.my_model');

    expect(screen.getByRole('columnheader', { name: 'Try' })).toBeInTheDocument();
    expect(screen.getByText('round 2, try 1')).toBeInTheDocument();
    expect(screen.getByText('round 2, try 2')).toBeInTheDocument();
    expect(screen.getByText('round 2, hand-over')).toBeInTheDocument();
    fireEvent.click(screen.getByText('round 2, try 2'));
    expect(screen.getByText('maintenance mode is on')).toBeInTheDocument();
    expect(screen.queryByText(/#\d/)).toBeNull();
  });

  it('shows the union of projects and nodes for a group', async () => {
    const fix = makeFix({
      status: 'skipped',
      node_id: 'core_schema.a',
      resolved_node_ids: ['core_schema.a', 'finance_schema.b'],
      projects: ['finance', 'core'],
    });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();

    await waitFor(() => screen.getByText('core_schema.a, finance_schema.b'));
    // Projects union is sorted.
    expect(screen.getByText('core, finance')).toBeInTheDocument();
  });

  it('reads the Projects column from the projects the fix carries, never from its node ids', async () => {
    // A remediation node id is "<schema>.<table>" — it names no project —
    // so the column shows the projects agent-remediation recorded for the
    // try (the same set the Project filter matches on), and a schema
    // never leaks into it.
    const fix = makeFix({
      status: 'skipped',
      node_id: 'e2e_schema.table_a',
      resolved_node_ids: ['e2e_schema.table_a', 'e2e_schema.table_b'],
      projects: ['service-1'],
    });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();

    await waitFor(() => screen.getByText('e2e_schema.table_a, e2e_schema.table_b'));
    expect(screen.getByText('service-1')).toBeInTheDocument();
    expect(screen.queryByText(/e2e_schema(,|$)/)).toBeNull();
  });

  it('shows the neutral info-strip when there are no fixes', async () => {
    mockFetchFixes.mockResolvedValue([]);
    renderPanel();
    await waitFor(() => expect(screen.getByText('No fixes yet.')).toBeInTheDocument());
  });

  it('renders the merged chip in the group PR column without expanding', async () => {
    const fix = makeFix({ status: 'skipped', pr_state: 'merged', pr_url: 'https://github.com/org/repo/pull/7', pr_number: 7 });
    mockFetchFixes.mockResolvedValue([fix]);
    renderPanel();
    const chip = await screen.findByText('merged');
    expect(chip).toHaveClass('pr-chip', 'pr-chip--merged');
  });

  it('renders the rejected chip in the group PR column without expanding', async () => {
    const fix = makeFix({ status: 'skipped', pr_state: 'rejected', pr_url: 'https://github.com/org/repo/pull/7', pr_number: 7 });
    mockFetchFixes.mockResolvedValue([fix]);
    renderPanel();
    const chip = await screen.findByText('rejected');
    expect(chip).toHaveClass('pr-chip', 'pr-chip--rejected');
  });

  it('reads a verifying group as an in-progress pill in the Latest status column, without expanding', async () => {
    mockFetchFixes.mockResolvedValue([makeFix({ status: 'verifying' })]);
    renderPanel();
    const pill = await screen.findByText('verifying');
    expect(pill).toHaveClass('pill-sm', 'pill-sm--running');
    expect(pill).toHaveAttribute('aria-busy', 'true');
  });
});

describe('RemediationPanel — status pills', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeProjects.mockResolvedValue([]);
  });

  it.each([
    ['proposed',   'pill-sm--succeeded'],
    ['failed',     'pill-sm--failed'],
    ['handed_over', 'pill-sm--failed'],
    ['skipped',    'pill-sm--skipped'],
    ['generating', 'pill-sm--pending'],
  ])('renders the %s status as a %s pill in the Latest status column', async (status, cls) => {
    // source_resolved=false keeps a 'proposed' try from auto-expanding,
    // so the only status text on screen is the group row's.
    mockFetchFixes.mockResolvedValue([makeFix({ status, source_resolved: false })]);
    renderPanel();
    const pill = await screen.findByText(status.replace(/_/g, ' '));
    expect(pill).toHaveClass('pill-sm', cls);
  });

  it('renders the try row status as the same pill once the group is open', async () => {
    mockFetchFixes.mockResolvedValue([makeFix({ status: 'failed' })]);
    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');

    const pills = screen.getAllByText('failed');
    expect(pills).toHaveLength(2); // group row + try row
    for (const pill of pills) expect(pill).toHaveClass('pill-sm', 'pill-sm--failed');
  });
});

describe('RemediationPanel — tries are nested inside their group', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeProjects.mockResolvedValue([]);
  });

  it('renders the try rows and their column labels inside the group\'s contained block, not as a peer table', async () => {
    mockFetchFixes.mockResolvedValue([makeFix({ status: 'skipped' })]);
    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');

    // The try row and the inner column labels both live inside the
    // .remediation-tries block hosted by the group's expansion row…
    const block = screen.getByText('high').closest('.remediation-tries');
    expect(block).not.toBeNull();
    expect(screen.getByText('Confidence').closest('.remediation-tries')).toBe(block);
    expect(block!.closest('tr')).toHaveClass('remediation-group__body');
    // …while the outer header is not inside any such block.
    expect(screen.getByText('Latest status').closest('.remediation-tries')).toBeNull();
  });

  it('reads the source column as resolved / unresolved rather than yes / no', async () => {
    mockFetchFixes.mockResolvedValue([
      makeFix({ id: 'a', status: 'skipped', source_resolved: true, try: 1 }),
      makeFix({ id: 'b', status: 'skipped', source_resolved: false, try: 2, created_at: '2026-06-24T11:00:00Z' }),
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

describe('RemediationPanel — expanding a group and its tries', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeProjects.mockResolvedValue([]);
  });

  it('reveals try rows on group click, then the rationale on try click', async () => {
    const fix = makeFix({ status: 'skipped', rationale: 'Fixes the JOIN clause that was missing a condition.' });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    // Nothing from the try is visible until the group is opened.
    expect(screen.queryByText('high')).toBeNull();
    expect(screen.queryByText('Fixes the JOIN clause that was missing a condition.')).toBeNull();

    expandGroupByNodes('svc.schema.my_model');
    // Try row now visible (confidence cell).
    expect(screen.getByText('high')).toBeInTheDocument();
    // Card still requires a try click.
    expect(screen.queryByText('Fixes the JOIN clause that was missing a condition.')).toBeNull();

    openTryByConfidence();
    expect(screen.getByText('Fixes the JOIN clause that was missing a condition.')).toBeInTheDocument();
  });

  it('opens a collapsed group on Enter and closes it on Space', async () => {
    const fix = makeFix({ status: 'skipped' });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    const groupRow = screen.getByText('svc.schema.my_model').closest('tr')!;
    expect(groupRow).toHaveAttribute('role', 'button');
    expect(groupRow).toHaveAttribute('tabIndex', '0');
    expect(groupRow).toHaveAttribute('aria-expanded', 'false');

    fireEvent.keyDown(groupRow, { key: 'Enter' });
    expect(groupRow).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByText('high')).toBeInTheDocument(); // try row shown

    fireEvent.keyDown(groupRow, { key: ' ' });
    expect(groupRow).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByText('high')).toBeNull();
  });

  it('shows the source warning in the try card', async () => {
    const fix = makeFix({ status: 'skipped', source_resolved: false });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openTryByConfidence();

    expect(screen.getByText(/No real-source fix — a PR cannot be opened for this fix/)).toBeInTheDocument();
  });

  it('shows the diff view/hide toggle in the try card, open by default', async () => {
    mockFetch(/\/api\/logs/, '');
    const fix = makeFix({ status: 'skipped', diff_uri: 's3://bucket/my.patch' });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openTryByConfidence();

    expect(screen.getByRole('link', { name: /open full ↗/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^hide$/i })).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /^hide$/i }));
    expect(screen.getByRole('button', { name: /^view$/i })).toBeInTheDocument();
  });

  it('renders one labelled diff view per edit when the fix carries edits', async () => {
    mockFetch(/\/api\/logs/, '');
    const fix = makeFix({
      status: 'skipped',
      diff_uri: 's3://bucket/legacy.patch',
      edits: [
        { path: 'contracts/a.yml', content_uri: 's3://bucket/a.yml', diff_uri: 's3://bucket/a.diff' },
        { path: 'scripts/a.py', content_uri: 's3://bucket/a.py', diff_uri: 's3://bucket/py.diff' },
      ],
    });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openTryByConfidence();

    expect(screen.getByText('contracts/a.yml')).toBeInTheDocument();
    expect(screen.getByText('scripts/a.py')).toBeInTheDocument();
    // The first edit's diff opens by default; the rest stay one click away.
    expect(screen.getByRole('button', { name: /^hide$/i })).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: /^view$/i })).toHaveLength(1);

    const links = screen.getAllByRole('link', { name: /open full ↗/i });
    expect(links).toHaveLength(2);
    expect(links[0].getAttribute('href')).toContain(encodeURIComponent('s3://bucket/a.diff'));
    expect(links[1].getAttribute('href')).toContain(encodeURIComponent('s3://bucket/py.diff'));
    expect(links.some((l) => l.getAttribute('href')?.includes(encodeURIComponent('s3://bucket/legacy.patch')))).toBe(false);
  });

  it('falls back to the single unlabelled diff view when the fix carries no edits', async () => {
    mockFetch(/\/api\/logs/, '');
    const fix = makeFix({ status: 'skipped', diff_uri: 's3://bucket/candidate.patch', edits: [] });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openTryByConfidence();

    const links = screen.getAllByRole('link', { name: /open full ↗/i });
    expect(links).toHaveLength(1);
    expect(links[0].getAttribute('href')).toContain(encodeURIComponent('s3://bucket/candidate.patch'));
  });

  it('renders the first edit diff open and above the rationale', async () => {
    mockFetch(/\/api\/logs/, '--- a\n+++ b\n-select amount\n+select amount_eur');
    const fix = makeFix({
      status: 'skipped',
      rationale: 'Failed: `svc.schema.my_model`\nModel\'s note: read amount_eur',
      edits: [
        { path: 'models/a.sql', content_uri: 's3://bucket/a.sql', diff_uri: 's3://bucket/a.diff' },
        { path: 'models/b.sql', content_uri: 's3://bucket/b.sql', diff_uri: 's3://bucket/b.diff' },
      ],
    });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openTryByConfidence();

    const firstDiff = await screen.findByText(/\+select amount_eur/);
    const rationale = screen.getByText(/Model's note: read amount_eur/);
    expect(firstDiff.compareDocumentPosition(rationale) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.getByRole('button', { name: /^hide$/i })).toBeInTheDocument();   // first edit open
    expect(screen.getAllByRole('button', { name: /^view$/i })).toHaveLength(1);    // second edit collapsed
  });

  it('keeps the rationale line breaks', async () => {
    const fix = makeFix({ status: 'skipped', rationale: 'Failed: `x`\nEdited: `p`' });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openTryByConfidence();

    const el = screen.getByText(/Failed: `x`/);
    expect(el.tagName).toBe('PRE');
    expect(el.textContent).toContain('\nEdited: `p`');
  });

  it('the try row within an expanded group carries the button role, the group row above it too', async () => {
    const fix = makeFix({ status: 'skipped' });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');

    const tryRow = screen.getByText('high').closest('tr')!;
    expect(tryRow).toHaveAttribute('role', 'button');
    expect(tryRow).toHaveAttribute('tabIndex', '0');
  });
});

describe('RemediationPanel — an actionable fix', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeProjects.mockResolvedValue([]);
  });

  it('auto-expands the group and its card with no click', async () => {
    const fix = makeFix({ status: 'proposed', source_resolved: true, pr_url: '', rationale: 'Adds the missing GROUP BY column.' });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();

    // The card title and the group Nodes cell both carry the node id — no click.
    await waitFor(() => expect(screen.getAllByText('svc.schema.my_model').length).toBeGreaterThan(1));
    expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
  });

  it('an auto-expanded group row has no role or tabIndex', async () => {
    const fix = makeFix({ status: 'proposed', source_resolved: true, pr_url: '' });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => expect(screen.getAllByText('svc.schema.my_model').length).toBeGreaterThan(0));
    const groupRow = screen.getAllByText('svc.schema.my_model')[0].closest('tr')!;
    expect(groupRow).not.toHaveAttribute('role');
    expect(groupRow).not.toHaveAttribute('tabIndex');
  });

  it.each(['', 'failed'])(
    'auto-expands and offers Create PR to an operator when pr_state is %j (retryable claim state)',
    async (pr_state) => {
      const fix = makeFix({ status: 'proposed', source_resolved: true, pr_url: '', pr_state, rationale: 'Adds the missing GROUP BY column.' });
      mockFetchFixes.mockResolvedValue([fix]);

      renderPanelAsOperator();

      expect(await screen.findByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /Create PR/i })).toBeInTheDocument();
    }
  );

  it('shows the no-repository banner instead of Create PR when the fix records no repo', async () => {
    const fix = makeFix({ status: 'proposed', source_resolved: true, pr_url: '', repo: '', rationale: 'Adds the missing GROUP BY column.' });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanelAsOperator();

    expect(await screen.findByText(/Adds the missing GROUP BY column/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Create PR/i })).toBeNull();
    expect(screen.getByText(/No repository recorded for this fix; a pull request cannot be opened\./)).toBeInTheDocument();
  });

  it('does not auto-expand the card and offers no Create PR when pr_state is opening', async () => {
    const fix = makeFix({ status: 'proposed', source_resolved: true, pr_url: '', pr_state: 'opening', rationale: 'Adds the missing GROUP BY column.' });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanelAsOperator();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    // Not actionable: nothing from the try is shown until the group opens.
    expect(screen.queryByText('Adds the missing GROUP BY column.')).toBeNull();

    expandGroupByNodes('svc.schema.my_model');
    openTryByConfidence();
    expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Create PR/i })).toBeNull();
  });

  it.each([
    ['skipped', { status: 'skipped', source_resolved: true, pr_url: '' }],
    ['handed_over', { status: 'handed_over', source_resolved: true, pr_url: '' }],
  ])('keeps a %s group collapsed until clicked', async (_label, overrides) => {
    const fix = makeFix({ rationale: 'Adds the missing GROUP BY column.', ...overrides });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expect(screen.queryByText('Adds the missing GROUP BY column.')).toBeNull();

    expandGroupByNodes('svc.schema.my_model');
    openTryByConfidence();
    expect(screen.getByText('Adds the missing GROUP BY column.')).toBeInTheDocument();
  });
});

describe('RemediationPanel — release checks on a try', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeProjects.mockResolvedValue([]);
  });

  it('lists each release check beneath its try with a link to its page', async () => {
    const fix = makeFix({
      status: 'verifying',
      resolved_node_ids: ['s.a', 's.b'],
      release_checks: [
        { project: 'core', kind: 'dbt', check_id: 'verify-rel-1-core-a1', phase: 'running', activated_at: '2026-09-02T10:01:00Z', error: '' },
        { project: 'ops', kind: 'python', check_id: 'verify-rel-1-ops-a1', phase: 'queued', activated_at: '', error: '' },
      ],
    });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('s.a, s.b'));
    expandGroupByNodes('s.a, s.b');

    expect(screen.getByText(/core · dbt · Checking the fix… · since 2026-09-02T10:01:00Z/)).toBeInTheDocument();
    expect(screen.getAllByRole('link', { name: /open release check →/ })[0]).toHaveAttribute('href', '/release-checks/verify-rel-1-core-a1');
    // A queued release check with no activation time omits the "since" clause.
    expect(screen.getByText(/ops · python · Queued for its release check$/)).toBeInTheDocument();
    expect(screen.getByText(/ops · python · Queued for its release check/).closest('.remediation-subrow__line')!
      .querySelector('a')).toHaveAttribute('href', '/release-checks/verify-rel-1-ops-a1');
    // Both release checks sit in the try's release-check sub-row, inside the group's block.
    expect(screen.getByText(/core · dbt · Checking the fix/).closest('.remediation-subrow')).not.toBeNull();
    expect(screen.getByText(/core · dbt · Checking the fix/).closest('.remediation-tries')).not.toBeNull();
  });

  it('falls back to the single release_check_id link when release_checks is empty', async () => {
    const fix = makeFix({ status: 'verifying', release_check_id: 'verify-rel-abc-a1' });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');

    const line = await screen.findByText('release check verify-rel-abc-a1');
    const link = line.closest('.remediation-subrow__line')!.querySelector('a');
    expect(link).toHaveTextContent('open release check →');
    expect(link).toHaveAttribute('href', '/release-checks/verify-rel-abc-a1');
  });

  it('shows why the release check failed in the try card', async () => {
    const fix = makeFix({
      status: 'failed',
      release_check_id: 'verify-rel-abc-a1',
      verify_error: 'column "revenue_total" does not exist',
    });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    expandGroupByNodes('svc.schema.my_model');
    openTryByConfidence();

    expect(await screen.findByText(/column "revenue_total" does not exist/)).toHaveTextContent('Release check failed: column "revenue_total" does not exist');
  });
});

describe('RemediationPanel — pull requests split across owning projects', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchNodeProjects.mockResolvedValue([]);
  });

  it('renders one labeled open PR link per pull_requests entry in the actionable card', async () => {
    const fix = makeFix({
      status: 'proposed',
      source_resolved: true,
      node_id: 'core.a',
      resolved_node_ids: ['core.a', 'finance.b'],
      pr_projects: ['core', 'finance'],
      pull_requests: [
        { project: 'core', repo: 'org/core-repo', branch: 'b', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'open', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '' },
        { project: 'finance', repo: 'org/finance-repo', branch: 'b', pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11, pr_state: '', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '' },
      ],
    });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    // Actionable (finance still '' → retryable), so the card is auto-shown.
    await waitFor(() => expect(screen.getAllByText('core.a, finance.b').length).toBeGreaterThan(0));

    expect(screen.getByRole('link', { name: /open PR \(core\) ↗/i })).toHaveAttribute('href', 'https://github.com/org/core-repo/pull/10');
    expect(screen.queryByRole('link', { name: /^open PR ↗$/i })).toBeNull();
  });

  it('shows a per-project state chip labeled by project in the group PR column', async () => {
    const fix = makeFix({
      status: 'skipped',
      pr_projects: ['core', 'finance'],
      pull_requests: [
        { project: 'core', repo: '', branch: '', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'merged', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '' },
        { project: 'finance', repo: '', branch: '', pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11, pr_state: 'rejected', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '' },
      ],
    });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();

    const mergedChip = await screen.findByText('merged');
    expect(mergedChip).toHaveClass('pr-chip', 'pr-chip--merged');
    expect(mergedChip.closest('.pr-chip-labeled')).toHaveTextContent('core merged');

    const rejectedChip = screen.getByText('rejected');
    expect(rejectedChip.closest('.pr-chip-labeled')).toHaveTextContent('finance rejected');

    // Both labelled chips share one wrapping row in the PR cell, so a
    // fix split across many projects stays one compact cell.
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
    const fix = makeFix({ status: 'skipped', pr_state, pr_url: 'https://github.com/org/repo/pull/7', pr_number: 7 });
    mockFetchFixes.mockResolvedValue([fix]);
    renderPanel();
    const chip = await screen.findByText(pr_state);
    expect(chip).toHaveClass('pr-chip', cls);
    // A legacy (unsplit) fix's chip carries no project prefix.
    expect(chip.closest('.pr-chip-labeled')).toBeNull();
  });

  it('stays actionable (auto-expanded) while one project still needs a PR', async () => {
    const fix = makeFix({
      status: 'proposed',
      source_resolved: true,
      pr_projects: ['core', 'finance'],
      pull_requests: [
        { project: 'core', repo: '', branch: '', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'merged', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '2026-07-01T00:00:00Z' },
      ],
    });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => expect(screen.getAllByText('svc.schema.my_model').length).toBeGreaterThan(1));
  });

  it('is not actionable once every owning project has a settled PR', async () => {
    const fix = makeFix({
      status: 'proposed',
      source_resolved: true,
      rationale: 'Adds the missing GROUP BY column.',
      pr_projects: ['core', 'finance'],
      pull_requests: [
        { project: 'core', repo: '', branch: '', pr_url: 'https://github.com/org/core-repo/pull/10', pr_number: 10, pr_state: 'open', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '' },
        { project: 'finance', repo: '', branch: '', pr_url: 'https://github.com/org/finance-repo/pull/11', pr_number: 11, pr_state: 'merged', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '2026-07-01T00:00:00Z' },
      ],
    });
    mockFetchFixes.mockResolvedValue([fix]);

    renderPanel();
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    // Not auto-expanded: the rationale is not visible without opening the group.
    expect(screen.queryByText('Adds the missing GROUP BY column.')).toBeNull();
  });
});

describe('RemediationPanel — project filter', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchFixes.mockResolvedValue([makeFix({ status: 'skipped' })]);
  });

  it('populates the Project select from fetchNodeProjects and filters the fetch on change', async () => {
    mockFetchNodeProjects.mockResolvedValue(['billing', 'ledger']);

    renderPanel();

    const select = await screen.findByLabelText('Project');
    await waitFor(() => expect(screen.getByRole('option', { name: 'billing' })).toBeInTheDocument());
    expect(mockFetchFixes).toHaveBeenLastCalledWith({}); // initial fetch, no filter

    fireEvent.change(select, { target: { value: 'billing' } });
    await waitFor(() => expect(mockFetchFixes).toHaveBeenLastCalledWith({ project: 'billing' }));
  });

  it('renders the Project filter as a .form-field above the Fixes section header', async () => {
    mockFetchNodeProjects.mockResolvedValue(['billing']);

    const { container } = renderPanel();

    const select = await screen.findByLabelText('Project');
    const field = select.closest('.form-field');
    expect(field).not.toBeNull();
    expect(field!.querySelector('label')).toHaveTextContent('Project');

    // The filter drives the list, so it sits above the list's header — never
    // inside the header's sub-line, which carries metadata only.
    const header = container.querySelector('.section-header');
    expect(header).not.toBeNull();
    expect(header!.contains(select)).toBe(false);
    expect(field!.compareDocumentPosition(header!) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it('ignores a superseded (slower, earlier) fixes response so it cannot overwrite the current filter', async () => {
    mockFetchNodeProjects.mockResolvedValue(['billing', 'ledger']);

    // The initial unfiltered request is held open and resolves LAST; the
    // filtered request resolves first. The stale unfiltered result must not
    // win the race and clobber the filtered list.
    let resolveInitial!: (p: FixDTO[]) => void;
    const initial = new Promise<FixDTO[]>(r => { resolveInitial = r; });
    const filtered = [makeFix({ id: 'f1', node_id: 'billing.only', status: 'skipped' })];
    const stale = [makeFix({ id: 's1', node_id: 'stale.everything', status: 'skipped' })];
    mockFetchFixes
      .mockReturnValueOnce(initial)                 // mount (project='')
      .mockResolvedValueOnce(filtered);             // after selecting billing

    renderPanel();
    const select = await screen.findByLabelText('Project');
    fireEvent.change(select, { target: { value: 'billing' } });

    // Filtered result lands first.
    await waitFor(() => screen.getByText('billing.only'));

    // Now the earlier unfiltered request finally resolves — it must be ignored.
    resolveInitial(stale);
    await new Promise(r => setTimeout(r, 0));
    expect(screen.queryByText('stale.everything')).toBeNull();
    expect(screen.getByText('billing.only')).toBeInTheDocument();
  });

  it('leaves the list working with only an All projects option when the projects fetch fails', async () => {
    mockFetchNodeProjects.mockRejectedValue(new Error('nope'));

    renderPanel();

    // The fixes still render.
    await waitFor(() => screen.getByText('svc.schema.my_model'));
    // Only the default option exists.
    const options = screen.getAllByRole('option').map(o => o.textContent);
    expect(options).toEqual(['All projects']);
  });
});
