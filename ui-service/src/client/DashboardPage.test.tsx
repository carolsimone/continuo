// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import DashboardPage from './DashboardPage';
import { ProposalDTO } from './types';

// Mock all API calls the Dashboard makes
vi.mock('./remediation-api', () => ({
  fetchProposals: vi.fn(),
}));

import { fetchProposals } from './remediation-api';
const mockFetchProposals = fetchProposals as ReturnType<typeof vi.fn>;

// Stub global fetch for non-remediation endpoints (schedules, topology, nodes)
const mockFetch = vi.fn();
beforeEach(() => {
  vi.clearAllMocks();
  mockFetch.mockImplementation((url: string) => {
    if (url === '/api/schedules') {
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ schedules: [] }) });
    }
    if (url === '/api/topology/schedules') {
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ schedules: [] }) });
    }
    if (String(url).startsWith('/api/nodes')) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ total_count: 0 }) });
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
  });
  global.fetch = mockFetch;
});

const makeProposal = (overrides: Partial<ProposalDTO> = {}): ProposalDTO => ({
  id: 'prop-1',
  source: 's3://bucket/key.sql',
  release_id: 'rel-abc',
  node_id: 'svc.schema.my_model',
  error_signature: 'syntax error',
  attempt: 1,
  status: 'proposed',
  confidence: 'high',
  rationale: 'The fix addresses the syntax issue.',
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
  ...overrides,
});

function renderDashboard() {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <DashboardPage />
    </MemoryRouter>
  );
}

describe('DashboardPage — Remediation tab count badge', () => {
  it('counts only proposals awaiting a human (pr_state empty or failed), excluding opened PRs', async () => {
    const proposals = [
      makeProposal({ id: 'p1', pr_state: '' }),       // awaiting → counted
      makeProposal({ id: 'p2', pr_state: 'failed' }),  // retryable → counted
      makeProposal({ id: 'p3', pr_state: 'open' }),    // PR already open → NOT counted
    ];
    mockFetchProposals.mockResolvedValue(proposals);

    renderDashboard();

    // Two of the three proposals are awaiting a human, so the badge shows 2.
    await waitFor(() => {
      const remediationTab = screen.getByRole('tab', { name: /remediation/i });
      expect(remediationTab).toBeInTheDocument();
      const badge = remediationTab.querySelector('.tabs__count');
      expect(badge).toBeInTheDocument();
      expect(badge?.textContent).toBe('2');
    });

    // Ensure fetchProposals was called with 'proposed'
    expect(mockFetchProposals).toHaveBeenCalledWith('proposed');
  });

  it('omits the count badge on the Remediation tab when there are no pending proposals', async () => {
    mockFetchProposals.mockResolvedValue([]);

    renderDashboard();

    await waitFor(() => {
      const remediationTab = screen.getByRole('tab', { name: /remediation/i });
      expect(remediationTab).toBeInTheDocument();
      // No pending proposals → no pill (like the count-less Releases tab).
      expect(remediationTab.querySelector('.tabs__count')).toBeNull();
    });
  });

  it('omits the count badge on the Remediation tab when the fetch fails (best-effort, no crash)', async () => {
    mockFetchProposals.mockRejectedValue(new Error('network error'));

    renderDashboard();

    await waitFor(() => {
      const remediationTab = screen.getByRole('tab', { name: /remediation/i });
      expect(remediationTab).toBeInTheDocument();
    });

    // Fetch failed → count stays 0 → no pill.
    const remediationTab = screen.getByRole('tab', { name: /remediation/i });
    expect(remediationTab.querySelector('.tabs__count')).toBeNull();
  });
});
