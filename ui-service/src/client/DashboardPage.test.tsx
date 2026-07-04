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
  pr_closed_at: '',
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
  it('counts proposals with an open PR (pr_state=open)', async () => {
    const proposals = [
      makeProposal({ id: 'p1', pr_state: 'open' }),
      makeProposal({ id: 'p2', pr_state: 'open' }),
    ];
    mockFetchProposals.mockResolvedValue(proposals);

    renderDashboard();

    // The server returns only open-PR proposals; the badge is the list length.
    await waitFor(() => {
      const remediationTab = screen.getByRole('tab', { name: /remediation/i });
      expect(remediationTab).toBeInTheDocument();
      const badge = remediationTab.querySelector('.tabs__count');
      expect(badge).toBeInTheDocument();
      expect(badge?.textContent).toBe('2');
    });

    // The badge fetches only open-PR proposals.
    expect(mockFetchProposals).toHaveBeenCalledWith({ pr_state: 'open' });
  });

  it('omits the count badge on the Remediation tab when there are no open PRs', async () => {
    mockFetchProposals.mockResolvedValue([]);

    renderDashboard();

    await waitFor(() => {
      const remediationTab = screen.getByRole('tab', { name: /remediation/i });
      expect(remediationTab).toBeInTheDocument();
      // No open PRs → no pill (like the count-less Releases tab).
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
