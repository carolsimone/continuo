// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, findByText } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import CreatePrModal from './CreatePrModal';
import RemediationPanel from './RemediationPanel';
import { ProposalDTO } from './types';

vi.mock('./remediation-api', () => ({
  fetchProposals: vi.fn(),
  createPullRequest: vi.fn(),
}));

vi.mock('./auth/AuthContext', () => ({
  useCurrentUser: vi.fn(),
}));

import { fetchProposals, createPullRequest } from './remediation-api';
import { useCurrentUser } from './auth/AuthContext';

const mockFetchProposals = fetchProposals as ReturnType<typeof vi.fn>;
const mockCreatePullRequest = createPullRequest as ReturnType<typeof vi.fn>;
const mockUseCurrentUser = useCurrentUser as ReturnType<typeof vi.fn>;

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

function renderModal(proposalOverrides: Partial<ProposalDTO> = {}) {
  const proposal = makeProposal(proposalOverrides);
  const onClose = vi.fn();
  const onCreated = vi.fn();
  const { container } = render(
    <MemoryRouter>
      <CreatePrModal proposal={proposal} onClose={onClose} onCreated={onCreated} />
    </MemoryRouter>
  );
  return { container, onClose, onCreated };
}

function renderPanel() {
  return render(
    <MemoryRouter>
      <RemediationPanel />
    </MemoryRouter>
  );
}

describe('CreatePrModal', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders the modal with proposal context and confirms Create PR button', () => {
    renderModal();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(screen.getByText(/svc\.schema\.my_model/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Create PR/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Cancel/i })).toBeInTheDocument();
  });

  it('closes on Cancel button click', () => {
    const { onClose } = renderModal();
    fireEvent.click(screen.getByRole('button', { name: /Cancel/i }));
    expect(onClose).toHaveBeenCalled();
  });

  it('closes on overlay click', () => {
    const { onClose } = renderModal();
    const overlay = document.querySelector('.dialog-overlay') as HTMLElement;
    fireEvent.click(overlay);
    expect(onClose).toHaveBeenCalled();
  });

  it('closes on Escape key', () => {
    const { onClose } = renderModal();
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(onClose).toHaveBeenCalled();
  });

  it('confirming calls createPullRequest, shows Creating… then Created, then PR link', async () => {
    mockCreatePullRequest.mockResolvedValue({ pr_url: 'https://github.com/org/repo/pull/99', pr_number: 99 });
    const { onClose, onCreated } = renderModal();

    const createBtn = screen.getByRole('button', { name: /Create PR/i });
    fireEvent.click(createBtn);

    // Should show loading state
    expect(screen.getByRole('button', { name: /Creating…/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Creating…/i })).toBeDisabled();

    // Wait for success
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /Created/i })).toBeInTheDocument();
    });

    expect(mockCreatePullRequest).toHaveBeenCalledWith('prop-1');
    expect(onCreated).toHaveBeenCalledWith('https://github.com/org/repo/pull/99');
    expect(onClose).toHaveBeenCalled();
  });

  it('treats 409 with pr_url as success — calls onCreated, no error strip', async () => {
    mockCreatePullRequest.mockRejectedValue({ status: 409, pr_url: 'https://github.com/org/repo/pull/77', message: 'already exists' });
    const { onClose, onCreated } = renderModal();

    fireEvent.click(screen.getByRole('button', { name: /Create PR/i }));

    await waitFor(() => {
      expect(onCreated).toHaveBeenCalledWith('https://github.com/org/repo/pull/77');
    });
    expect(onClose).toHaveBeenCalled();
    expect(document.querySelector('.info-strip--error')).toBeNull();
  });

  it('renders .info-strip--error on non-409 failure', async () => {
    mockCreatePullRequest.mockRejectedValue({ status: 502, message: 'upstream timeout' });
    renderModal();

    fireEvent.click(screen.getByRole('button', { name: /Create PR/i }));

    await waitFor(() => {
      expect(document.querySelector('.info-strip--error')).toBeInTheDocument();
      expect(screen.getByText(/upstream timeout/i)).toBeInTheDocument();
    });

    // Modal should remain open, no PR link produced
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });
});

describe('RemediationPanel — Create PR trigger gating', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('shows Create PR trigger for operator when source_resolved=true and no pr_url', async () => {
    mockUseCurrentUser.mockReturnValue({ userId: 'u1', email: 'op@x.com', name: 'Op', role: 'operator' });
    const proposal = makeProposal({ source_resolved: true, pr_url: '' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    expect(screen.getByRole('button', { name: /Create PR/i })).toBeInTheDocument();
  });

  it('does NOT show Create PR trigger for viewer', async () => {
    mockUseCurrentUser.mockReturnValue({ userId: 'u2', email: 'v@x.com', name: 'Viewer', role: 'viewer' });
    const proposal = makeProposal({ source_resolved: true, pr_url: '' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    expect(screen.queryByRole('button', { name: /Create PR/i })).toBeNull();
  });

  it('does NOT show Create PR trigger when source_resolved=false', async () => {
    mockUseCurrentUser.mockReturnValue({ userId: 'u1', email: 'op@x.com', name: 'Op', role: 'operator' });
    const proposal = makeProposal({ source_resolved: false, pr_url: '' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    expect(screen.queryByRole('button', { name: /Create PR/i })).toBeNull();
  });

  it('does NOT show Create PR trigger when pr_url is already set', async () => {
    mockUseCurrentUser.mockReturnValue({ userId: 'u1', email: 'op@x.com', name: 'Op', role: 'operator' });
    const proposal = makeProposal({ source_resolved: true, pr_url: 'https://github.com/org/repo/pull/5' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    expect(screen.queryByRole('button', { name: /Create PR/i })).toBeNull();
  });

  it('after successful PR creation, shows open PR link in the detail card', async () => {
    mockUseCurrentUser.mockReturnValue({ userId: 'u1', email: 'op@x.com', name: 'Op', role: 'operator' });
    mockCreatePullRequest.mockResolvedValue({ pr_url: 'https://github.com/org/repo/pull/55', pr_number: 55 });
    const proposal = makeProposal({ source_resolved: true, pr_url: '' });
    mockFetchProposals.mockResolvedValue([proposal]);

    renderPanel();

    await waitFor(() => screen.getByText('svc.schema.my_model'));
    fireEvent.click(screen.getByText('svc.schema.my_model'));

    // Open the modal
    fireEvent.click(screen.getByRole('button', { name: /Create PR/i }));

    // Confirm in modal
    const modalCreateBtn = screen.getAllByRole('button', { name: /Create PR/i }).find(
      btn => btn.closest('[role="dialog"]')
    );
    expect(modalCreateBtn).toBeInTheDocument();
    fireEvent.click(modalCreateBtn!);

    // Wait for PR link to appear in the detail card
    await waitFor(() => {
      const link = screen.getByRole('link', { name: /open PR ↗/i });
      expect(link).toHaveAttribute('href', 'https://github.com/org/repo/pull/55');
    });
  });
});
