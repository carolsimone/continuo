// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import ReleasesPanel from './ReleasesPanel';
import { ReleaseListItem } from './types';

const item = (o: Partial<ReleaseListItem> & { release_id: string; status: string }): ReleaseListItem => ({
  created_at: '2026-07-02T10:00:00Z', resolved_at: null, node_count: 0, bootstrap: false, ...o,
});

function mockFetch(releases: ReleaseListItem[]) {
  global.fetch = vi.fn((url: string) => {
    if (String(url).startsWith('/api/releases/current-prod')) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
    }
    if (String(url).startsWith('/api/releases')) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ releases, next_cursor: '' }) });
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
  }) as unknown as typeof fetch;
}

beforeEach(() => { vi.clearAllMocks(); });

describe('ReleasesPanel — reason column', () => {
  it('shows the humanized reason in the Reason column for a rejected row', async () => {
    mockFetch([item({ release_id: 'rel-1', status: 'rejected', reject_reason: 'compile_failed' })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    expect(await screen.findByText('Compilation')).toBeInTheDocument();
    // The raw token and the old error box must be gone.
    expect(screen.queryByText('compile_failed')).toBeNull();
    expect(document.querySelector('.info-strip--error')).toBeNull();
  });

  it('renders a dash in the Reason column for a promoted row', async () => {
    mockFetch([item({ release_id: 'rel-2', status: 'promoted', node_count: 5 })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    await waitFor(() => expect(screen.getByText('rel-2')).toBeInTheDocument());
    expect(screen.queryByText('Compilation')).toBeNull();
    expect(document.querySelector('.nodes-reason')).toBeNull();
    expect(document.querySelector('.nodes-dash')).not.toBeNull();
  });

  it('renders a dash for a rejected row with an empty reject_reason', async () => {
    mockFetch([item({ release_id: 'rel-3', status: 'rejected', reject_reason: '' })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    expect(await screen.findByText('rel-3')).toBeInTheDocument();
    expect(document.querySelector('.nodes-reason')).toBeNull();
    expect(document.querySelector('.nodes-dash')).not.toBeNull();
  });

  it('has a Reason column header between Status and When', async () => {
    mockFetch([item({ release_id: 'rel-4', status: 'promoted' })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    await waitFor(() => expect(screen.getByText('rel-4')).toBeInTheDocument());
    const headers = Array.from(document.querySelectorAll('thead th')).map(th => th.textContent);
    expect(headers).toEqual(['Release', 'Status', 'Reason', 'When', 'Nodes']);
  });
});
