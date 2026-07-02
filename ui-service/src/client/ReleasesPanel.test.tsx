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

beforeEach(() => vi.clearAllMocks());

describe('ReleasesPanel — reject_reason', () => {
  it('shows the reject reason on a rejected row', async () => {
    mockFetch([item({ release_id: 'rel-1', status: 'rejected', reject_reason: 'compile_failed' })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    expect(await screen.findByText('compile_failed')).toBeInTheDocument();
  });

  it('does not render a reason chip for a non-rejected row', async () => {
    mockFetch([item({ release_id: 'rel-2', status: 'promoted', node_count: 5 })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    await waitFor(() => expect(screen.getByText('rel-2')).toBeInTheDocument());
    expect(screen.queryByText('compile_failed')).toBeNull();
  });
});
