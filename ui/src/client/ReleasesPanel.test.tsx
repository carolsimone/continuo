// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import ReleasesPanel from './ReleasesPanel';
import { ReleaseListItem } from './types';

const item = (o: Partial<ReleaseListItem> & { release_id: string; status: string }): ReleaseListItem => ({
  created_at: '2026-07-02T10:00:00Z', resolved_at: null, node_count: 0, bootstrap: false, shadow: false, ...o,
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
    expect(document.querySelector('.nodes-reason')).not.toBeNull();
    // The Reason cell (index 3: Release, Author, Status, Reason) is the reason,
    // not a dash. The Author cell may still be a dash when no author resolved.
    const reasonCell = document.querySelectorAll('tbody tr td')[3];
    expect(reasonCell?.querySelector('.nodes-dash')).toBeNull();
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
    expect(headers).toEqual(['Release', 'Author', 'Status', 'Reason', 'When', 'Nodes']);
  });
});

describe('ReleasesPanel — author column', () => {
  it('renders @login linked to the GitHub profile, with an avatar', async () => {
    mockFetch([item({
      release_id: 'rel-a', status: 'promoted',
      author: { login: 'octocat', avatar_url: 'https://avatars/octocat.png', html_url: 'https://github.com/octocat' },
    })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    const link = await screen.findByText('@octocat');
    expect(link.closest('a')?.getAttribute('href')).toBe('https://github.com/octocat');
    expect(document.querySelector('.release-author__avatar')?.getAttribute('src'))
      .toBe('https://avatars/octocat.png');
  });

  it('renders the plain git author name (no link) when the account is unlinked', async () => {
    mockFetch([item({ release_id: 'rel-b', status: 'promoted', author: { name: 'Grace Hopper' } })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    const name = await screen.findByText('Grace Hopper');
    expect(name.closest('a')).toBeNull();
  });

  it('renders a dash when the release has no author', async () => {
    mockFetch([item({ release_id: 'rel-c', status: 'promoted' })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    await screen.findByText('rel-c');
    // Reason (promoted) and Author are both dashes; the author cell is the second.
    const row = document.querySelector('tbody tr');
    const dashes = row?.querySelectorAll('.nodes-dash');
    expect(dashes && dashes.length).toBeGreaterThanOrEqual(2);
  });
});

describe('ReleasesPanel — shadow verification label', () => {
  it('renders a verification pill next to the release id for a shadow release', async () => {
    mockFetch([item({ release_id: 'rel-5', status: 'validated', shadow: true })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    await screen.findByText('rel-5');
    expect(screen.getByText('verification')).toBeInTheDocument();
  });

  it('does not render a verification pill for a non-shadow release', async () => {
    mockFetch([item({ release_id: 'rel-6', status: 'promoted', shadow: false })]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    await screen.findByText('rel-6');
    expect(screen.queryByText('verification')).toBeNull();
  });

  it('includes validated in the status filter, so shadow verification runs can be filtered to', async () => {
    mockFetch([]);
    render(<MemoryRouter><ReleasesPanel /></MemoryRouter>);
    await waitFor(() => expect(document.querySelector('#release-status-filter')).toBeInTheDocument());
    const options = Array.from(document.querySelectorAll('#release-status-filter option'))
      .map(o => (o as HTMLOptionElement).value);
    expect(options).toContain('validated');
  });
});
