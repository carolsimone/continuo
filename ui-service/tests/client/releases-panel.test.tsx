// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import ReleasesPanel from '../../src/client/ReleasesPanel';

const REL = {
  release_id: 'rel_abc',
  status: 'promoted',
  created_at: '2026-06-01T10:00:00Z',
  resolved_at: '2026-06-01T10:05:00Z',
  node_count: 7,
  bootstrap: false,
};

function mockFetch(releases: any[], currentProd: any) {
  return vi.fn((url: string) => {
    if (url.startsWith('/api/releases/current-prod')) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve(currentProd) });
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve({ releases, next_cursor: '' }) });
  });
}

function renderPanel() {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Routes>
        <Route path="/" element={<ReleasesPanel />} />
        <Route path="/releases/:id" element={<div>DETAIL</div>} />
      </Routes>
    </MemoryRouter>
  );
}

afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

describe('ReleasesPanel', () => {
  it('renders info-strip banner, .form-field filter, and a nodes-table; no legacy classes', async () => {
    vi.stubGlobal('fetch', mockFetch([REL], {
      current_prod_release_id: 'rel_live', node_count: 21, updated_at: '2026-06-01T09:00:00Z',
    }));
    const { container } = renderPanel();

    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    expect(container.querySelector('.info-strip--info')).toBeTruthy();
    expect(container.querySelector('.form-field select')).toBeTruthy();
    expect(container.querySelector('table.nodes-table')).toBeTruthy();
    expect(container.querySelector('.section-header__title')?.textContent).toBe('Releases');

    expect(container.querySelector('.release-banner')).toBeNull();
    expect(container.querySelector('.release-filter')).toBeNull();
    expect(container.querySelector('.release-table')).toBeNull();
  });

  it('exposes a real link to the release detail (supports new-tab / keyboard)', async () => {
    vi.stubGlobal('fetch', mockFetch([REL], {
      current_prod_release_id: 'rel_live', node_count: 21, updated_at: '2026-06-01T09:00:00Z',
    }));
    const { container } = renderPanel();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    const link = container.querySelector('a[href="/releases/rel_abc"]');
    expect(link).toBeTruthy();
    expect(link?.textContent).toBe('rel_abc');
  });

  it('navigates when the release link is clicked', async () => {
    vi.stubGlobal('fetch', mockFetch([REL], {
      current_prod_release_id: 'rel_live', node_count: 21, updated_at: '2026-06-01T09:00:00Z',
    }));
    renderPanel();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    fireEvent.click(screen.getByText('rel_abc'));
    expect(screen.getByText('DETAIL')).toBeInTheDocument();
  });

  it('navigates when a non-link cell of the row is clicked', async () => {
    vi.stubGlobal('fetch', mockFetch([REL], {
      current_prod_release_id: 'rel_live', node_count: 21, updated_at: '2026-06-01T09:00:00Z',
    }));
    renderPanel();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    // node-count cell is not a link; clicking it exercises the row-wide handler
    fireEvent.click(screen.getByText('7'));
    expect(screen.getByText('DETAIL')).toBeInTheDocument();
  });

  it('reloads with the status filter when the select changes', async () => {
    const fetch = mockFetch([REL], {
      current_prod_release_id: 'rel_live', node_count: 21, updated_at: '2026-06-01T09:00:00Z',
    });
    vi.stubGlobal('fetch', fetch);
    renderPanel();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    fireEvent.change(screen.getByLabelText('Status'), { target: { value: 'promoted' } });
    await waitFor(() =>
      expect(fetch.mock.calls.some(c =>
        String(c[0]).includes('/api/releases?') && String(c[0]).includes('status=promoted'))).toBe(true));
  });
});
