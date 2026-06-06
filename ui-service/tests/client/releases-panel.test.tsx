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

afterEach(() => { vi.restoreAllMocks(); });

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

  it('navigates to the detail route when a row is clicked', async () => {
    vi.stubGlobal('fetch', mockFetch([REL], {
      current_prod_release_id: 'rel_live', node_count: 21, updated_at: '2026-06-01T09:00:00Z',
    }));
    renderPanel();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    fireEvent.click(screen.getByText('rel_abc'));
    expect(screen.getByText('DETAIL')).toBeInTheDocument();
  });

  it('navigates when a row is activated by keyboard', async () => {
    vi.stubGlobal('fetch', mockFetch([REL], {
      current_prod_release_id: 'rel_live', node_count: 21, updated_at: '2026-06-01T09:00:00Z',
    }));
    renderPanel();
    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    const row = screen.getByText('rel_abc').closest('tr')!;
    fireEvent.keyDown(row, { key: 'Enter' });
    expect(screen.getByText('DETAIL')).toBeInTheDocument();
  });
});
