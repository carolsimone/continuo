// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import ReleaseDetailPage from '../../src/client/ReleaseDetailPage';

const DETAIL = {
  release_id: 'rel_abc',
  status: 'rejected',
  transitions: [{ to: 'received', at: '2026-06-01T10:00:00Z' }],
  validation_node_ids: null,
  reject_reason: 'schema drift',
  failing_nodes: null,
  per_node_results: [
    { node_id: 'm.dim_x', status: 'failed', duration_ms: 1200, dbt_log_uri: 's3://logs/x' },
  ],
  image_tags: { 'service-1': 'sha123' },
};

function renderDetail() {
  return render(
    <MemoryRouter initialEntries={['/releases/rel_abc']}>
      <Routes>
        <Route path="/releases/:id" element={<ReleaseDetailPage />} />
      </Routes>
    </MemoryRouter>
  );
}

afterEach(() => { vi.restoreAllMocks(); });

describe('ReleaseDetailPage', () => {
  it('renders a compliant header, section-headers, nodes-table, and reject-reason strip', async () => {
    vi.stubGlobal('fetch', vi.fn(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve(DETAIL) })));
    const { container } = renderDetail();

    await waitFor(() => expect(screen.getByText('rel_abc')).toBeInTheDocument());

    expect(container.querySelector('.detail-back-link')).toBeTruthy();
    expect(container.querySelector('.detail-page-title')).toBeTruthy();
    expect(container.querySelector('.page-header .pill')).toBeTruthy();
    expect(container.querySelectorAll('.section-header').length).toBe(2);
    expect(container.querySelector('table.nodes-table')).toBeTruthy();
    expect(container.querySelector('.info-strip--error')?.textContent).toContain('schema drift');

    expect(container.querySelector('.release-table')).toBeNull();
    expect(container.querySelector('.log-view')).toBeNull();
    expect(container.querySelector('.btn--small')).toBeNull();
  });

  it('toggles the log block and always shows a link-out', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/api/releases/log')) {
        return Promise.resolve({ ok: true, text: () => Promise.resolve('LOG CONTENT') });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve(DETAIL) });
    });
    vi.stubGlobal('fetch', fetchMock);
    const { container } = renderDetail();

    await waitFor(() => expect(screen.getByText('m.dim_x')).toBeInTheDocument());

    expect(container.querySelector('a[href*="/api/releases/log"]')).toBeTruthy();
    expect(container.querySelector('.log-block')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'view' }));
    await waitFor(() =>
      expect(container.querySelector('.log-block')?.textContent).toContain('LOG CONTENT'));
  });

  it('renders a log fetch error as an info-strip, not as log text', async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes('/api/releases/log')) {
        return Promise.resolve({ ok: false, status: 500, text: () => Promise.resolve('') });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve(DETAIL) });
    });
    vi.stubGlobal('fetch', fetchMock);
    const { container } = renderDetail();
    await waitFor(() => expect(screen.getByText('m.dim_x')).toBeInTheDocument());

    fireEvent.click(screen.getByRole('button', { name: 'view' }));
    await waitFor(() => {
      const strips = Array.from(container.querySelectorAll('.info-strip--error'));
      expect(strips.some(s => s.textContent?.includes('HTTP 500'))).toBe(true);
    });
    expect(container.querySelector('.log-block')).toBeNull();
  });
});
