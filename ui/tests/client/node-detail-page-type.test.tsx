// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router';
import NodeDetailPage from '../../src/client/NodeDetailPage';

const mockFetch = vi.fn();
beforeEach(() => {
  mockFetch.mockReset();
  vi.stubGlobal('fetch', mockFetch);
});

function jsonResp(body: unknown, status = 200) {
  return Promise.resolve({ ok: status >= 200 && status < 300, status, json: async () => body } as Response);
}

function withMeta(meta: unknown, metaStatus = 200) {
  mockFetch.mockImplementation((input: RequestInfo | URL) => {
    const url = typeof input === 'string' ? input : input.toString();
    if (url.includes('/meta')) return jsonResp(meta, metaStatus);
    return jsonResp({ runs: [] });
  });
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/node/svc.schema.tbl']}>
      <Routes>
        <Route path="/node/:fqn" element={<NodeDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('NodeDetailPage node-type header', () => {
  it('shows the family icon and the exact node_type chip', async () => {
    withMeta({ node_type: 'dbt-seed', test_count: 1, test_count_known: true });
    const { container } = renderPage();
    await waitFor(() => {
      expect(container.querySelector('[data-node-type-icon="dbt"]')).not.toBeNull();
    });
    expect(screen.getByText('dbt-seed')).toBeInTheDocument();
  });

  it('shows the CSV source line for a python-csv node', async () => {
    withMeta({
      node_type: 'python-csv', test_count: 0, test_count_known: true,
      source_uri: 's3://bucket/vendor/orders.csv',
    });
    const { container } = renderPage();
    await waitFor(() => {
      expect(container.querySelector('[data-node-type-icon="python-csv"]')).not.toBeNull();
    });
    expect(screen.getByText(/s3:\/\/bucket\/vendor\/orders\.csv/)).toBeInTheDocument();
  });

  it('shows no source line for a non-csv node', async () => {
    withMeta({ node_type: 'python-model', test_count: 0, test_count_known: true });
    const { container } = renderPage();
    await waitFor(() => {
      expect(container.querySelector('[data-node-type-icon="python"]')).not.toBeNull();
    });
    expect(screen.queryByText(/source:/i)).toBeNull();
  });

  it('shows no dangling source label when a csv node has no recorded URI', async () => {
    withMeta({ node_type: 'python-csv', test_count: 0, test_count_known: true, source_uri: '' });
    const { container } = renderPage();
    await waitFor(() => {
      expect(container.querySelector('[data-node-type-icon="python-csv"]')).not.toBeNull();
    });
    expect(screen.queryByText(/source:/i)).toBeNull();
  });

  it('renders the plain header when the meta fetch fails', async () => {
    withMeta({ error: 'node missing' }, 404);
    renderPage();
    expect(await screen.findByText(/svc\.schema\.tbl/)).toBeInTheDocument();
    expect(screen.queryByText(/dbt|python/)).toBeNull();
  });
});
