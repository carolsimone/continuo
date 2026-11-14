// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router';
import DetailPage from '../../src/client/DetailPage';

const RUN_ID = 'run-1';
const SCHED = 'hourly-events';
const CSV_NODE_ID = 'svc1.public.vendor_feed';

function mockFetchSequence(routes: Record<string, () => Promise<unknown>>) {
  return vi.fn(async (input: RequestInfo | URL) => {
    const url = typeof input === 'string' ? input : input.toString();
    for (const [pattern, handler] of Object.entries(routes)) {
      if (url.includes(pattern)) {
        const body = await handler();
        return { ok: true, status: 200, json: async () => body } as Response;
      }
    }
    return { ok: true, status: 200, json: async () => ({}) } as Response;
  });
}

// The per-node focus legend lives on the topology-catalog view (latest mode),
// which renders the schedule's node-level dependency graph. Selecting a node
// there reveals a legend keyed off the node's family, plus an "Open node
// detail" link. The Run view is status-first and carries no such graph.
// Ordering matters: the mock matches the first pattern the URL contains, so the
// specific `/api/schedules/:name/...` routes must precede the broad
// `/api/schedules` list route.
const routes = {
  [`/api/schedules/${SCHED}/graph`]: async () => ({
    nodes: [
      { node_id: CSV_NODE_ID, node_type: 'python-csv', schedule_name: SCHED },
    ],
    edges: [],
  }),
  [`/api/schedules/${SCHED}/runs`]: async () => ({ runs: [] }),
  '/api/schedules': async () => ({
    schedules: [{ schedule_name: SCHED, last_run_id: RUN_ID }],
  }),
  [`/api/runs/${RUN_ID}/graph`]: async () => ({
    nodes: [
      { node_id: CSV_NODE_ID, node_type: 'python-csv', schedule_name: SCHED, status: 'succeeded' },
    ],
    edges: [],
    run_promotion_seq: 1,
    latest_promotion_seq: 1,
  }),
  [`/api/runs/${RUN_ID}`]: async () => ({
    run_id: RUN_ID, schedule_name: SCHED, status: 'succeeded',
    created_at: null, started_at: null, completed_at: null, cancelled_at: null, cancelled_by: '',
  }),
};

function renderLatest() {
  return render(
    <MemoryRouter initialEntries={[`/schedule/${SCHED}/latest`]}>
      <Routes>
        <Route path="/schedule/:name/latest" element={<DetailPage mode="latest" />} />
      </Routes>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('DetailPage focus legend node type', () => {
  it('does not show the "Open node detail" link when no node is selected', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(routes));

    const { container, queryByRole } = renderLatest();

    await waitFor(() => {
      expect(container.querySelector(`.react-flow__node[data-id="${CSV_NODE_ID}"]`)).not.toBeNull();
    });

    expect(queryByRole('link', { name: /open node detail/i })).toBeNull();
  });

  it('shows the selected node family icon in the legend title', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(routes));

    const { container } = renderLatest();

    await waitFor(() => {
      expect(container.querySelector(`.react-flow__node[data-id="${CSV_NODE_ID}"]`)).not.toBeNull();
    });
    fireEvent.click(container.querySelector(`.react-flow__node[data-id="${CSV_NODE_ID}"]`)!);

    await waitFor(() => {
      const legend = container.querySelector('.dag-focus-legend');
      expect(legend).not.toBeNull();
      expect(legend!.querySelector('[data-node-type-icon="python-csv"]')).not.toBeNull();
      expect(legend!.textContent).toContain('vendor_feed');
    });
  });

  it('reveals an "Open node detail" link for the selected node', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(routes));

    const { container, findByRole } = renderLatest();

    await waitFor(() => {
      expect(container.querySelector(`.react-flow__node[data-id="${CSV_NODE_ID}"]`)).not.toBeNull();
    });
    fireEvent.click(container.querySelector(`.react-flow__node[data-id="${CSV_NODE_ID}"]`)!);

    const link = await findByRole('link', { name: /open node detail/i });
    expect(link.getAttribute('href')).toBe(`/node/${CSV_NODE_ID}`);
  });
});

// An unknown-external node is a table a node reads and no node builds: it has no node
// page, so a click only highlights the nodes it feeds.
describe('DetailPage focus legend for an unknown-external node', () => {
  const UNKNOWN_EXTERNAL_ID = '.raw.ingest_probe';
  const READER_ID = 'svc1.public.reader';
  const unknownExternalRoutes = {
    ...routes,
    [`/api/schedules/${SCHED}/graph`]: async () => ({
      nodes: [
        { node_id: UNKNOWN_EXTERNAL_ID, node_type: 'unknown-external', schedule_name: SCHED },
        { node_id: READER_ID, node_type: 'dbt-model', schedule_name: SCHED },
      ],
      edges: [{ from_node_id: UNKNOWN_EXTERNAL_ID, to_node_id: READER_ID }],
    }),
  };

  async function renderAndSelect(nodeId: string) {
    vi.stubGlobal('fetch', mockFetchSequence(unknownExternalRoutes));
    const view = renderLatest();
    await waitFor(() => {
      expect(view.container.querySelector(`.react-flow__node[data-id="${nodeId}"]`)).not.toBeNull();
    });
    fireEvent.click(view.container.querySelector(`.react-flow__node[data-id="${nodeId}"]`)!);
    return view;
  }

  it('keeps the legend and offers no "Open node detail" link for a selected unknown-external node', async () => {
    const { container, queryByRole } = await renderAndSelect(UNKNOWN_EXTERNAL_ID);

    await waitFor(() => {
      const legend = container.querySelector('.dag-focus-legend');
      expect(legend).not.toBeNull();
      expect(legend!.textContent).toContain('ingest_probe');
      expect(legend!.textContent).toContain('Required by (1)');
    });
    expect(container.querySelector('.dag-focus-open-link')).toBeNull();
    expect(queryByRole('link', { name: /open node detail/i })).toBeNull();
  });

  it('names the selected unknown-external node with its schema, as the canvas does', async () => {
    const { container } = await renderAndSelect(UNKNOWN_EXTERNAL_ID);

    await waitFor(() => {
      expect(container.querySelector('.dag-focus-legend-title')?.textContent).toBe('raw.ingest_probe');
    });
    const canvasNode = container.querySelector(`.react-flow__node[data-id="${UNKNOWN_EXTERNAL_ID}"]`);
    expect(canvasNode?.textContent).toContain('raw.ingest_probe');
  });

  it('still offers the link for an ordinary node of the same graph', async () => {
    const { container, findByRole } = await renderAndSelect(READER_ID);

    const link = await findByRole('link', { name: /open node detail/i });
    expect(link.getAttribute('href')).toBe(`/node/${READER_ID}`);
    expect(container.querySelector('.dag-focus-legend')?.textContent).toContain('Depends on (1)');
    expect(container.querySelector('.dag-focus-legend-title')?.textContent).toBe('reader');
  });
});
