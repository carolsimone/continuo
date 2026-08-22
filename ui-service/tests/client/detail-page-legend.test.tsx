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

const routes = {
  [`/api/runs/${RUN_ID}/graph`]: async () => ({
    nodes: [
      { node_id: CSV_NODE_ID, node_type: 'python-csv', schedule_name: SCHED, status: 'succeeded' },
    ],
    edges: [],
    run_topology_generation: 1,
    latest_topology_generation: 1,
  }),
  [`/api/schedules/${SCHED}/graph`]: async () => ({
    nodes: [
      { node_id: CSV_NODE_ID, node_type: 'python-csv', schedule_name: SCHED },
    ],
    edges: [],
  }),
  [`/api/schedulers/${RUN_ID}/tasks`]: async () => ({
    tasks: [
      {
        task_id: 't1', service_name: 'svc1', schema_name: 'public', table_name: 'vendor_feed',
        job_name: 'vendor_feed', status: 'succeeded', retry_count: 0, max_retries: 0, created_at: null,
      },
    ],
  }),
  [`/api/schedulers/${RUN_ID}/executions`]: async () => ({ executions: [] }),
  [`/api/schedules/${SCHED}/runs`]: async () => ({ runs: [] }),
  [`/api/schedulers/${RUN_ID}`]: async () => ({
    scheduler: {
      schedule_id: RUN_ID, schedule_name: SCHED, status: 'succeeded',
      created_at: null, started_at: null, completed_at: null, cancelled_at: null, cancelled_by: '',
    },
  }),
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe('DetailPage focus legend node type', () => {
  it('shows the selected node family icon in the legend title', async () => {
    vi.stubGlobal('fetch', mockFetchSequence(routes));

    const { container } = render(
      <MemoryRouter initialEntries={[{ pathname: `/schedule/${SCHED}`, state: { last_run_id: RUN_ID } }]}>
        <Routes>
          <Route path="/schedule/:name" element={<DetailPage />} />
        </Routes>
      </MemoryRouter>,
    );

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
});
