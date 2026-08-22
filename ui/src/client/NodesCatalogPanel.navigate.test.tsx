// @vitest-environment jsdom
import { it, expect } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router';
import NodesCatalogPanel from './NodesCatalogPanel';
import type { NodeSummary } from './types';

function node(over: Partial<NodeSummary> = {}): NodeSummary {
  return {
    service_name: 'svc',
    schema_name: 'schema',
    table_name: 'customers',
    run_count: 3,
    success_rate_pct: 100,
    avg_duration_sec: 5,
    p95_duration_sec: 9,
    flaky_rate_pct: 0,
    last_status: 'succeeded',
    last_run_at: '2026-05-10T10:00:00Z',
    operation: 'run',
    ...over,
  };
}

// Probe that renders the state landed on /node/:fqn so we can assert on the
// navigation state without mocking useNavigate.
function LocationProbe() {
  const location = useLocation();
  return <div data-testid="probe">{JSON.stringify(location.state)}</div>;
}

function stubFetch(nodes: NodeSummary[]) {
  global.fetch = ((url: string) => {
    if (String(url).includes('/api/nodes?')) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ total_count: nodes.length, nodes }) });
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve({ names: [] }) });
  }) as any;
}

function renderWithProbe() {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Routes>
        <Route path="/" element={<NodesCatalogPanel />} />
        <Route path="/node/:fqn" element={<LocationProbe />} />
      </Routes>
    </MemoryRouter>,
  );
}

it('clicking a row while operation is test navigates carrying the selected operation', async () => {
  stubFetch([node()]);
  renderWithProbe();

  fireEvent.change(await screen.findByLabelText('Operation'), { target: { value: 'test' } });

  const row = await screen.findByText('customers');
  fireEvent.click(row.closest('tr') as HTMLElement);

  await waitFor(() => {
    const probe = screen.getByTestId('probe');
    expect(JSON.parse(probe.textContent || 'null')).toEqual({
      from: { type: 'nodes' },
      operation: 'test',
    });
  });
});

it('clicking a row while operation is the default run navigates carrying operation=run', async () => {
  stubFetch([node()]);
  renderWithProbe();

  const row = await screen.findByText('customers');
  fireEvent.click(row.closest('tr') as HTMLElement);

  await waitFor(() => {
    const probe = screen.getByTestId('probe');
    expect(JSON.parse(probe.textContent || 'null')).toEqual({
      from: { type: 'nodes' },
      operation: 'run',
    });
  });
});
