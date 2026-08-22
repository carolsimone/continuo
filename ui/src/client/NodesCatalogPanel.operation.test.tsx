// @vitest-environment jsdom
import { it, expect } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import NodesCatalogPanel from './NodesCatalogPanel';

// stub fetch to capture the operation query param and return an empty page
function stubFetch() {
  const calls: string[] = [];
  global.fetch = ((url: string) => {
    calls.push(url);
    return Promise.resolve({ ok: true, json: () => Promise.resolve({ total_count: 0, nodes: [] }) });
  }) as any;
  return calls;
}

it('defaults to the model operation and requests operation=run', async () => {
  const calls = stubFetch();
  render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
  await waitFor(() => expect(calls.some(u => u.includes('/api/nodes?') && u.includes('operation=run'))).toBe(true));
});

it('shows the build info-strip only when Build is selected', async () => {
  stubFetch();
  render(<MemoryRouter><NodesCatalogPanel /></MemoryRouter>);
  expect(screen.queryByText(/dbt build/i)).toBeNull();
  fireEvent.change(screen.getByLabelText('Operation'), { target: { value: 'build' } });
  await waitFor(() => expect(screen.getByText(/dbt build/i)).toBeInTheDocument());
});
