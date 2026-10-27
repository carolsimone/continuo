// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router';
import RunRedirect from '../../src/client/RunRedirect';

function SchedulePage() {
  const location = useLocation();
  return <div data-testid="schedule">{location.pathname} {JSON.stringify(location.state)}</div>;
}

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/run/:id" element={<RunRedirect />} />
        <Route path="/schedule/:name" element={<SchedulePage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('RunRedirect', () => {
  afterEach(() => { vi.unstubAllGlobals(); });

  it('opens the run on its schedule\'s run page', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true, json: async () => ({ run_id: 'run-1', schedule_name: 'single-node-run-ab12cd34' }),
    });
    vi.stubGlobal('fetch', fetchMock);

    renderAt('/run/run-1');

    const page = await screen.findByTestId('schedule');
    expect(fetchMock).toHaveBeenCalledWith('/api/runs/run-1');
    expect(page.textContent).toBe('/schedule/single-node-run-ab12cd34 {"last_run_id":"run-1"}');
  });

  it('says so when the run cannot be read', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 404, json: async () => ({}) }));

    const { container } = renderAt('/run/missing');

    await waitFor(() => expect(container.querySelector('.info-strip--error')).toBeTruthy());
    expect(container.querySelector('.info-strip--error')!.textContent).toContain('missing');
  });
});
