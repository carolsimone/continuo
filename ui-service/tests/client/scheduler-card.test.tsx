// @vitest-environment jsdom
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, waitFor, cleanup, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import SchedulerCard from '../../src/client/SchedulerCard';
import { ScheduleSummary } from '../../src/client/types';

function baseSchedule(overrides: Partial<ScheduleSummary> = {}): ScheduleSummary {
  return {
    schedule_name: 'hourly-events',
    cron_expression: '0 * * * *',
    description: '',
    timezone: 'UTC',
    is_running: true,
    last_run_at: null,
    last_run_status: 'PENDING',
    last_run_id: 'run-1',
    ...overrides,
  };
}

function renderCard(schedule: ScheduleSummary) {
  return render(
    <MemoryRouter>
      <SchedulerCard schedule={schedule} />
    </MemoryRouter>,
  );
}

// Renders the card behind a route so navigation can be observed by asserting
// which route ends up on screen, rather than mocking useNavigate.
function renderCardWithRouteProbe(schedule: ScheduleSummary) {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Routes>
        <Route path="/" element={<SchedulerCard schedule={schedule} />} />
        <Route path="/schedule/:name" element={<div>SCHEDULE DETAIL</div>} />
      </Routes>
    </MemoryRouter>,
  );
}

// Per-test fetch mock keyed by URL substring. Any unmatched URL resolves to an
// empty body so unrelated polls (e.g. /api/schedulers/:id/tasks) don't blow up.
type FetchMap = { graph?: any; tasks?: any };

function installFetch(map: FetchMap = {}) {
  const handler = vi.fn((input: RequestInfo | URL) => {
    const url = typeof input === 'string' ? input : input.toString();
    if (url.includes('/graph') && map.graph !== undefined) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve(map.graph) } as Response);
    }
    if (url.includes('/tasks')) {
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve(map.tasks ?? { tasks: [] }),
      } as Response);
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve({}) } as Response);
  });
  vi.stubGlobal('fetch', handler);
  return handler;
}

beforeEach(() => { installFetch(); });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

describe('SchedulerCard — trigger button uses .btn', () => {
  it('Trigger run button has .btn.btn--secondary, no legacy class', () => {
    renderCard(baseSchedule({ is_running: false, last_run_id: 'r1' }));
    const btn = screen.getByTitle('Trigger a full DAG run');
    expect(btn.className).toMatch(/\bbtn\b/);
    expect(btn.className).toMatch(/\bbtn--secondary\b/);
    expect(btn.className).not.toMatch(/\btrigger-run-btn\b/);
    expect(btn).toHaveTextContent('Trigger run');
  });
});

describe('SchedulerCard — operation selector', () => {
  it('trigger button verb tracks the operation select, POST carries operation', async () => {
    const handler = installFetch();
    renderCard(baseSchedule({ is_running: false, last_run_id: 'r1' }));

    const btn = screen.getByTitle('Trigger a full DAG run');
    expect(btn).toHaveTextContent(/trigger run/i);

    const sel = screen.getByLabelText(/operation/i);
    fireEvent.change(sel, { target: { value: 'build' } });

    expect(btn).toHaveTextContent(/trigger build/i);

    fireEvent.click(btn);

    await waitFor(() => {
      const triggerCall = handler.mock.calls.find(([input]) =>
        String(input).includes('/trigger'),
      );
      expect(triggerCall).toBeDefined();
      const [, init] = triggerCall as [RequestInfo, RequestInit];
      const body = JSON.parse(String(init?.body));
      expect(body).toMatchObject({ operation: 'build' });
    });
  });

  it('clicking the operation .form-field does not navigate the card, unlike the card body', async () => {
    const schedule = baseSchedule({ is_running: false, last_run_id: 'r1' });
    renderCardWithRouteProbe(schedule);

    const formField = screen.getByLabelText(/operation/i).closest('.form-field');
    expect(formField).not.toBeNull();
    fireEvent.click(formField as Element);

    // Give any (unwanted) navigation a chance to happen before asserting.
    await Promise.resolve();
    expect(screen.queryByText('SCHEDULE DETAIL')).toBeNull();

    // Contrast: clicking the card body (outside the form-field) does navigate.
    fireEvent.click(screen.getByText(schedule.schedule_name));
    await waitFor(() => {
      expect(screen.getByText('SCHEDULE DETAIL')).toBeInTheDocument();
    });
  });
});

describe('SchedulerCard — cancel button uses .btn', () => {
  it('Cancel button has .btn.btn--danger, no legacy class', () => {
    renderCard(baseSchedule({ is_running: true, last_run_id: 'r1' }));
    const btn = screen.getByTitle('Cancel the active run');
    expect(btn.className).toMatch(/\bbtn\b/);
    expect(btn.className).toMatch(/\bbtn--danger\b/);
    expect(btn.className).not.toMatch(/\bcancel-run-btn\b/);
  });
});

describe('SchedulerCard — drift strip', () => {
  it('renders the stale strip when /api/runs/:id/graph reports stale (in-flight run)', async () => {
    installFetch({ graph: { run_topology_generation: 5, latest_topology_generation: 7 } });
    renderCard(baseSchedule({ is_running: true, last_run_id: 'run-1' }));
    const strip = await screen.findByText(/source 2 gen behind latest/);
    expect(strip.closest('.info-strip')).toBeInTheDocument();
    expect(strip.closest('.info-strip--warning')).toBeInTheDocument();
  });

  it('renders the stale strip when the last finalised run is stale', async () => {
    installFetch({ graph: { run_topology_generation: 3, latest_topology_generation: 10 } });
    renderCard(baseSchedule({
      is_running: false,
      last_run_status: 'SUCCEEDED',
      last_run_id: 'run-final',
    }));
    const strip = await screen.findByText(/source 7 gen behind latest/);
    expect(strip.closest('.info-strip--warning')).toBeInTheDocument();
  });

  it('renders the neutral unknown strip when run_topology_generation is 0', async () => {
    installFetch({ graph: { run_topology_generation: 0, latest_topology_generation: 7 } });
    renderCard(baseSchedule({ is_running: true, last_run_id: 'run-1' }));
    const strip = await screen.findByText('topology version unknown');
    expect(strip.closest('.info-strip--neutral')).toBeInTheDocument();
  });

  it('does NOT render the strip when drift is fresh', async () => {
    const handler = installFetch({ graph: { run_topology_generation: 7, latest_topology_generation: 7 } });
    renderCard(baseSchedule({ is_running: true, last_run_id: 'run-1' }));
    await waitFor(() => {
      expect(handler).toHaveBeenCalled();
    });
    expect(screen.queryByText(/gen behind latest/i)).toBeNull();
    expect(screen.queryByText(/topology version unknown/i)).toBeNull();
  });

  it('does NOT fetch graph or render strip for a never-run schedule', async () => {
    const handler = installFetch({ graph: { run_topology_generation: 1, latest_topology_generation: 9 } });
    renderCard(baseSchedule({ is_running: false, last_run_id: null, last_run_status: '' }));
    await Promise.resolve();
    const calledGraph = handler.mock.calls.some(([input]) =>
      String(input).includes('/graph')
    );
    expect(calledGraph).toBe(false);
    expect(screen.queryByText(/gen behind latest/i)).toBeNull();
    expect(screen.queryByText(/topology version unknown/i)).toBeNull();
  });
});
