// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
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
    active_run_topology_generation: null,
    active_run_id: null,
    ...overrides,
  };
}

function renderCard(schedule: ScheduleSummary, latestGen: number) {
  return render(
    <MemoryRouter>
      <SchedulerCard schedule={schedule} latestTopologyGeneration={latestGen} />
    </MemoryRouter>,
  );
}

describe('SchedulerCard — trigger button uses .btn', () => {
  it('Trigger run button has .btn.btn--secondary, no legacy class', () => {
    renderCard(baseSchedule({ is_running: false, last_run_id: 'r1' }), 0);
    const btn = screen.getByTitle('Trigger a full DAG run');
    expect(btn.className).toMatch(/\bbtn\b/);
    expect(btn.className).toMatch(/\bbtn--secondary\b/);
    expect(btn.className).not.toMatch(/\btrigger-run-btn\b/);
    expect(btn).toHaveTextContent('Trigger run');
  });
});

describe('SchedulerCard — cancel button uses .btn', () => {
  it('Cancel button has .btn.btn--danger, no legacy class', () => {
    renderCard(baseSchedule({ is_running: true, last_run_id: 'r1' }), 0);
    const btn = screen.getByTitle('Cancel the active run');
    expect(btn.className).toMatch(/\bbtn\b/);
    expect(btn.className).toMatch(/\bbtn--danger\b/);
    expect(btn.className).not.toMatch(/\bcancel-run-btn\b/);
  });
});

describe('SchedulerCard — stale strip', () => {
  it('renders the stale strip with gen N/M when active run drift is stale', () => {
    renderCard(
      baseSchedule({ active_run_id: 'run-1', active_run_topology_generation: 5 }),
      7,
    );
    const strip = screen.getByText(/source 2 gen behind latest/);
    expect(strip).toBeInTheDocument();
    expect(strip.closest('.info-strip')).toBeInTheDocument();
    expect(strip.closest('.info-strip--warning')).toBeInTheDocument();
  });

  it('renders the unknown variant of the strip when run gen is 0', () => {
    renderCard(
      baseSchedule({ active_run_id: 'run-1', active_run_topology_generation: 0 }),
      7,
    );
    const strip = screen.getByText('topology version unknown');
    expect(strip).toBeInTheDocument();
    expect(strip.closest('.info-strip--neutral')).toBeInTheDocument();
  });

  it('does NOT render the strip when active_run_id is null (run finalised)', () => {
    renderCard(
      baseSchedule({ active_run_id: null, active_run_topology_generation: null }),
      7,
    );
    expect(screen.queryByText(/gen behind latest/i)).toBeNull();
    expect(screen.queryByText(/topology version unknown/i)).toBeNull();
  });

  it('does NOT render the strip when drift is fresh', () => {
    renderCard(
      baseSchedule({ active_run_id: 'run-1', active_run_topology_generation: 7 }),
      7,
    );
    expect(screen.queryByText(/gen behind latest/i)).toBeNull();
    expect(screen.queryByText(/topology version unknown/i)).toBeNull();
  });
});
