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

describe('SchedulerCard — stale strip', () => {
  it('renders the stale strip with gen N/M when active run drift is stale', () => {
    renderCard(
      baseSchedule({ active_run_id: 'run-1', active_run_topology_generation: 5 }),
      7,
    );
    const strip = screen.getByText(
      /Stale — pinned to topology gen 5; latest is 7\. Rerun will use the older snapshot\./,
    );
    expect(strip).toBeInTheDocument();
    expect(strip.closest('.scheduler-card-stale-strip')).toBeInTheDocument();
    expect(strip.closest('.scheduler-card-stale-strip--unknown')).toBeNull();
  });

  it('renders the unknown variant of the strip when run gen is 0', () => {
    renderCard(
      baseSchedule({ active_run_id: 'run-1', active_run_topology_generation: 0 }),
      7,
    );
    const strip = screen.getByText(
      'Topology version unknown for this run. May not match the current topology.',
    );
    expect(strip.closest('.scheduler-card-stale-strip--unknown')).toBeInTheDocument();
  });

  it('does NOT render the strip when active_run_id is null (run finalised)', () => {
    renderCard(
      baseSchedule({ active_run_id: null, active_run_topology_generation: null }),
      7,
    );
    expect(screen.queryByText(/Stale/i)).toBeNull();
    expect(screen.queryByText(/Topology version unknown/i)).toBeNull();
  });

  it('does NOT render the strip when drift is fresh', () => {
    renderCard(
      baseSchedule({ active_run_id: 'run-1', active_run_topology_generation: 7 }),
      7,
    );
    expect(screen.queryByText(/Stale/i)).toBeNull();
    expect(screen.queryByText(/Topology version unknown/i)).toBeNull();
  });
});
