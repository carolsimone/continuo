// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import DashboardPage from '../../src/client/DashboardPage';

// The Runs card of one 41-task run, driven through the states a cancel moves
// it through: running, then cancelling while its six in-flight tasks stop,
// then cancelled. The card polls every 5 s; these tests hold the fake clock
// short of the next poll to see what an action shows on its own.
type Phase = 'running' | 'cancelling' | 'cancelled';

const RUN_ID = 'run-1';
const POLL_MS = 5000;

function tasksFor(phase: Phase) {
  const statuses: string[] = [];
  const add = (status: string, n: number) => { for (let i = 0; i < n; i++) statuses.push(status); };
  if (phase === 'running') { add('succeeded', 20); add('running', 6); add('pending', 15); }
  if (phase === 'cancelling') { add('succeeded', 20); add('stopping', 6); add('cancelled', 15); }
  if (phase === 'cancelled') { add('succeeded', 20); add('cancelled', 21); }
  return statuses.map((status, i) => ({
    task_id: `t${i}`, run_id: RUN_ID, service_name: 's', schema_name: 'a', table_name: `t${i}`,
    job_name: '', status, attempt: 0, max_retries: 0, created_at: null,
  }));
}

let phase: Phase;
// When set, the next periodic /api/schedules poll is held until the test
// releases it, so it can land after a later read.
let holdNextSchedulesPoll: boolean;
let releaseHeldPoll: (() => void) | null;

function scheduleNow() {
  return {
    schedule_name: 'daily',
    cron_expression: '0 0 * * *',
    description: '',
    timezone: 'UTC',
    has_active_run: phase !== 'cancelled',
    last_run_at: null,
    last_run_status: phase,
    last_run_id: RUN_ID,
  };
}

function ok(body: unknown) {
  return Promise.resolve({ ok: true, json: () => Promise.resolve(body) } as Response);
}

beforeEach(() => {
  phase = 'running';
  holdNextSchedulesPoll = false;
  releaseHeldPoll = null;
  vi.useFakeTimers();
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url === '/api/schedules') {
      const body = { schedules: [scheduleNow()] };
      if (holdNextSchedulesPoll) {
        holdNextSchedulesPoll = false;
        return new Promise<Response>((resolve) => {
          releaseHeldPoll = () => resolve({ ok: true, json: () => Promise.resolve(body) } as Response);
        });
      }
      return ok(body);
    }
    if (url.startsWith(`/api/runs/${RUN_ID}/tasks`)) {
      const tasks = tasksFor(phase);
      return ok({ total_count: tasks.length, tasks });
    }
    if (url === '/api/config') return ok({ cancellation_reasons: ['Incorrect trigger', 'Other'] });
    if (url === `/api/runs/${RUN_ID}/cancel` && init?.method === 'POST') {
      phase = 'cancelling';
      return ok({ run_id: RUN_ID, status: 'cancelling' });
    }
    return ok({});
  }));
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

async function advance(ms: number) {
  await act(async () => { await vi.advanceTimersByTimeAsync(ms); });
}

function badge(container: HTMLElement) {
  return container.querySelector('.status-badge');
}

function summary(container: HTMLElement) {
  return container.querySelector('.summary-row')?.textContent ?? '';
}

async function cancelThroughDialog() {
  fireEvent.click(screen.getByTitle('Cancel the active run'));
  await advance(0);
  fireEvent.change(screen.getByRole('combobox', { name: /reason/i }), { target: { value: 'Incorrect trigger' } });
  fireEvent.click(screen.getByRole('button', { name: /cancel run/i }));
  await advance(0);
}

describe('DashboardPage — the Runs card after a cancel', () => {
  it('reads cancelling with its stopping tasks as soon as the cancel succeeds, then cancelled at the next poll', async () => {
    const { container } = render(<MemoryRouter><DashboardPage /></MemoryRouter>);
    await advance(0);
    expect(badge(container)).toHaveTextContent('running');
    expect(screen.getByText('Completed: 20/41')).toBeInTheDocument();

    await cancelThroughDialog();
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(badge(container)).toHaveTextContent('cancelling');
    expect(summary(container)).toMatch(/6 stopping/);
    expect(summary(container)).toMatch(/15 cancelled/);
    expect(screen.getByText('Completed: 35/41')).toBeInTheDocument();

    phase = 'cancelled';
    await advance(POLL_MS);
    expect(badge(container)).toHaveTextContent('cancelled');
    expect(screen.getByText('Completed: 41/41')).toBeInTheDocument();
    expect(summary(container)).toMatch(/21 cancelled/);
    expect(screen.queryByTitle('Cancel the active run')).toBeNull();
  });

  it('keeps cancelling when a poll sent before the cancel answers after the read that follows it', async () => {
    const { container } = render(<MemoryRouter><DashboardPage /></MemoryRouter>);
    await advance(0);

    fireEvent.click(screen.getByTitle('Cancel the active run'));
    await advance(0);
    holdNextSchedulesPoll = true;
    await advance(POLL_MS);
    expect(releaseHeldPoll).not.toBeNull();

    fireEvent.change(screen.getByRole('combobox', { name: /reason/i }), { target: { value: 'Incorrect trigger' } });
    fireEvent.click(screen.getByRole('button', { name: /cancel run/i }));
    await advance(0);
    expect(badge(container)).toHaveTextContent('cancelling');

    await act(async () => { releaseHeldPoll!(); });
    await advance(0);
    expect(badge(container)).toHaveTextContent('cancelling');
  });
});

describe('DashboardPage — the Runs card after a trigger', () => {
  it('shows the new run as soon as the trigger succeeds', async () => {
    phase = 'cancelled';
    const fetchMock = vi.mocked(fetch);
    const base = fetchMock.getMockImplementation()!;
    let triggered = false;
    fetchMock.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url === '/api/schedules/daily/trigger' && init?.method === 'POST') {
        triggered = true;
        return ok({ run_id: 'run-2' });
      }
      if (url === '/api/schedules' && triggered) {
        return ok({ schedules: [{ ...scheduleNow(), has_active_run: true, last_run_status: 'running', last_run_id: 'run-2' }] });
      }
      if (url.startsWith('/api/runs/run-2/tasks')) {
        const tasks = tasksFor('running').map((t) => ({ ...t, run_id: 'run-2', status: 'pending' }));
        return ok({ total_count: tasks.length, tasks });
      }
      return base(input, init);
    });

    const { container } = render(<MemoryRouter><DashboardPage /></MemoryRouter>);
    await advance(0);
    expect(badge(container)).toHaveTextContent('cancelled');

    fireEvent.click(screen.getByTitle('Trigger a full DAG run'));
    await advance(0);
    expect(badge(container)).toHaveTextContent('running');
    expect(screen.getByText('Completed: 0/41')).toBeInTheDocument();
    expect(summary(container)).toMatch(/41 pending/);
  });
});
