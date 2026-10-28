// @vitest-environment jsdom
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, waitFor, cleanup, fireEvent, act } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router';
import SchedulerCard from '../../src/client/SchedulerCard';
import { ScheduleSummary, Task } from '../../src/client/types';

function baseSchedule(overrides: Partial<ScheduleSummary> = {}): ScheduleSummary {
  return {
    schedule_name: 'hourly-events',
    cron_expression: '0 * * * *',
    description: '',
    timezone: 'UTC',
    has_active_run: true,
    last_run_at: null,
    last_run_status: 'pending',
    last_run_id: 'run-1',
    ...overrides,
  };
}

function renderCard(schedule: ScheduleSummary) {
  return render(
    <MemoryRouter>
      <SchedulerCard schedule={schedule} onRunChanged={() => {}} />
    </MemoryRouter>,
  );
}

// Renders the card behind a route so navigation can be observed by asserting
// which route ends up on screen, rather than mocking useNavigate.
function renderCardWithRouteProbe(schedule: ScheduleSummary) {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Routes>
        <Route path="/" element={<SchedulerCard schedule={schedule} onRunChanged={() => {}} />} />
        <Route path="/schedule/:name" element={<div>SCHEDULE DETAIL</div>} />
      </Routes>
    </MemoryRouter>,
  );
}

// Per-test fetch mock keyed by URL substring. Any unmatched URL resolves to an
// empty body so unrelated polls (e.g. /api/runs/:id/tasks) don't blow up.
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
    renderCard(baseSchedule({ has_active_run: false, last_run_id: 'r1' }));
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
    renderCard(baseSchedule({ has_active_run: false, last_run_id: 'r1' }));

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
    const schedule = baseSchedule({ has_active_run: false, last_run_id: 'r1' });
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

  it('pressing Enter on the operation select does not navigate, unlike Enter on the card', async () => {
    const schedule = baseSchedule({ has_active_run: false, last_run_id: 'r1' });
    renderCardWithRouteProbe(schedule);

    // Enter on a nested control must not bubble up and navigate the card.
    fireEvent.keyDown(screen.getByLabelText(/operation/i), { key: 'Enter' });
    await Promise.resolve();
    expect(screen.queryByText('SCHEDULE DETAIL')).toBeNull();

    // Contrast: Enter on the card itself (the focused role=button) navigates.
    const card = screen.getByText(schedule.schedule_name).closest('.scheduler-card');
    expect(card).not.toBeNull();
    fireEvent.keyDown(card as Element, { key: 'Enter' });
    await waitFor(() => {
      expect(screen.getByText('SCHEDULE DETAIL')).toBeInTheDocument();
    });
  });
});

describe('SchedulerCard — cancel button uses .btn', () => {
  it('Cancel button has .btn.btn--danger, no legacy class', () => {
    renderCard(baseSchedule({ has_active_run: true, last_run_id: 'r1' }));
    const btn = screen.getByTitle('Cancel the active run');
    expect(btn.className).toMatch(/\bbtn\b/);
    expect(btn.className).toMatch(/\bbtn--danger\b/);
    expect(btn.className).not.toMatch(/\bcancel-run-btn\b/);
  });
});

describe('SchedulerCard — a cancelling run', () => {
  it('reads cancelling, counts as active and keeps Cancel available', () => {
    const { container } = renderCard(baseSchedule({ has_active_run: true, last_run_status: 'cancelling', last_run_id: 'r1' }));
    const badge = container.querySelector('.status-badge')!;
    expect(badge).toHaveTextContent('cancelling');
    expect(badge.className).toMatch(/\bstatus-cancelling\b/);
    expect(container.querySelector('.card-border-grey')).toBeNull();
    expect(screen.getByTitle('Cancel the active run')).toBeInTheDocument();
    expect(screen.getByTitle('A run is already active')).toBeDisabled();
  });

  it('still reads running for a run that is not cancelling', () => {
    const { container } = renderCard(baseSchedule({ has_active_run: true, last_run_status: 'running', last_run_id: 'r1' }));
    expect(container.querySelector('.status-badge')).toHaveTextContent('running');
  });

  it('counts stopping tasks in the summary row', async () => {
    const tasks = ['stopping', 'stopping', 'running', 'succeeded'].map((status, i) => ({
      task_id: `t${i}`, service_name: 's', schema_name: 'a', table_name: `t${i}`, job_name: '',
      status, attempt: 0, max_retries: 0, created_at: null,
    }));
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      const body = url.includes('/tasks') ? { total_count: tasks.length, tasks } : {};
      return Promise.resolve({ ok: true, json: () => Promise.resolve(body) } as Response);
    }));
    const { container } = renderCard(baseSchedule({ has_active_run: true, last_run_status: 'cancelling', last_run_id: 'r1' }));
    await waitFor(() => expect(container.querySelector('.summary-row')).not.toBeNull());
    expect(container.querySelector('.summary-row')!.textContent).toMatch(/2 stopping/);
  });
});

describe('SchedulerCard — drift strip', () => {
  it('renders the stale strip when /api/runs/:id/graph reports stale (in-flight run)', async () => {
    installFetch({ graph: { run_promotion_seq: 5, latest_promotion_seq: 7 } });
    renderCard(baseSchedule({ has_active_run: true, last_run_id: 'run-1' }));
    const strip = await screen.findByText(/source 2 releases behind latest/);
    expect(strip.closest('.info-strip')).toBeInTheDocument();
    expect(strip.closest('.info-strip--warning')).toBeInTheDocument();
  });

  it('renders the stale strip when the last finalised run is stale', async () => {
    installFetch({ graph: { run_promotion_seq: 3, latest_promotion_seq: 10 } });
    renderCard(baseSchedule({
      has_active_run: false,
      last_run_status: 'succeeded',
      last_run_id: 'run-final',
    }));
    const strip = await screen.findByText(/source 7 releases behind latest/);
    expect(strip.closest('.info-strip--warning')).toBeInTheDocument();
  });

  it('does NOT render a strip when run_promotion_seq is 0 (unknown, pre-tracking run)', async () => {
    const handler = installFetch({ graph: { run_promotion_seq: 0, latest_promotion_seq: 7 } });
    renderCard(baseSchedule({ has_active_run: true, last_run_id: 'run-1' }));
    await waitFor(() => {
      expect(handler).toHaveBeenCalled();
    });
    expect(screen.queryByText(/topology version unknown/i)).toBeNull();
    expect(screen.queryByText(/behind latest/i)).toBeNull();
  });

  it('does NOT render the strip when drift is fresh', async () => {
    const handler = installFetch({ graph: { run_promotion_seq: 7, latest_promotion_seq: 7 } });
    renderCard(baseSchedule({ has_active_run: true, last_run_id: 'run-1' }));
    await waitFor(() => {
      expect(handler).toHaveBeenCalled();
    });
    expect(screen.queryByText(/behind latest/i)).toBeNull();
    expect(screen.queryByText(/topology version unknown/i)).toBeNull();
  });

  it('does NOT fetch graph or render strip for a never-run schedule', async () => {
    const handler = installFetch({ graph: { run_promotion_seq: 1, latest_promotion_seq: 9 } });
    renderCard(baseSchedule({ has_active_run: false, last_run_id: null, last_run_status: '' }));
    await Promise.resolve();
    const calledGraph = handler.mock.calls.some(([input]) =>
      String(input).includes('/graph')
    );
    expect(calledGraph).toBe(false);
    expect(screen.queryByText(/behind latest/i)).toBeNull();
    expect(screen.queryByText(/topology version unknown/i)).toBeNull();
  });
});

describe('SchedulerCard — activity badge', () => {
  it('shows Active with cron + timezone when the schedule is in schedules.yaml', () => {
    const { container } = renderCard(baseSchedule({
      cron_expression: '0 1 * * *',
      timezone: 'Europe/Paris',
      has_active_run: false,
      last_run_id: 'r1',
    }));
    expect(screen.getByText('Active')).toBeInTheDocument();
    expect(screen.queryByText('Inactive')).toBeNull();
    const cron = container.querySelector('.schedule-cron');
    expect(cron).not.toBeNull();
    expect(cron).toHaveTextContent('0 1 * * *');
    expect(cron).toHaveTextContent('Europe/Paris');
  });

  it('shows Inactive and no cron text when the schedule is not in schedules.yaml', () => {
    const { container } = renderCard(baseSchedule({
      cron_expression: '',
      timezone: '',
      has_active_run: false,
      last_run_id: 'r1',
    }));
    expect(screen.getByText('Inactive')).toBeInTheDocument();
    expect(screen.queryByText('Active')).toBeNull();
    expect(container.querySelector('.schedule-cron')).toBeNull();
  });
});

describe('SchedulerCard — task counts', () => {
  const mkTask = (i: number, status: string): Task => ({
    task_id: `t-${i}`, service_name: 'svc', schema_name: 'schema', table_name: `tbl${i}`,
    job_name: 'job', status, attempt: 0, max_retries: 0, created_at: null,
  });

  it('counts every task status once, dispatched, queued and stopping before running and pending, and the counts add up to the total', async () => {
    const statuses = [
      'succeeded', 'succeeded', 'failed', 'skipped', 'cancelled', 'cancelled',
      'dispatched', 'queued', 'queued', 'stopping', 'running', 'pending', 'pending', 'pending',
    ];
    const tasks = statuses.map((s, i) => mkTask(i, s));
    installFetch({ tasks: { tasks, total_count: tasks.length } });
    const { container } = renderCard(baseSchedule({ has_active_run: true, last_run_id: 'r1' }));

    await waitFor(() => {
      expect(container.querySelector('.summary-row')).not.toBeNull();
    });
    const counts = [...container.querySelectorAll('.summary-row > span')].map((el) => {
      const m = /(\d+)\s+(\w+)/.exec(el.textContent ?? '');
      return [m?.[2], Number(m?.[1])] as const;
    });
    expect(counts).toEqual([
      ['succeeded', 2], ['failed', 1], ['skipped', 1], ['cancelled', 2],
      ['dispatched', 1], ['queued', 2], ['stopping', 1], ['running', 1], ['pending', 3],
    ]);
    expect(counts.reduce((sum, [, n]) => sum + n, 0)).toBe(tasks.length);
  });
});

describe('SchedulerCard — request id', () => {
  const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

  // Answers the trigger with the next reply in order, a rejection standing
  // for a request that got no answer, and returns the request ids it received.
  function installTrigger(replies: Array<{ ok: boolean; status?: number; body?: unknown } | 'timeout'>) {
    const ids: string[] = [];
    let n = 0;
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).includes('/trigger')) {
        ids.push(JSON.parse(String(init?.body)).request_id);
        const reply = replies[n++];
        if (reply === 'timeout') return Promise.reject(new Error('timeout'));
        return Promise.resolve({ ok: reply.ok, status: reply.status ?? (reply.ok ? 200 : 409), json: () => Promise.resolve(reply.body ?? {}) } as Response);
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ tasks: [] }) } as Response);
    }));
    return ids;
  }

  it('SchedulerCard trigger: the body carries a request id', async () => {
    const ids = installTrigger([{ ok: true, body: { run_id: 'r2' } }]);
    renderCard(baseSchedule({ has_active_run: false, last_run_id: 'r1' }));

    fireEvent.click(screen.getByTitle('Trigger a full DAG run'));

    await waitFor(() => expect(ids).toHaveLength(1));
    expect(ids[0]).toMatch(UUID);
  });

  it('requestId: a press after a timeout is a retry and sends the same id', async () => {
    const ids = installTrigger(['timeout', { ok: true }]);
    renderCard(baseSchedule({ has_active_run: false, last_run_id: 'r1' }));
    const button = () => screen.getByTitle('Trigger a full DAG run');

    fireEvent.click(button());
    await waitFor(() => expect(screen.getByText(/timeout/)).toBeInTheDocument());
    fireEvent.click(button());

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toBe(ids[0]);
  });

  it('requestId: a press after a 500 is a retry too, because the command may have committed', async () => {
    const ids = installTrigger([{ ok: false, status: 500, body: { error: 'upstream timed out' } }, { ok: true }]);
    renderCard(baseSchedule({ has_active_run: false, last_run_id: 'r1' }));

    fireEvent.click(screen.getByTitle('Trigger a full DAG run'));
    await waitFor(() => expect(screen.getByText(/upstream timed out/)).toBeInTheDocument());
    fireEvent.click(screen.getByTitle('Trigger a full DAG run'));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toBe(ids[0]);
  });

  it('requestId: a press after the server answered mints a new id', async () => {
    // The success cue disables the button for 3 s; fake that timer so the test
    // does not wait for it.
    vi.useFakeTimers({ toFake: ['setTimeout'] });
    try {
      const ids = installTrigger([{ ok: true, body: { run_id: 'r2' } }, { ok: true, body: { run_id: 'r3' } }]);
      renderCard(baseSchedule({ has_active_run: false, last_run_id: 'r1' }));
      const flush = () => act(async () => { for (let i = 0; i < 5; i++) await Promise.resolve(); });

      fireEvent.click(screen.getByTitle('Trigger a full DAG run'));
      await flush();
      expect(ids).toHaveLength(1);
      expect(screen.getByTitle('Trigger a full DAG run')).toHaveTextContent('Triggered');
      await act(async () => { vi.advanceTimersByTime(3000); });
      fireEvent.click(screen.getByTitle('Trigger a full DAG run'));
      await flush();

      expect(ids).toHaveLength(2);
      expect(ids[1]).toMatch(UUID);
      expect(ids[1]).not.toBe(ids[0]);
    } finally {
      vi.useRealTimers();
    }
  });

  it('requestId: a press hours after an unanswered one, with no newer run on the card, is a retry with the same id', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    try {
      const ids = installTrigger(['timeout', { ok: true, body: { run_id: 'r3' } }]);
      renderCard(baseSchedule({ has_active_run: false, last_run_id: 'r1' }));

      fireEvent.click(screen.getByTitle('Trigger a full DAG run'));
      await waitFor(() => expect(screen.getByText(/timeout/)).toBeInTheDocument());
      vi.setSystemTime(Date.now() + 6 * 60 * 60 * 1000);
      fireEvent.click(screen.getByTitle('Trigger a full DAG run'));

      await waitFor(() => expect(ids).toHaveLength(2));
      expect(ids[1]).toBe(ids[0]);
    } finally {
      vi.useRealTimers();
    }
  });

  it('requestId: a press after the card shows a newer run than the unanswered press saw is a new action', async () => {
    const ids = installTrigger(['timeout', { ok: true, body: { run_id: 'r3' } }]);
    const view = renderCard(baseSchedule({ has_active_run: false, last_run_id: 'r1' }));

    fireEvent.click(screen.getByTitle('Trigger a full DAG run'));
    await waitFor(() => expect(screen.getByText(/timeout/)).toBeInTheDocument());
    // The schedule list's next poll names the run the unanswered press made.
    view.rerender(
      <MemoryRouter>
        <SchedulerCard schedule={baseSchedule({ has_active_run: false, last_run_id: 'r2' })} onRunChanged={() => {}} />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByTitle('Trigger a full DAG run'));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).toMatch(UUID);
    expect(ids[1]).not.toBe(ids[0]);
  });

  it('requestId: changing the operation mints a new id', async () => {
    const ids = installTrigger(['timeout', { ok: true }]);
    renderCard(baseSchedule({ has_active_run: false, last_run_id: 'r1' }));

    fireEvent.click(screen.getByTitle('Trigger a full DAG run'));
    await waitFor(() => expect(screen.getByText(/timeout/)).toBeInTheDocument());
    fireEvent.change(screen.getByLabelText(/operation/i), { target: { value: 'build' } });
    fireEvent.click(screen.getByTitle('Trigger a full DAG run'));

    await waitFor(() => expect(ids).toHaveLength(2));
    expect(ids[1]).not.toBe(ids[0]);
  });
});
