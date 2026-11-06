// @vitest-environment jsdom
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, waitFor, cleanup, fireEvent, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route, useLocation, useParams } from 'react-router';
import SchedulerCard from '../../src/client/SchedulerCard';
import { ScheduleSummary, Task } from '../../src/client/types';
import { AuthContext } from '../../src/client/auth/AuthContext';
import type { AuthUser } from '../../src/client/auth/useAuth';

function baseSchedule(overrides: Partial<ScheduleSummary> = {}): ScheduleSummary {
  return {
    schedule_name: 'hourly-events',
    cron: '0 * * * *',
    timezone: 'UTC',
    description: '',
    paused: false,
    next_fire_at: null,
    version: 1,
    updated_by: 'alice@example.com',
    updated_at: '2026-10-21T12:02:00.000Z',
    model_count: 6,
    cron_words: 'Every hour',
    has_active_run: true,
    last_run_at: null,
    last_run_status: 'pending',
    last_run_id: 'run-1',
    ...overrides,
  };
}

const IDLE = { has_active_run: false, last_run_status: 'succeeded', last_run_id: 'r1' } as const;
const ACTIVE = { has_active_run: true, last_run_status: 'running', last_run_id: 'r1' } as const;

const OPERATOR: AuthUser = { userId: 'u1', email: 'op@example.com', name: 'Op', role: 'operator' };
const VIEWER: AuthUser = { userId: 'u2', email: 'viewer@example.com', name: 'Viewer', role: 'viewer' };

// One tree for a first render and for a rerender of it: a rerender with any
// other root remounts the card and drops its state.
function cardTree(schedule: ScheduleSummary, user: AuthUser | null = null, onRunChanged: () => void = () => {}) {
  return (
    <AuthContext.Provider value={user}>
      <MemoryRouter>
        <SchedulerCard schedule={schedule} onRunChanged={onRunChanged} />
      </MemoryRouter>
    </AuthContext.Provider>
  );
}

function renderCard(schedule: ScheduleSummary, user: AuthUser | null = null) {
  return render(cardTree(schedule, user));
}

function ScheduleDetailProbe() {
  const location = useLocation();
  const { name } = useParams();
  return <div data-testid="schedule-detail" data-name={name}>{JSON.stringify(location.state)}</div>;
}

// Renders the card behind a route so navigation can be observed by asserting
// which route ends up on screen, and with what state, rather than mocking
// useNavigate.
function renderCardWithRouteProbe(schedule: ScheduleSummary, user: AuthUser | null = null) {
  return render(
    <AuthContext.Provider value={user}>
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<SchedulerCard schedule={schedule} onRunChanged={() => {}} />} />
          <Route path="/schedule/:name" element={<ScheduleDetailProbe />} />
        </Routes>
      </MemoryRouter>
    </AuthContext.Provider>,
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

function mkTask(i: number, status: string): Task {
  return {
    task_id: `t-${i}`, service_name: 'svc', schema_name: 'schema', table_name: `tbl${i}`,
    job_name: 'job', status, attempt: 0, max_retries: 0, created_at: null,
  };
}

function tasksBody(...statuses: string[]) {
  const tasks = statuses.map((s, i) => mkTask(i, s));
  return { tasks, total_count: tasks.length };
}

function repeat(status: string, n: number): string[] {
  return Array.from({ length: n }, () => status);
}

const runStatus = (container: HTMLElement) => container.querySelector('[data-testid="run-status"]')!;
const shownCounts = (container: HTMLElement) =>
  [...container.querySelectorAll('.run-progress-counts .rp-count')].map(el => el.textContent);
const triggerButton = () => screen.queryByTitle('Trigger a full DAG run');
const cancelButton = () => screen.queryByTitle('Cancel the active run');

beforeEach(() => { installFetch(); });
afterEach(() => {
  cleanup();
  window.getSelection()?.removeAllRanges();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('SchedulerCard — status', () => {
  it('shows the status of the last run in the pill every other tab uses', () => {
    const { container } = renderCard(baseSchedule({ ...IDLE, last_run_status: 'failed' }));
    const pill = runStatus(container);
    expect(pill).toHaveTextContent('failed');
    expect(pill.className).toBe('pill-sm pill-sm--failed');
  });

  it('explains a cancelling run on hover', () => {
    const { container } = renderCard(baseSchedule({ ...ACTIVE, last_run_status: 'cancelling' }));
    expect(runStatus(container)).toHaveAttribute('title', 'cancelling — waiting for its Jobs to stop');
  });

  it('reads running for an active run whose status is still pending', () => {
    const { container } = renderCard(baseSchedule({ ...ACTIVE, last_run_status: 'pending' }));
    expect(runStatus(container)).toHaveTextContent('running');
  });

  it('reads never run, in the neutral pill, for a schedule with no run', () => {
    const { container } = renderCard(baseSchedule({ has_active_run: false, last_run_status: '', last_run_id: null }));
    const pill = runStatus(container);
    expect(pill).toHaveTextContent('never run');
    expect(pill.className).toBe('pill-sm pill-sm--pending');
  });

  it('gives a failed run a rail', () => {
    const { container } = renderCard(baseSchedule({ ...IDLE, last_run_status: 'failed' }));
    expect(container.querySelector('.scheduler-card--failed')).not.toBeNull();
  });

  it.each([
    ['succeeded', false],
    ['cancelled', false],
    ['running', true],
    ['cancelling', true],
  ] as const)('gives a %s run no rail', (status, active) => {
    const { container } = renderCard(baseSchedule({ has_active_run: active, last_run_status: status, last_run_id: 'r1' }));
    expect(container.querySelector('.scheduler-card--failed')).toBeNull();
  });
});

describe('SchedulerCard — navigation', () => {
  const STATE = JSON.stringify({ last_run_id: 'r1' });

  it('clicking anywhere on the card that is not a control opens the schedule, passing its last run', () => {
    renderCardWithRouteProbe(baseSchedule(IDLE));
    fireEvent.click(screen.getByText('Every hour'));
    expect(screen.getByTestId('schedule-detail')).toHaveTextContent(STATE);
    expect(screen.getByTestId('schedule-detail')).toHaveAttribute('data-name', 'hourly-events');
  });

  it('clicking the name opens the schedule, passing its last run', () => {
    renderCardWithRouteProbe(baseSchedule(IDLE));
    fireEvent.click(screen.getByRole('link', { name: 'hourly-events' }));
    expect(screen.getByTestId('schedule-detail')).toHaveTextContent(STATE);
  });

  it('makes the name a real link for the keyboard and a new tab, and the card no button around other controls', () => {
    const { container } = renderCard(baseSchedule(IDLE));
    const link = screen.getByRole('link', { name: 'hourly-events' });
    expect(link).toHaveAttribute('href', '/schedule/hourly-events');
    // A link set draggable=false turns a drag on it into a click, which navigates.
    expect(link).not.toHaveAttribute('draggable');
    const card = container.querySelector('.scheduler-card')!;
    expect(card).not.toHaveAttribute('role');
    expect(card).not.toHaveAttribute('tabindex');
  });

  it('routes a name that needs encoding, from the card and from the link', () => {
    const schedule = baseSchedule({ ...IDLE, schedule_name: 'a#b?c' });
    renderCardWithRouteProbe(schedule);
    fireEvent.click(screen.getByText('Every hour'));
    expect(screen.getByTestId('schedule-detail')).toHaveAttribute('data-name', 'a#b?c');
    cleanup();

    renderCardWithRouteProbe(schedule);
    fireEvent.click(screen.getByRole('link', { name: 'a#b?c' }));
    expect(screen.getByTestId('schedule-detail')).toHaveAttribute('data-name', 'a#b?c');
  });

  it.each(['metaKey', 'ctrlKey', 'shiftKey'])('a click with %s on the card leaves the page to the browser', (modifier) => {
    renderCardWithRouteProbe(baseSchedule(IDLE));
    fireEvent.click(screen.getByText('Every hour'), { [modifier]: true });
    expect(screen.queryByTestId('schedule-detail')).toBeNull();
  });

  it('does not navigate from a control or the gap between controls', () => {
    const { container } = renderCardWithRouteProbe(baseSchedule(IDLE), OPERATOR);
    fireEvent.click(screen.getByLabelText(/operation/i).closest('.btn-group') as Element);
    fireEvent.click(container.querySelector('.scheduler-card-actions') as Element);
    fireEvent.click(triggerButton() as Element);
    expect(screen.queryByTestId('schedule-detail')).toBeNull();
  });

  it('does not navigate from the drift chip or the no-models strip', async () => {
    installFetch({ graph: { run_promotion_seq: 5, latest_promotion_seq: 7 } });
    renderCardWithRouteProbe(baseSchedule({ ...IDLE, model_count: 0 }));
    fireEvent.click(await screen.findByText(/source 2 releases behind latest/));
    fireEvent.click(screen.getByText(/No model in the live release/));
    expect(screen.queryByTestId('schedule-detail')).toBeNull();
  });

  it('a drag that selects text on the card does not navigate', async () => {
    const user = userEvent.setup();
    renderCardWithRouteProbe(baseSchedule(IDLE));
    await user.pointer([
      { keys: '[MouseLeft>]', target: screen.getByText('Every hour'), offset: 0 },
      { offset: 5 },
      { keys: '[/MouseLeft]' },
    ]);
    expect(window.getSelection()?.toString()).toBe('Every');
    expect(screen.queryByTestId('schedule-detail')).toBeNull();
  });

  it('a click on the name navigates even with text selected on the page: a browser keeps the selection through a press on a link', () => {
    renderCardWithRouteProbe(baseSchedule(IDLE));
    window.getSelection()?.selectAllChildren(screen.getByText('Every hour'));
    expect(window.getSelection()?.toString()).toBe('Every hour');
    fireEvent.click(screen.getByRole('link', { name: 'hourly-events' }));
    expect(screen.getByTestId('schedule-detail')).toBeInTheDocument();
  });
});

describe('SchedulerCard — operation', () => {
  it('Trigger run button has .btn.btn--secondary', () => {
    renderCard(baseSchedule(IDLE));
    const btn = triggerButton() as HTMLElement;
    expect(btn.className).toMatch(/\bbtn\b/);
    expect(btn.className).toMatch(/\bbtn--secondary\b/);
    expect(btn).toHaveTextContent('Trigger run');
  });

  it('trigger button verb tracks the operation select, POST carries operation', async () => {
    const handler = installFetch();
    renderCard(baseSchedule(IDLE));

    const btn = triggerButton() as HTMLElement;
    expect(btn).toHaveTextContent(/trigger run/i);

    fireEvent.change(screen.getByLabelText(/operation/i), { target: { value: 'build' } });
    expect(btn).toHaveTextContent(/trigger build/i);

    fireEvent.click(btn);

    await waitFor(() => {
      const triggerCall = handler.mock.calls.find(([input]) =>
        String(input).includes('/trigger'),
      );
      expect(triggerCall).toBeDefined();
      const [, init] = triggerCall as unknown as [RequestInfo, RequestInit];
      const body = JSON.parse(String(init?.body));
      expect(body).toMatchObject({ operation: 'build' });
    });
  });

  it('labels the operation select for a screen reader and on hover, with no visible label', () => {
    renderCard(baseSchedule(IDLE));
    const select = screen.getByRole('combobox', { name: 'Operation' });
    const label = select.closest('label') as HTMLElement;
    expect(label.className).toMatch(/\bbtn--caret\b/);
    expect(label).toHaveAttribute('title', 'Operation');
    expect(label.querySelector('.sr-only')).toHaveTextContent('Operation');
    expect([...(select as HTMLSelectElement).options].map(o => o.value)).toEqual(['run', 'test', 'build']);
  });
});

describe('SchedulerCard — the run controls', () => {
  it('offers Trigger and no Cancel while nothing runs', () => {
    renderCard(baseSchedule(IDLE));
    expect(triggerButton()).toBeEnabled();
    expect(screen.getByLabelText(/operation/i)).toBeEnabled();
    expect(cancelButton()).toBeNull();
  });

  it('offers Cancel and no Trigger while a run is active', () => {
    renderCard(baseSchedule(ACTIVE));
    expect(cancelButton()).toBeEnabled();
    expect(triggerButton()).toBeNull();
    expect(screen.queryByLabelText(/operation/i)).toBeNull();
  });

  it('Cancel button has .btn.btn--danger', () => {
    renderCard(baseSchedule(ACTIVE));
    const btn = cancelButton() as HTMLElement;
    expect(btn.className).toMatch(/\bbtn\b/);
    expect(btn.className).toMatch(/\bbtn--danger\b/);
  });

  it('keeps the chosen operation for the next trigger after a run', () => {
    const view = render(cardTree(baseSchedule(IDLE)));
    fireEvent.change(screen.getByLabelText(/operation/i), { target: { value: 'test' } });
    view.rerender(cardTree(baseSchedule(ACTIVE)));
    view.rerender(cardTree(baseSchedule(IDLE)));
    expect(triggerButton()).toHaveTextContent('Trigger test');
  });

  it('offers Cancel as soon as a run is active, while Trigger stays until its request has answered and held Triggered', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout'] });
    try {
      let answer!: (r: Response) => void;
      vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
        if (String(input).includes('/trigger')) return new Promise<Response>(resolve => { answer = resolve; });
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ tasks: [] }) } as Response);
      }));
      const view = render(cardTree(baseSchedule(IDLE)));

      fireEvent.click(triggerButton() as HTMLElement);
      expect(triggerButton()).toHaveTextContent('Triggering…');
      expect(triggerButton()).toBeDisabled();
      expect(screen.getByLabelText(/operation/i)).toBeDisabled();
      expect(cancelButton()).toBeNull();

      // The schedule list's poll names the new run before the request answers.
      view.rerender(cardTree(baseSchedule({ ...ACTIVE, last_run_id: 'r2' })));
      expect(triggerButton()).toHaveTextContent('Triggering…');
      expect(cancelButton()).toBeEnabled();

      await act(async () => {
        answer({ ok: true, status: 200, json: () => Promise.resolve({ run_id: 'r2' }) } as Response);
        for (let i = 0; i < 5; i++) await Promise.resolve();
      });
      expect(triggerButton()).toHaveTextContent('Triggered');
      expect(triggerButton()).toBeDisabled();
      expect(screen.getByLabelText(/operation/i)).toBeDisabled();
      expect(cancelButton()).toBeEnabled();

      await act(async () => { vi.advanceTimersByTime(3000); });
      expect(triggerButton()).toBeNull();
      expect(cancelButton()).toBeEnabled();
    } finally {
      vi.useRealTimers();
    }
  });

  it('a refused trigger returns to Trigger and says why in a strip', async () => {
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      if (String(input).includes('/trigger')) {
        return Promise.resolve({ ok: false, status: 409, json: () => Promise.resolve({ error: 'a run is active' }) } as Response);
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ tasks: [] }) } as Response);
    }));
    renderCard(baseSchedule(IDLE));
    fireEvent.click(triggerButton() as HTMLElement);
    const strip = (await screen.findByText('a run is active')).closest('.info-strip');
    expect(strip?.className).toMatch(/\binfo-strip--error\b/);
    expect(triggerButton()).toBeEnabled();
    expect(triggerButton()).toHaveTextContent('Trigger run');
  });

  it('drops a refused trigger\'s error once the card shows a newer run, since Trigger is not there to clear it', async () => {
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      if (String(input).includes('/trigger')) {
        return Promise.resolve({ ok: false, status: 409, json: () => Promise.resolve({ error: 'a run is active' }) } as Response);
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ tasks: [] }) } as Response);
    }));
    const view = render(cardTree(baseSchedule(IDLE)));
    fireEvent.click(triggerButton() as HTMLElement);
    await screen.findByText('a run is active');

    view.rerender(cardTree(baseSchedule({ ...ACTIVE, last_run_id: 'r2' })));
    await waitFor(() => expect(screen.queryByText('a run is active')).toBeNull());
  });

  it('keeps a refused trigger\'s error while the card shows the same run', async () => {
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      if (String(input).includes('/trigger')) {
        return Promise.resolve({ ok: false, status: 500, json: () => Promise.resolve({ error: 'upstream timed out' }) } as Response);
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ tasks: [] }) } as Response);
    }));
    const view = render(cardTree(baseSchedule(IDLE)));
    fireEvent.click(triggerButton() as HTMLElement);
    await screen.findByText('upstream timed out');

    view.rerender(cardTree(baseSchedule(IDLE)));
    expect(screen.getByText('upstream timed out')).toBeInTheDocument();
  });

  it('does not navigate from a refused trigger\'s error, so its message can be read', async () => {
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      if (String(input).includes('/trigger')) {
        return Promise.resolve({ ok: false, status: 409, json: () => Promise.resolve({ error: 'a run is active' }) } as Response);
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ tasks: [] }) } as Response);
    }));
    renderCardWithRouteProbe(baseSchedule(IDLE));
    fireEvent.click(triggerButton() as HTMLElement);
    fireEvent.click(await screen.findByText('a run is active'));
    expect(screen.queryByTestId('schedule-detail')).toBeNull();
  });
});

describe('SchedulerCard — the focus when a run control leaves', () => {
  // The group is no action: a key meant for the control that left starts nothing,
  // where Trigger, which has no confirmation, would start a run.
  const controlsGroup = () => screen.getByRole('group', { name: 'hourly-events controls' });

  it('moves from Trigger to the group of controls', () => {
    const view = render(cardTree(baseSchedule(IDLE)));
    (triggerButton() as HTMLElement).focus();
    view.rerender(cardTree(baseSchedule(ACTIVE)));
    expect(document.activeElement).toBe(controlsGroup());
  });

  it('moves from the operation select to the group of controls', () => {
    const view = render(cardTree(baseSchedule(IDLE)));
    screen.getByLabelText(/operation/i).focus();
    view.rerender(cardTree(baseSchedule(ACTIVE)));
    expect(document.activeElement).toBe(controlsGroup());
  });

  it('moves from Cancel to the group of controls, never to Trigger', () => {
    const view = render(cardTree(baseSchedule(ACTIVE)));
    (cancelButton() as HTMLElement).focus();
    view.rerender(cardTree(baseSchedule(IDLE)));
    expect(document.activeElement).toBe(controlsGroup());
    expect(document.activeElement).not.toBe(triggerButton());
  });

  it('is not taken back when it had already left the control that goes', () => {
    const view = render(cardTree(baseSchedule(ACTIVE)));
    const cancel = cancelButton() as HTMLElement;
    cancel.focus();
    cancel.blur();
    view.rerender(cardTree(baseSchedule(IDLE)));
    expect(document.activeElement).toBe(document.body);
  });

  it('stays on another control of the card', () => {
    const view = render(cardTree(baseSchedule(IDLE), OPERATOR));
    const edit = screen.getByRole('button', { name: 'Edit cron' });
    edit.focus();
    view.rerender(cardTree(baseSchedule(ACTIVE), OPERATOR));
    expect(document.activeElement).toBe(edit);
  });

  it('is not taken when it was never on a control of the card', () => {
    const view = render(cardTree(baseSchedule(IDLE)));
    view.rerender(cardTree(baseSchedule(ACTIVE)));
    expect(document.activeElement).toBe(document.body);
  });
});

describe('SchedulerCard — a cancelling run', () => {
  it('reads cancelling, counts as active and keeps Cancel available', () => {
    const { container } = renderCard(baseSchedule({ ...ACTIVE, last_run_status: 'cancelling' }));
    const pill = runStatus(container);
    expect(pill).toHaveTextContent('cancelling');
    expect(pill.className).toMatch(/\bpill-sm--cancelling\b/);
    expect(cancelButton()).toBeEnabled();
    expect(triggerButton()).toBeNull();
  });

  it('counts stopping tasks', async () => {
    installFetch({ tasks: tasksBody('stopping', 'stopping', 'running', 'succeeded') });
    const { container } = renderCard(baseSchedule({ ...ACTIVE, last_run_status: 'cancelling' }));
    await waitFor(() => expect(shownCounts(container)).toContain('2 stopping'));
  });
});

describe('SchedulerCard — drift chip', () => {
  it('renders the stale chip beside the status when /api/runs/:id/graph reports stale (in-flight run)', async () => {
    installFetch({ graph: { run_promotion_seq: 5, latest_promotion_seq: 7 } });
    const { container } = renderCard(baseSchedule({ has_active_run: true, last_run_id: 'run-1' }));
    const chip = await screen.findByText(/source 2 releases behind latest/);
    expect(chip.className).toMatch(/\binfo-strip--warning\b/);
    expect(chip.className).toMatch(/\binfo-strip--inline\b/);
    expect(chip.closest('.scheduler-card-header')).toBe(container.querySelector('.scheduler-card-header'));
    expect(chip.querySelector('.info-strip__icon')).toHaveAttribute('aria-hidden', 'true');
  });

  it('renders the stale chip when the last finalised run is stale', async () => {
    installFetch({ graph: { run_promotion_seq: 3, latest_promotion_seq: 10 } });
    renderCard(baseSchedule({ has_active_run: false, last_run_status: 'succeeded', last_run_id: 'run-final' }));
    const chip = await screen.findByText(/source 7 releases behind latest/);
    expect(chip.closest('.info-strip--warning')).toBeInTheDocument();
  });

  it('does NOT render a chip when run_promotion_seq is 0 (unknown, pre-tracking run)', async () => {
    const handler = installFetch({ graph: { run_promotion_seq: 0, latest_promotion_seq: 7 } });
    renderCard(baseSchedule({ has_active_run: true, last_run_id: 'run-1' }));
    await waitFor(() => {
      expect(handler).toHaveBeenCalled();
    });
    expect(screen.queryByText(/topology version unknown/i)).toBeNull();
    expect(screen.queryByText(/behind latest/i)).toBeNull();
  });

  it('does NOT render the chip when drift is fresh', async () => {
    const handler = installFetch({ graph: { run_promotion_seq: 7, latest_promotion_seq: 7 } });
    renderCard(baseSchedule({ has_active_run: true, last_run_id: 'run-1' }));
    await waitFor(() => {
      expect(handler).toHaveBeenCalled();
    });
    expect(screen.queryByText(/behind latest/i)).toBeNull();
    expect(screen.queryByText(/topology version unknown/i)).toBeNull();
  });

  it('does NOT fetch graph or render a chip for a never-run schedule', async () => {
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

describe('SchedulerCard — the facts line', () => {
  it('reads a scheduled schedule as the cron in words with the raw cron on hover, its timezone and the next fire in that timezone', () => {
    const { container } = renderCard(baseSchedule({
      ...IDLE,
      cron: '0 1 * * *',
      timezone: 'Europe/Paris',
      next_fire_at: '2026-10-28T00:00:00.000Z',
      cron_words: 'At 01:00',
    }));
    const cron = container.querySelector('.schedule-cron')!;
    expect(cron).toHaveTextContent('At 01:00');
    expect(cron).toHaveAttribute('title', '0 1 * * *');
    expect(cron.className).not.toMatch(/\bschedule-cron--raw\b/);
    expect(cron.className).not.toMatch(/\bschedule-cron--paused\b/);
    expect(screen.getByText('Europe/Paris')).toBeInTheDocument();
    expect(screen.getByText('next 28 Oct 01:00')).toBeInTheDocument();
    expect(container.querySelector('.pill-sm--paused')).toBeNull();
  });

  it('shows the raw cron in monospace when the server has no words for it', () => {
    const { container } = renderCard(baseSchedule({ ...IDLE, cron: '0 9 * * */9', cron_words: '' }));
    const cron = container.querySelector('.schedule-cron')!;
    expect(cron).toHaveTextContent('0 9 * * */9');
    expect(cron.className).toMatch(/\bschedule-cron--raw\b/);
    expect(cron).not.toHaveAttribute('title');
  });

  it('marks a paused schedule with its one pill, the cron struck through and who paused it', () => {
    const { container } = renderCard(baseSchedule({
      ...IDLE,
      cron: '0 * * * *',
      paused: true,
      next_fire_at: null,
      updated_by: 'bob@example.com',
      updated_at: '2026-10-27T13:02:00.000Z',
    }));
    const pill = container.querySelector('.pill-sm--paused')!;
    expect(pill).toHaveTextContent('paused');
    // It wraps with the facts rather than squeezing them into a column beside it.
    expect(pill.parentElement).toBe(container.querySelector('.schedule-facts'));
    expect(container.querySelector('.schedule-cron')!.className).toMatch(/\bschedule-cron--paused\b/);
    expect(screen.getByText(/^paused by bob@example\.com · 27 Oct \d\d:\d\d$/)).toBeInTheDocument();
    expect(screen.queryByText(/^next /)).toBeNull();
  });

  it('says a schedule with no cron runs only when triggered, with no pill or cron, and offers an operator Add cron', () => {
    const { container } = renderCard(baseSchedule({
      ...IDLE, cron: '', timezone: '', paused: false, version: 0, updated_by: '', updated_at: null, cron_words: '',
    }), OPERATOR);
    expect(container.querySelector('.pill-sm--paused')).toBeNull();
    expect(container.querySelector('.schedule-cron')).toBeNull();
    expect(screen.getByText('No cron: runs only when triggered')).toBeInTheDocument();
    const add = screen.getByRole('button', { name: 'Add cron' });
    expect(add.className).toMatch(/\bbtn--secondary\b/);
    expect(screen.queryByRole('button', { name: 'Edit cron' })).toBeNull();
  });

  it('says how long ago the last run started, with the exact time on hover', () => {
    const iso = new Date(Date.now() - 18 * 60 * 1000).toISOString();
    renderCard(baseSchedule({ ...IDLE, last_run_at: iso }));
    const fact = screen.getByText('started 18m ago');
    expect(fact).toHaveAttribute('title', new Date(iso).toLocaleString());
  });

  it('says a run started under a minute ago started just now', () => {
    renderCard(baseSchedule({ ...ACTIVE, last_run_at: new Date(Date.now() - 5000).toISOString() }));
    expect(screen.getByText('started just now')).toBeInTheDocument();
  });

  it('leaves the fact out when the start time is not known, and for a schedule that never ran', () => {
    renderCard(baseSchedule({ ...IDLE, last_run_at: null }));
    expect(screen.queryByText(/^started/)).toBeNull();
    cleanup();
    renderCard(baseSchedule({ has_active_run: false, last_run_status: '', last_run_id: null, last_run_at: null }));
    expect(screen.queryByText(/^started/)).toBeNull();
  });

  it('separates the facts with one separator between each', () => {
    const { container } = renderCard(baseSchedule({
      ...IDLE,
      timezone: 'UTC',
      next_fire_at: '2026-10-28T00:00:00.000Z',
      last_run_at: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
    }));
    const line = container.querySelector('.schedule-facts')!;
    expect(line.textContent).toBe('Every hour·UTC·next 28 Oct 00:00·started 1h ago');
    const separators = [...line.querySelectorAll('.preview-facts__sep')];
    expect(separators).toHaveLength(3);
    // A screen reader reads the facts, not a dot between each.
    for (const sep of separators) expect(sep).toHaveAttribute('aria-hidden', 'true');
  });
});

describe('SchedulerCard — cron controls and warning', () => {
  it('SchedulerCard shows Edit cron to an operator and no button to a viewer', () => {
    const scheduled = baseSchedule(IDLE);
    renderCard(scheduled, OPERATOR);
    const edit = screen.getByRole('button', { name: 'Edit cron' });
    expect(edit.className).toMatch(/\bbtn\b/);
    expect(edit.className).toMatch(/\bbtn--secondary\b/);
    cleanup();

    renderCard(scheduled, VIEWER);
    expect(screen.queryByRole('button', { name: 'Edit cron' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Add cron' })).toBeNull();
    cleanup();

    renderCard(baseSchedule({ ...IDLE, cron: '' }), VIEWER);
    expect(screen.queryByRole('button', { name: 'Add cron' })).toBeNull();
  });

  it('SchedulerCard shows the warning strip on a scheduled name with no models', () => {
    const { container } = renderCard(baseSchedule({
      ...IDLE, schedule_name: 'weekly', cron: '0 3 * * 1', model_count: 0,
    }));
    const strip = container.querySelector('.info-strip--warning')!;
    expect(strip).not.toBeNull();
    expect(strip).toHaveTextContent(
      'No model in the live release has the tag weekly. Each fire records a failed run until a release brings them back, or you remove its cron.',
    );
    expect(strip.querySelector('b')).toHaveTextContent('weekly');
    expect(strip.querySelector('.info-strip__icon')).toHaveAttribute('aria-hidden', 'true');
  });

  it('SchedulerCard shows no model warning on a paused or a manual name with no models, nor on a scheduled one with models', () => {
    const cases = [
      baseSchedule({ cron: '0 3 * * 1', paused: true, model_count: 0 }),
      baseSchedule({ cron: '', model_count: 0 }),
      baseSchedule({ cron: '0 3 * * 1', model_count: 4 }),
    ];
    for (const schedule of cases) {
      renderCard({ ...schedule, ...IDLE });
      expect(screen.queryByText(/No model in the live release has the tag/)).toBeNull();
      cleanup();
    }
  });

  it('SchedulerCard Edit cron opens the dialog without navigating the card', async () => {
    renderCardWithRouteProbe(baseSchedule(IDLE), OPERATOR);

    fireEvent.click(screen.getByRole('button', { name: 'Edit cron' }));
    expect(await screen.findByRole('heading', { name: 'Edit cron · hourly-events' })).toBeInTheDocument();
    // Clicking inside the dialog does not reach the card either.
    fireEvent.click(screen.getByLabelText('Description'));
    await Promise.resolve();
    expect(screen.queryByTestId('schedule-detail')).toBeNull();
  });

  it('SchedulerCard keeps the dialog it opened when a poll adds a cron meanwhile, and reads the list again on close', async () => {
    const onRunChanged = vi.fn();
    const manualSchedule = baseSchedule({ ...IDLE, cron: '', version: 0 });
    const view = render(cardTree(manualSchedule, OPERATOR, onRunChanged));
    fireEvent.click(screen.getByRole('button', { name: 'Add cron' }));
    expect(await screen.findByRole('heading', { name: 'Add cron · hourly-events' })).toBeInTheDocument();

    view.rerender(cardTree(baseSchedule({ ...IDLE, cron: '0 2 * * *', version: 1 }), OPERATOR, onRunChanged));
    expect(screen.getByRole('heading', { name: 'Add cron · hourly-events' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Remove cron' })).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(onRunChanged).toHaveBeenCalledTimes(1);
  });
});

describe('SchedulerCard — task counts', () => {
  it('counts every status with tasks once, in the run page order, and the counts add up to the total', async () => {
    installFetch({ tasks: tasksBody(
      'succeeded', 'succeeded', 'failed', 'skipped', 'cancelled', 'cancelled',
      'dispatched', 'queued', 'queued', 'stopping', 'running', 'pending', 'pending', 'pending',
    ) });
    const { container } = renderCard(baseSchedule({ has_active_run: true, last_run_id: 'r1' }));

    await waitFor(() => expect(shownCounts(container)).toHaveLength(9));
    expect(shownCounts(container)).toEqual([
      '2 succeeded', '1 dispatched', '2 queued', '1 stopping', '1 running',
      '1 failed', '1 skipped', '2 cancelled', '3 pending',
    ]);
  });

  it('leaves out a status with no tasks', async () => {
    installFetch({ tasks: tasksBody(...repeat('succeeded', 28), 'queued', ...repeat('running', 6), ...repeat('pending', 6)) });
    const { container } = renderCard(baseSchedule({ has_active_run: true, last_run_id: 'r1' }));
    await waitFor(() => expect(shownCounts(container)).toEqual([
      '28 succeeded', '1 queued', '6 running', '6 pending',
    ]));
  });

  it('paints no emoji', async () => {
    installFetch({
      graph: { run_promotion_seq: 5, latest_promotion_seq: 7 },
      tasks: tasksBody('succeeded', 'failed', 'skipped', 'cancelled', 'dispatched', 'queued', 'stopping', 'running', 'pending'),
    });
    const { container } = renderCard(baseSchedule({ has_active_run: true, last_run_id: 'r1', model_count: 0 }), OPERATOR);
    await waitFor(() => expect(shownCounts(container)).toHaveLength(9));
    await screen.findByText(/behind latest/);
    expect(container.textContent).not.toMatch(/\p{Emoji_Presentation}|\uFE0F/u);
  });
});

describe('SchedulerCard — the progress line', () => {
  it('reads nodes complete and the percentage while the run is active', async () => {
    installFetch({ tasks: tasksBody(...repeat('succeeded', 28), ...repeat('running', 13)) });
    const { container } = renderCard(baseSchedule(ACTIVE));
    const line = await screen.findByText('28 of 41 nodes complete');
    expect(line.closest('.schedule-facts')!.textContent).toBe('28 of 41 nodes complete·68%');
    expect(container.querySelector('.run-progress-bar')).not.toBeNull();
  });

  it('reads the task total alone once the run has ended, so a failed run never reads 100%', async () => {
    installFetch({ tasks: tasksBody('succeeded', 'failed', 'skipped') });
    const { container } = renderCard(baseSchedule({ ...IDLE, last_run_status: 'failed' }));
    expect(await screen.findByText('3 nodes')).toBeInTheDocument();
    expect(container.textContent).not.toMatch(/%/);
    expect(screen.queryByText(/nodes complete/)).toBeNull();
  });

  it('groups the thousands of a large run', async () => {
    installFetch({ tasks: tasksBody(...repeat('succeeded', 1200), ...repeat('running', 800)) });
    renderCard(baseSchedule(ACTIVE));
    expect(await screen.findByText('1,200 of 2,000 nodes complete')).toBeInTheDocument();
    cleanup();

    installFetch({ tasks: tasksBody(...repeat('succeeded', 2000)) });
    renderCard(baseSchedule(IDLE));
    expect(await screen.findByText('2,000 nodes')).toBeInTheDocument();
  });

  it('never reads 100% while a task is unfinished', async () => {
    installFetch({ tasks: tasksBody(...repeat('succeeded', 1999), 'running') });
    const { container } = renderCard(baseSchedule(ACTIVE));
    const line = await screen.findByText('1,999 of 2,000 nodes complete');
    expect(line.closest('.schedule-facts')!.textContent).toBe('1,999 of 2,000 nodes complete·99%');
    expect(container.textContent).not.toMatch(/100%/);
  });

  it('says 1 node for a run of one task', async () => {
    installFetch({ tasks: tasksBody('succeeded') });
    renderCard(baseSchedule(IDLE));
    expect(await screen.findByText('1 node')).toBeInTheDocument();
  });

  it('says 0 of 1 node complete for an active run of one task', async () => {
    installFetch({ tasks: tasksBody('running') });
    renderCard(baseSchedule(ACTIVE));
    expect(await screen.findByText('0 of 1 node complete')).toBeInTheDocument();
  });

  it('draws one segment per status with tasks', async () => {
    installFetch({ tasks: tasksBody(...repeat('succeeded', 12), 'failed', ...repeat('skipped', 28)) });
    const { container } = renderCard(baseSchedule({ ...IDLE, last_run_status: 'failed' }));
    await screen.findByText('41 nodes');
    const segments = [...container.querySelectorAll('.rp-seg')].map(s => s.className);
    expect(segments).toEqual(['rp-seg rp-succeeded', 'rp-seg rp-failed', 'rp-seg rp-skipped']);
  });
});

describe('SchedulerCard — before the tasks are read', () => {
  it('holds an empty track before the first read answers, with no counts or total', () => {
    vi.stubGlobal('fetch', vi.fn(() => new Promise<Response>(() => {})));
    const { container } = renderCard(baseSchedule(IDLE));
    expect(container.querySelector('.scheduler-card-body')).not.toBeNull();
    expect(container.querySelector('.run-progress-bar')).not.toBeNull();
    expect(container.querySelector('.rp-seg')).toBeNull();
    expect(container.querySelector('.run-progress-counts')).toBeNull();
  });

  it('has no body once a run with no tasks has answered', async () => {
    const handler = installFetch({ tasks: tasksBody() });
    const { container } = renderCard(baseSchedule({ ...IDLE, last_run_status: 'failed' }));
    await waitFor(() => expect(handler).toHaveBeenCalled());
    await waitFor(() => expect(container.querySelector('.scheduler-card-body')).toBeNull());
  });

  it('has no body after the first read fails, and fills in when a later read answers', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    try {
      let fail = true;
      vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
        if (String(input).includes('/tasks') && fail) return Promise.reject(new Error('down'));
        const body = String(input).includes('/tasks') ? tasksBody('succeeded') : {};
        return Promise.resolve({ ok: true, json: () => Promise.resolve(body) } as Response);
      }));
      const { container } = renderCard(baseSchedule(IDLE));
      await act(async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); });
      expect(container.querySelector('.scheduler-card-body')).toBeNull();

      fail = false;
      await act(async () => {
        vi.advanceTimersByTime(5000);
        for (let i = 0; i < 20; i++) await Promise.resolve();
      });
      expect(shownCounts(container)).toEqual(['1 succeeded']);
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps the counts it has when a later read of the same run fails', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    try {
      let fail = false;
      vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
        if (String(input).includes('/tasks') && fail) return Promise.reject(new Error('down'));
        const body = String(input).includes('/tasks') ? tasksBody('succeeded', 'running') : {};
        return Promise.resolve({ ok: true, json: () => Promise.resolve(body) } as Response);
      }));
      const { container } = renderCard(baseSchedule(ACTIVE));
      await act(async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); });
      expect(shownCounts(container)).toEqual(['1 succeeded', '1 running']);

      fail = true;
      await act(async () => {
        vi.advanceTimersByTime(5000);
        for (let i = 0; i < 20; i++) await Promise.resolve();
      });
      expect(shownCounts(container)).toEqual(['1 succeeded', '1 running']);
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not show the previous run\'s counts under a new run', async () => {
    let newRunAnswer!: (r: Response) => void;
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes('/runs/r1/tasks')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve(tasksBody(...repeat('succeeded', 41))) } as Response);
      }
      if (url.includes('/runs/r2/tasks')) return new Promise<Response>(resolve => { newRunAnswer = resolve; });
      return Promise.resolve({ ok: true, json: () => Promise.resolve({}) } as Response);
    }));
    const view = render(cardTree(baseSchedule(IDLE)));
    await screen.findByText('41 nodes');

    view.rerender(cardTree(baseSchedule({ ...ACTIVE, last_run_id: 'r2' })));
    expect(screen.queryByText(/41/)).toBeNull();
    expect(view.container.querySelector('.rp-seg')).toBeNull();
    expect(view.container.querySelector('.run-progress-bar')).not.toBeNull();

    await act(async () => {
      newRunAnswer({ ok: true, json: () => Promise.resolve(tasksBody('running', 'pending')) } as Response);
    });
    expect(await screen.findByText('0 of 2 nodes complete')).toBeInTheDocument();
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
    view.rerender(cardTree(baseSchedule({ has_active_run: false, last_run_id: 'r2' })));
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
