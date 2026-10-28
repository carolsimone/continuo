// @vitest-environment jsdom
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, act, cleanup } from '@testing-library/react';
import CronDialog from '../../src/client/CronDialog';
import type { ScheduleSummary } from '../../src/client/types';

function daily(overrides: Partial<ScheduleSummary> = {}): ScheduleSummary {
  return {
    schedule_name: 'daily',
    cron: '0 1 * * *',
    timezone: 'Europe/Paris',
    description: 'Runs daily at 1am CET',
    paused: false,
    next_fire_at: '2026-10-28T00:00:00.000Z',
    version: 3,
    updated_by: 'alice@example.com',
    updated_at: '2026-10-21T12:02:00.000Z',
    model_count: 41,
    cron_words: 'every day at 01:00',
    has_active_run: false,
    last_run_at: null,
    last_run_status: 'succeeded',
    last_run_id: 'r1',
    ...overrides,
  };
}

function manual(overrides: Partial<ScheduleSummary> = {}): ScheduleSummary {
  return daily({
    schedule_name: 'e2e-schedule', cron: '', timezone: '', description: '', paused: false,
    next_fire_at: null, version: 0, updated_by: '', updated_at: null, model_count: 5, cron_words: '',
    ...overrides,
  });
}

interface Reply { status: number; body?: unknown }
interface Call { url: string; method: string; body: any }

const DAILY_PREVIEW: Reply = {
  status: 200,
  body: {
    cron_words: 'every day at 01:00',
    next_fire_times: ['2026-10-28T00:00:00.000Z', '2026-10-29T00:00:00.000Z', '2026-10-30T00:00:00.000Z'],
  },
};

function okPreview(words: string): Reply {
  return { status: 200, body: { cron_words: words, next_fire_times: ['2026-10-28T05:30:00.000Z'] } };
}

// installFetch answers the dialog's three routes and records every call. A
// handler may return a promise, which stands for a slow answer; the answer is
// delivered even after the dialog has aborted the request, as a server that
// has already replied would.
function installFetch(handlers: {
  preview?: (cron: string, timezone: string) => Reply | Promise<Reply>;
  put?: (body: any) => Reply | Promise<Reply>;
  del?: () => Reply | Promise<Reply>;
} = {}): Call[] {
  const calls: Call[] = [];
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method ?? 'GET';
    const body = init?.body ? JSON.parse(String(init.body)) : undefined;
    calls.push({ url, method, body });
    let reply: Reply | Promise<Reply>;
    if (url.startsWith('/api/schedules/cron-preview')) {
      const q = new URL(url, 'http://ui').searchParams;
      reply = handlers.preview ? handlers.preview(q.get('cron') ?? '', q.get('timezone') ?? '') : DAILY_PREVIEW;
    } else if (method === 'PUT') {
      reply = handlers.put ? handlers.put(body) : { status: 200, body: {} };
    } else if (method === 'DELETE') {
      reply = handlers.del ? handlers.del() : { status: 204 };
    } else {
      reply = { status: 404, body: { error: 'unexpected call' } };
    }
    return Promise.resolve(reply).then(r => ({
      ok: r.status >= 200 && r.status < 300,
      status: r.status,
      json: () => Promise.resolve(r.body ?? {}),
    }) as Response);
  }));
  return calls;
}

const previews = (calls: Call[]) => calls.filter(c => c.url.startsWith('/api/schedules/cron-preview'));
const previewCron = (call: Call) => new URL(call.url, 'http://ui').searchParams.get('cron');

// advance moves the fake clock and lets every answer that is due settle.
async function advance(ms = 0) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
    for (let i = 0; i < 10; i++) await Promise.resolve();
  });
}

function typeCron(value: string) {
  fireEvent.change(screen.getByLabelText('Cron'), { target: { value } });
}

const saveButton = () => screen.getByRole('button', { name: /^(Save|Saving…|Saved)$/ });

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('CronDialog', () => {
  it('CronDialog edit shows the words line and three next runs from the preview', async () => {
    const calls = installFetch();
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();

    expect(screen.getByRole('heading', { name: 'Edit cron · daily' })).toBeInTheDocument();
    expect(screen.getByText(/Last changed by alice@example\.com/).closest('.info-strip--neutral')).not.toBeNull();
    expect(screen.getByLabelText('Cron')).toHaveValue('0 1 * * *');
    expect(screen.getByLabelText('Timezone')).toHaveValue('Europe/Paris');
    expect(screen.getByLabelText('Description')).toHaveValue('Runs daily at 1am CET');
    expect(screen.getByText('every day at 01:00')).toBeInTheDocument();

    const rows = Array.from(document.querySelectorAll('.preview-list > li'), li => li.textContent);
    expect(rows).toEqual(['Wed 28 Oct 202601:00', 'Thu 29 Oct 202601:00', 'Fri 30 Oct 202601:00']);
    expect(screen.getByText('Next runs').closest('.section-header')?.querySelector('.section-header__count'))
      .toHaveTextContent('3');

    expect(previews(calls)).toHaveLength(1);
    const q = new URL(previews(calls)[0].url, 'http://ui').searchParams;
    expect(q.get('cron')).toBe('0 1 * * *');
    expect(q.get('timezone')).toBe('Europe/Paris');
    expect(screen.getByRole('checkbox', { name: /Paused/ })).not.toBeChecked();
    expect(screen.getByRole('button', { name: 'Remove cron' })).toBeInTheDocument();
    expect(saveButton()).toBeEnabled();
  });

  it('CronDialog add hides Paused and Remove cron', async () => {
    const calls = installFetch();
    render(<CronDialog schedule={manual()} mode="add" onClose={() => {}} />);
    await advance(1000);

    expect(screen.getByRole('heading', { name: 'Add cron · e2e-schedule' })).toBeInTheDocument();
    expect(screen.getByText(/runs only when triggered\. A cron makes it also run on its own\./)).toBeInTheDocument();
    expect(screen.queryByRole('checkbox', { name: /Paused/ })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Remove cron' })).toBeNull();
    expect(screen.queryByText(/Last changed by/)).toBeNull();
    // An empty cron is not previewed, and there is nothing to add yet.
    expect(previews(calls)).toHaveLength(0);
    expect(screen.getByRole('button', { name: 'Add cron' })).toBeDisabled();
  });

  it('CronDialog add defaults the timezone to the browser zone and saves paused false at version 0', async () => {
    const browserZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const calls = installFetch({ preview: () => okPreview('at 06:30, Monday through Friday') });
    const onClose = vi.fn();
    render(<CronDialog schedule={manual()} mode="add" onClose={onClose} />);

    expect(screen.getByLabelText('Timezone')).toHaveValue(browserZone);
    typeCron('30 6 * * 1-5');
    await advance(300);
    expect(screen.getByText('at 06:30, Monday through Friday')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Add cron' }));
    await advance();

    const put = calls.find(c => c.method === 'PUT')!;
    expect(put.url).toBe('/api/schedules/e2e-schedule/cron');
    expect(put.body).toEqual({
      cron: '30 6 * * 1-5', timezone: browserZone, description: '', paused: false, expected_version: 0,
    });
    expect(screen.getByRole('button', { name: 'Added' })).toBeInTheDocument();
    await advance(1000);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("CronDialog an invalid cron disables Save and shows the parser's message", async () => {
    const parserMessage = 'cron "0 25 * * *": end of range (25) above maximum (23): 25';
    installFetch({
      preview: cron => cron === '0 25 * * *' ? { status: 400, body: { error: parserMessage } } : DAILY_PREVIEW,
    });
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();
    expect(saveButton()).toBeEnabled();

    typeCron('0 25 * * *');
    await advance(300);

    const strip = screen.getByText(parserMessage).closest('.info-strip');
    expect(strip).not.toBeNull();
    expect(strip!.className).toMatch(/\binfo-strip--error\b/);
    expect(saveButton()).toBeDisabled();
    expect(screen.getByLabelText('Cron')).toHaveAttribute('aria-invalid', 'true');
    expect(screen.queryByText('every day at 01:00')).toBeNull();
    expect(screen.queryByText('Next runs')).toBeNull();
  });

  it('CronDialog a stale version shows who changed it', async () => {
    const calls = installFetch({
      put: () => ({
        status: 409,
        body: { error: 'schedule was changed by another edit', code: 'stale_version', updated_by: 'bob@example.com' },
      }),
    });
    const onClose = vi.fn();
    render(<CronDialog schedule={daily()} mode="edit" onClose={onClose} />);
    await advance();

    fireEvent.click(saveButton());
    await advance();

    expect(calls.find(c => c.method === 'PUT')!.body).toMatchObject({ expected_version: 3 });
    const strip = screen.getByText(
      'bob@example.com changed this schedule while you were editing. Close and open it again.',
    ).closest('.info-strip');
    expect(strip!.className).toMatch(/\binfo-strip--warning\b/);
    expect(saveButton()).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Remove cron' })).toBeDisabled();
    expect(onClose).not.toHaveBeenCalled();
  });

  it('CronDialog a stale version names nobody when the cron was removed meanwhile', async () => {
    installFetch({
      put: () => ({ status: 409, body: { error: 'schedule was changed by another edit', code: 'stale_version', updated_by: null } }),
    });
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();

    fireEvent.click(saveButton());
    await advance();

    expect(screen.getByText('This schedule was changed while you were editing. Close and open it again.'))
      .toBeInTheDocument();
  });

  it('CronDialog fetches the preview 300 ms after the last keystroke', async () => {
    const calls = installFetch();
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();
    expect(previews(calls)).toHaveLength(1);

    typeCron('0 2 * * *');
    await advance(200);
    typeCron('0 3 * * *');
    await advance(299);
    expect(previews(calls)).toHaveLength(1);

    await advance(1);
    expect(previews(calls)).toHaveLength(2);
    expect(previewCron(previews(calls)[1])).toBe('0 3 * * *');
  });

  it('CronDialog a slow preview of an older cron does not replace the newer one', async () => {
    let answerOlder: (r: Reply) => void = () => {};
    installFetch({
      preview: cron => {
        if (cron === '0 2 * * *') return new Promise<Reply>(resolve => { answerOlder = resolve; });
        if (cron === '0 3 * * *') return okPreview('every day at 03:00');
        return DAILY_PREVIEW;
      },
    });
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();

    typeCron('0 2 * * *');
    await advance(300);
    typeCron('0 3 * * *');
    await advance(300);
    expect(screen.getByText('every day at 03:00')).toBeInTheDocument();

    answerOlder(okPreview('every day at 02:00'));
    await advance();

    expect(screen.getByText('every day at 03:00')).toBeInTheDocument();
    expect(screen.queryByText('every day at 02:00')).toBeNull();
    expect(saveButton()).toBeEnabled();
  });

  it('CronDialog Save waits for the preview of the cron as typed, then saves it and closes', async () => {
    const calls = installFetch({
      preview: cron => cron === '0 4 * * *' ? okPreview('every day at 04:00') : DAILY_PREVIEW,
    });
    const onClose = vi.fn();
    render(<CronDialog schedule={daily()} mode="edit" onClose={onClose} />);
    await advance();
    expect(saveButton()).toBeEnabled();

    typeCron('0 4 * * *');
    // The preview of the new cron is not back yet: what Save would send has
    // not been read by the parser that fires it.
    expect(saveButton()).toBeDisabled();
    await advance(300);
    expect(saveButton()).toBeEnabled();

    fireEvent.click(saveButton());
    await advance();
    expect(calls.find(c => c.method === 'PUT')!.body).toEqual({
      cron: '0 4 * * *', timezone: 'Europe/Paris', description: 'Runs daily at 1am CET', paused: false, expected_version: 3,
    });
    expect(saveButton()).toHaveTextContent('Saved');
    expect(saveButton().className).toMatch(/\bis-success\b/);
    await advance(1000);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('CronDialog saves with the version it opened with, whatever a later poll brings', async () => {
    const calls = installFetch();
    const view = render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();

    view.rerender(<CronDialog schedule={daily({ version: 4, updated_by: 'bob@example.com' })} mode="edit" onClose={() => {}} />);
    fireEvent.click(saveButton());
    await advance();

    expect(calls.find(c => c.method === 'PUT')!.body).toMatchObject({ expected_version: 3 });
    expect(screen.getByText(/Last changed by alice@example\.com/)).toBeInTheDocument();
  });

  it('CronDialog Paused saves paused and lists no next runs', async () => {
    const calls = installFetch();
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();

    fireEvent.click(screen.getByRole('checkbox', { name: /Paused/ }));
    expect(screen.getByText('None while paused')).toBeInTheDocument();
    expect(document.querySelector('.preview-list')).toBeNull();
    fireEvent.click(saveButton());
    await advance();

    expect(calls.find(c => c.method === 'PUT')!.body).toMatchObject({ paused: true });
  });

  it('CronDialog Remove cron deletes the cron with no second confirmation', async () => {
    const confirm = vi.fn(() => true);
    vi.stubGlobal('confirm', confirm);
    const calls = installFetch();
    const onClose = vi.fn();
    render(<CronDialog schedule={daily()} mode="edit" onClose={onClose} />);
    await advance();

    const remove = screen.getByRole('button', { name: 'Remove cron' });
    expect(remove.className).toMatch(/\bbtn--danger\b/);
    expect(remove.closest('.dialog-actions')).toBeNull();
    expect(remove.closest('.dialog-remove-row')).toHaveTextContent('daily then runs only when triggered.');
    fireEvent.click(remove);
    await advance();

    const deletes = calls.filter(c => c.method === 'DELETE');
    expect(deletes).toHaveLength(1);
    expect(deletes[0].url).toBe('/api/schedules/daily/cron');
    expect(confirm).not.toHaveBeenCalled();
    await advance(1000);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('CronDialog shows a refused save in an error strip and keeps the dialog open', async () => {
    installFetch({ put: () => ({ status: 404, body: { error: 'no model of the live release belongs to this schedule' } }) });
    const onClose = vi.fn();
    render(<CronDialog schedule={daily()} mode="edit" onClose={onClose} />);
    await advance();

    fireEvent.click(saveButton());
    await advance();

    expect(screen.getByText('no model of the live release belongs to this schedule').closest('.info-strip--error'))
      .not.toBeNull();
    expect(saveButton()).toBeEnabled();
    expect(onClose).not.toHaveBeenCalled();
  });

  it('CronDialog lets Escape and a click on the overlay close it, and neither while a request is in flight', async () => {
    let answer: (r: Reply) => void = () => {};
    installFetch({ put: () => new Promise<Reply>(resolve => { answer = resolve; }) });
    const onClose = vi.fn();
    render(<CronDialog schedule={daily()} mode="edit" onClose={onClose} />);
    await advance();

    fireEvent.keyDown(document, { key: 'Enter' });
    expect(onClose).not.toHaveBeenCalled();
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(onClose).toHaveBeenCalledTimes(1);

    onClose.mockClear();
    const overlay = document.querySelector('.dialog-overlay')!;
    fireEvent.click(screen.getByRole('dialog'));
    expect(onClose).not.toHaveBeenCalled();
    fireEvent.click(overlay);
    expect(onClose).toHaveBeenCalledTimes(1);

    onClose.mockClear();
    fireEvent.click(saveButton());
    await advance();
    expect(saveButton()).toHaveTextContent('Saving…');
    fireEvent.keyDown(document, { key: 'Escape' });
    fireEvent.click(overlay);
    expect(onClose).not.toHaveBeenCalled();

    // A refused save ends the request, and the dialog closes again.
    answer({ status: 500, body: { error: 'state down' } });
    await advance();
    expect(screen.getByText('state down')).toBeInTheDocument();
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(onClose).toHaveBeenCalledTimes(1);
    fireEvent.click(overlay);
    expect(onClose).toHaveBeenCalledTimes(2);
  });

  it('CronDialog previews again when the timezone changes, and lists the next runs in it', async () => {
    const calls = installFetch();
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();
    expect(previews(calls)).toHaveLength(1);
    expect(Array.from(document.querySelectorAll('.preview-list > li'), li => li.textContent)[0])
      .toBe('Wed 28 Oct 202601:00');

    fireEvent.change(screen.getByLabelText('Timezone'), { target: { value: 'UTC' } });
    // What Save would send has not been read in the new zone yet.
    expect(saveButton()).toBeDisabled();
    await advance(299);
    expect(previews(calls)).toHaveLength(1);
    await advance(1);

    expect(previews(calls)).toHaveLength(2);
    const q = new URL(previews(calls)[1].url, 'http://ui').searchParams;
    expect(q.get('cron')).toBe('0 1 * * *');
    expect(q.get('timezone')).toBe('UTC');
    expect(Array.from(document.querySelectorAll('.preview-list > li'), li => li.textContent)[0])
      .toBe('Wed 28 Oct 202600:00');
    expect(saveButton()).toBeEnabled();
  });

  it('CronDialog a double click on Save sends one request', async () => {
    const calls = installFetch({ put: () => new Promise<Reply>(() => {}) });
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();

    const save = saveButton();
    fireEvent.click(save);
    fireEvent.click(save);
    await advance();

    expect(calls.filter(c => c.method === 'PUT')).toHaveLength(1);
    expect(save).toBeDisabled();
  });

  it('CronDialog announces the words line and the next runs as polite live regions', async () => {
    installFetch();
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);

    // The words line is in the page before the preview fills it, so the
    // answer is announced rather than inserted silently.
    const words = document.querySelector('.dialog-field__words')!;
    expect(words).toHaveAttribute('aria-live', 'polite');
    expect(words).toBeEmptyDOMElement();
    // So is the region the next runs fill: a live region inserted together
    // with its content is not reliably announced.
    const nextRuns = document.querySelector('.dialog-live')!;
    expect(nextRuns).toHaveAttribute('aria-live', 'polite');
    expect(nextRuns).toBeEmptyDOMElement();

    await advance();

    expect(document.querySelector('.dialog-field__words')).toBe(words);
    expect(words).toHaveTextContent('every day at 01:00');
    expect(document.querySelector('.dialog-live')).toBe(nextRuns);
    expect(nextRuns).toHaveTextContent('Next runs');
  });

  it("CronDialog announces the parser's refusal as an alert that the cron input is described by", async () => {
    const parserMessage = 'cron "0 25 * * *": end of range (25) above maximum (23): 25';
    installFetch({
      preview: cron => cron === '0 25 * * *' ? { status: 400, body: { error: parserMessage } } : DAILY_PREVIEW,
    });
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();
    const input = screen.getByLabelText('Cron');
    const describedBy = () => (input.getAttribute('aria-describedby') ?? '').split(' ')
      .map(id => document.getElementById(id));
    const hint = screen.getByText('minute · hour · day of month · month · day of week');
    const words = document.querySelector('.dialog-field__words');
    expect(screen.queryByRole('alert')).toBeNull();
    expect(describedBy()).toEqual([hint, words]);

    typeCron('0 25 * * *');
    await advance(300);

    const alert = screen.getByRole('alert');
    expect(alert).toHaveTextContent(parserMessage);
    expect(alert.className).toMatch(/\binfo-strip--error\b/);
    expect(input).toHaveAttribute('aria-invalid', 'true');
    expect(describedBy()).toEqual([hint, words, alert]);

    typeCron('0 2 * * *');
    await advance(300);
    expect(screen.queryByRole('alert')).toBeNull();
    expect(input).not.toHaveAttribute('aria-invalid');
    expect(describedBy()).toEqual([hint, words]);
  });

  it('CronDialog announces a refused save and a stale version as alerts', async () => {
    installFetch({
      put: body => body.cron === '0 2 * * *'
        ? { status: 409, body: { error: 'changed', code: 'stale_version', updated_by: 'bob@example.com' } }
        : { status: 404, body: { error: 'no model of the live release belongs to this schedule' } },
    });
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();

    fireEvent.click(saveButton());
    await advance();
    expect(screen.getByRole('alert')).toHaveTextContent('no model of the live release belongs to this schedule');

    typeCron('0 2 * * *');
    await advance(300);
    fireEvent.click(saveButton());
    await advance();
    const alerts = screen.getAllByRole('alert').map(a => a.textContent);
    expect(alerts).toContainEqual(expect.stringContaining('bob@example.com changed this schedule while you were editing'));
  });

  it('CronDialog opens with the cron input focused and gives the focus back to it when a request ends', async () => {
    let answer: (r: Reply) => void = () => {};
    installFetch({ put: () => new Promise<Reply>(resolve => { answer = resolve; }) });
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();
    const input = screen.getByLabelText('Cron');
    expect(input).toHaveFocus();

    const save = saveButton();
    save.focus();
    fireEvent.click(save);
    await advance();
    expect(input).toBeDisabled();
    // A browser takes the focus off a control that becomes disabled; jsdom
    // keeps it there, so the focus is dropped the way removing its holder does.
    const holder = document.createElement('button');
    document.body.appendChild(holder);
    holder.focus();
    holder.remove();
    expect(document.body).toHaveFocus();

    answer({ status: 500, body: { error: 'state down' } });
    await advance();

    expect(input).toBeEnabled();
    expect(input).toHaveFocus();
  });

  it('CronDialog leaves the focus alone when it is elsewhere when a request ends', async () => {
    let answer: (r: Reply) => void = () => {};
    installFetch({ put: () => new Promise<Reply>(resolve => { answer = resolve; }) });
    const outside = document.createElement('button');
    document.body.appendChild(outside);
    render(<CronDialog schedule={daily()} mode="edit" onClose={() => {}} />);
    await advance();

    // Focus on another page control when the request starts is not the
    // dialog's to take back.
    outside.focus();
    fireEvent.click(saveButton());
    await advance();
    answer({ status: 500, body: { error: 'state down' } });
    await advance();
    expect(outside).toHaveFocus();

    // Focus that leaves the dialog while the request is in flight stays out.
    answer = () => {};
    const save = saveButton();
    save.focus();
    fireEvent.click(save);
    await advance();
    outside.focus();
    answer({ status: 500, body: { error: 'state down' } });
    await advance();
    expect(outside).toHaveFocus();
    outside.remove();
  });
});
