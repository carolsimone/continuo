// @vitest-environment jsdom
import { afterEach, describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import RerunDialog from '../../src/client/RerunDialog';
import type { Task } from '../../src/client/types';
import type { TaskStatus } from '../../src/server/generated/vocabulary.gen';

const RUN_ID = 'run-1';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SERVICE_COLORS = new Map([['billing', '#0ea5e9'], ['core', '#f59e0b']]);

function task(service: string, table: string, status: TaskStatus): Task {
  return {
    task_id: `t-${table}`, service_name: service, schema_name: 'stg', table_name: table,
    job_name: table, status, attempt: 0, max_retries: 0, created_at: null,
  };
}

// TASKS is a run where one node failed, the node below it was skipped and two
// nodes succeeded.
const TASKS: Task[] = [
  task('core', 'customers', 'succeeded'),
  task('billing', 'invoices', 'failed'),
  task('core', 'orders', 'succeeded'),
  task('billing', 'revenue', 'skipped'),
];

const TASKS_PATH = `/api/runs/${RUN_ID}/tasks`;

type Reply = { status?: number; body: unknown };
type Handler = (method: string, url: string) => Reply | 'pending';

function apiFetch(handler: Handler) {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const reply = handler((init?.method ?? 'GET').toUpperCase(), input.toString());
    if (reply === 'pending') return new Promise<Response>(() => {});
    const { status = 200, body } = reply;
    return { ok: status >= 200 && status < 300, status, json: async () => body } as Response;
  });
}

// withTasks answers the read of the run's tasks with TASKS and every other
// request through post.
function withTasks(post: Handler): Handler {
  return (method, url) => (method === 'GET' && url.startsWith(TASKS_PATH)
    ? { body: { tasks: TASKS, total_count: TASKS.length } }
    : post(method, url));
}

const accepting: Handler = withTasks(() => ({ body: {} }));

// listed waits for the run's tasks the dialog reads as it opens.
async function listed() {
  await screen.findByText('Will run');
}

function renderDialog(over: Partial<Parameters<typeof RerunDialog>[0]> = {}) {
  const props = {
    runId: RUN_ID,
    releaseId: 'rel-6' as string | null | undefined,
    driftLabel: null as string | null,
    serviceColors: SERVICE_COLORS,
    onClose: vi.fn(),
    onSubmitted: vi.fn(),
    ...over,
  };
  const view = render(<RerunDialog {...props} />);
  return { props, view };
}

const rerunButton = () => screen.getByRole('button', { name: /^Rerun/ });
const cancelButton = () => screen.getByRole('button', { name: /^Cancel$/ });

function postsOf(fetchMock: ReturnType<typeof apiFetch>) {
  return fetchMock.mock.calls.filter(c => (c[1] as RequestInit | undefined)?.method === 'POST');
}

function requestIdsOf(fetchMock: ReturnType<typeof apiFetch>): string[] {
  return postsOf(fetchMock)
    .map(c => JSON.parse(String((c[1] as RequestInit).body)).request_id);
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('RerunDialog — what it asks', () => {
  it('is a labelled wide modal dialog titled Rerun this run', () => {
    vi.stubGlobal('fetch', apiFetch(accepting));
    renderDialog();

    const dialog = screen.getByRole('dialog');
    expect(dialog).toHaveClass('dialog', 'dialog--wide');
    expect(dialog).toHaveAttribute('aria-modal', 'true');
    expect(dialog.parentElement).toHaveClass('dialog-overlay');
    const title = document.getElementById(dialog.getAttribute('aria-labelledby') as string);
    expect(title).toHaveTextContent('Rerun this run');
    expect(title).toHaveClass('dialog-title');
  });

  it('names the release the rerun runs on', () => {
    vi.stubGlobal('fetch', apiFetch(accepting));
    renderDialog();

    expect(screen.getByText('On release rel-6, the release this run ran on.')).toHaveClass('dialog-subtitle');
  });

  it('says "the release this run ran on" when the run names none', () => {
    vi.stubGlobal('fetch', apiFetch(accepting));
    renderDialog({ releaseId: null });

    expect(screen.getByText('On the release this run ran on.')).toHaveClass('dialog-subtitle');
  });

  it('counts the nodes that will run again out of the run\'s nodes', async () => {
    vi.stubGlobal('fetch', apiFetch(accepting));
    renderDialog();
    await listed();

    const header = screen.getByText('Will run').closest('.section-header') as HTMLElement;
    expect(within(header).getByText('2')).toHaveClass('section-header__count');
    expect(within(header).getByText('of 4 nodes · 2 inherited')).toHaveClass('section-header__sub');
  });

  it('lists every node that did not succeed, by schema.table with its status, and no node that did', async () => {
    vi.stubGlobal('fetch', apiFetch(accepting));
    renderDialog();
    await listed();

    const list = document.querySelector('.preview-list') as HTMLElement;
    const rows = Array.from(list.querySelectorAll('li'));
    expect(rows.map(r => r.textContent)).toEqual(['stg.invoicesfailed', 'stg.revenueskipped']);
    expect(within(list).queryByText('stg.customers')).toBeNull();
  });

  it('shows the drift strip at the top only when driftLabel is set', () => {
    vi.stubGlobal('fetch', apiFetch(accepting));
    renderDialog({ driftLabel: 'source 3 releases behind latest' });

    const strip = screen.getByText('source 3 releases behind latest').closest('.info-strip') as HTMLElement;
    expect(strip).toHaveClass('info-strip--warning');
  });

  it('has no drift strip without driftLabel', async () => {
    vi.stubGlobal('fetch', apiFetch(accepting));
    renderDialog();
    await listed();

    expect(document.querySelector('.info-strip')).toBeNull();
  });

  it('reads the run\'s tasks as it opens, saying so in a neutral strip with Rerun disabled meanwhile', async () => {
    const fetchMock = apiFetch((method, url) => (method === 'GET' && url.startsWith(TASKS_PATH) ? 'pending' : { body: {} }));
    vi.stubGlobal('fetch', fetchMock);
    renderDialog();

    expect(screen.getByText('Loading the nodes this run did not finish…').closest('.info-strip')).toHaveClass('info-strip--neutral');
    expect(rerunButton()).toBeDisabled();
    expect(screen.queryByText('Will run')).toBeNull();
    expect(String(fetchMock.mock.calls[0][0])).toBe(`${TASKS_PATH}?limit=200&offset=0`);
  });

  it('shows a failed read as an error strip and keeps Rerun disabled', async () => {
    vi.stubGlobal('fetch', apiFetch(() => ({ status: 503, body: { error: 'unavailable' } })));
    renderDialog();

    const strip = (await screen.findByText('Can\'t read the nodes of this run. Close and try again.')).closest('.info-strip');
    expect(strip).toHaveClass('info-strip--error');
    expect(rerunButton()).toBeDisabled();
    expect(screen.queryByText('Will run')).toBeNull();
  });

  it('puts Cancel (.btn--secondary) before Rerun (.btn--primary) in the footer', async () => {
    vi.stubGlobal('fetch', apiFetch(accepting));
    renderDialog();
    await listed();

    const footer = document.querySelector('.dialog-actions') as HTMLElement;
    expect(Array.from(footer.querySelectorAll('button')).map(b => b.textContent)).toEqual(['Cancel', 'Rerun']);
    expect(cancelButton()).toHaveClass('btn', 'btn--secondary');
    expect(rerunButton()).toHaveClass('btn', 'btn--primary');
    expect(rerunButton()).toBeEnabled();
  });

  it('closes on Cancel, Escape and an overlay click, but not on a click inside the dialog', async () => {
    vi.stubGlobal('fetch', apiFetch(accepting));
    const user = userEvent.setup();
    const { props } = renderDialog();

    await user.click(screen.getByRole('dialog'));
    expect(props.onClose).not.toHaveBeenCalled();

    await user.click(cancelButton());
    fireEvent.keyDown(document, { key: 'Escape' });
    fireEvent.click(document.querySelector('.dialog-overlay') as HTMLElement);
    expect(props.onClose).toHaveBeenCalledTimes(3);
  });
});

describe('RerunDialog — confirming', () => {
  it('posts the rerun with a request id, and nothing before the press', async () => {
    const fetchMock = apiFetch(accepting);
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    const { props } = renderDialog();
    await listed();
    expect(postsOf(fetchMock)).toHaveLength(0);

    await user.click(rerunButton());

    await waitFor(() => expect(props.onSubmitted).toHaveBeenCalledTimes(1));
    expect(postsOf(fetchMock)).toHaveLength(1);
    const [url, init] = postsOf(fetchMock)[0] as [string, RequestInit];
    expect(url).toBe(`/api/runs/${RUN_ID}/rerun`);
    expect(init).toMatchObject({ method: 'POST', headers: { 'Content-Type': 'application/json' } });
    expect(JSON.parse(String(init.body))).toEqual({ request_id: expect.stringMatching(UUID) });
  });

  it('keeps a refusal inside the dialog as an error strip and lets the user try again', async () => {
    vi.stubGlobal('fetch', apiFetch(withTasks(() => ({ status: 409, body: { error: 'schedule already has an active run' } }))));
    const user = userEvent.setup();
    const { props } = renderDialog();
    await listed();

    await user.click(rerunButton());

    const strip = (await screen.findByText('schedule already has an active run')).closest('.info-strip') as HTMLElement;
    expect(strip).toHaveClass('info-strip--error');
    expect(strip.closest('.dialog')).toBeTruthy();
    expect(props.onSubmitted).not.toHaveBeenCalled();
    expect(rerunButton()).toBeEnabled();
    expect(rerunButton()).not.toHaveClass('is-loading');
  });

  it('says the request failed when the network does', async () => {
    vi.stubGlobal('fetch', apiFetch(withTasks(() => { throw new Error('offline'); })));
    const user = userEvent.setup();
    renderDialog();
    await listed();

    await user.click(rerunButton());

    expect((await screen.findByText('Request failed — please try again')).closest('.info-strip--error')).toBeTruthy();
  });

  it('marks Rerun loading, disables both buttons, and ignores Escape and the overlay while in flight', async () => {
    vi.stubGlobal('fetch', apiFetch(withTasks(() => 'pending')));
    const user = userEvent.setup();
    const { props } = renderDialog();
    await listed();

    await user.click(rerunButton());

    expect(rerunButton()).toHaveTextContent('Rerunning…');
    expect(rerunButton()).toHaveClass('is-loading');
    expect(rerunButton()).toBeDisabled();
    expect(cancelButton()).toBeDisabled();
    fireEvent.keyDown(document, { key: 'Escape' });
    fireEvent.click(document.querySelector('.dialog-overlay') as HTMLElement);
    expect(props.onClose).not.toHaveBeenCalled();
  });
});

describe('RerunDialog — request id', () => {
  it('a retry after a timeout inside the same dialog sends the id the first try sent', async () => {
    let posts = 0;
    const fetchMock = apiFetch(withTasks(() => {
      posts += 1;
      if (posts === 1) throw new Error('timeout');
      return { body: {} };
    }));
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    const { props } = renderDialog();
    await listed();

    await user.click(rerunButton());
    await screen.findByText('Request failed — please try again');
    await user.click(rerunButton());

    await waitFor(() => expect(props.onSubmitted).toHaveBeenCalledTimes(1));
    const ids = requestIdsOf(fetchMock);
    expect(ids).toHaveLength(2);
    expect(ids[0]).toMatch(UUID);
    expect(ids[1]).toBe(ids[0]);
  });

  it('a dialog opened again mints a new one', async () => {
    const fetchMock = apiFetch(accepting);
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    const { view } = renderDialog();
    await listed();
    await user.click(rerunButton());
    await waitFor(() => expect(requestIdsOf(fetchMock)).toHaveLength(1));
    view.unmount();

    renderDialog();
    await listed();
    await user.click(rerunButton());
    await waitFor(() => expect(requestIdsOf(fetchMock)).toHaveLength(2));

    const ids = requestIdsOf(fetchMock);
    expect(ids[1]).not.toBe(ids[0]);
  });
});
