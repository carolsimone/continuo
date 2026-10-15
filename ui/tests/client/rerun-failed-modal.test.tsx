// @vitest-environment jsdom
import { afterEach, describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import RerunFailedModal from '../../src/client/RerunFailedModal';
import type { ChangedUpstreamNode, RebasePreview } from '../../src/client/types';

const RUN_ID = 'run-1';
const PREVIEW_PATH = `/api/schedulers/${RUN_ID}/rebase-preview`;

const changedInvoices: ChangedUpstreamNode = {
  service_name: 'billing', schema_name: 'stg', table_name: 'invoices', downstream: 3, last_success_seconds: null,
};
const changedOrders: ChangedUpstreamNode = {
  service_name: 'core', schema_name: 'stg', table_name: 'orders', downstream: 18, last_success_seconds: 252.4,
};

// PREVIEW is the rebase with its changed upstream nodes included.
const PREVIEW: RebasePreview = {
  release_id: 'rel-7', promotion_seq: 7, total_nodes: 2000, will_run: 41,
  failed_or_skipped: 3, new_nodes: 1, changed_upstream: 2, downstream: 35, inherited: 1959,
  changed_nodes: [changedInvoices, changedOrders],
};

// WITHOUT_CHANGED is the same rebase with its changed upstream nodes left
// out; the server still names them.
const WITHOUT_CHANGED: RebasePreview = {
  ...PREVIEW, will_run: 21, changed_upstream: 0, downstream: 17, inherited: 1979,
};

const SERVICE_COLORS = new Map([['billing', '#0ea5e9'], ['core', '#f59e0b']]);

type Reply = { status?: number; body: unknown };
type Handler = (method: string, url: string, body: string | undefined) => Reply | Promise<Reply> | 'pending';

// apiFetch answers each request through handler; 'pending' never settles, and
// a promise settles when the test resolves it.
function apiFetch(handler: Handler) {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = input.toString();
    const reply = handler((init?.method ?? 'GET').toUpperCase(), url, init?.body as string | undefined);
    if (reply === 'pending') return new Promise<Response>(() => {});
    const { status = 200, body } = await reply;
    return { ok: status >= 200 && status < 300, status, json: async () => body } as Response;
  });
}

// previewing answers the preview by its toggle and accepts every POST.
function previewing(withChanged: RebasePreview = PREVIEW, without: RebasePreview = WITHOUT_CHANGED): Handler {
  return (method, url) => {
    if (method === 'GET' && url.startsWith(PREVIEW_PATH)) {
      return { body: url.endsWith('include_changed_upstream=true') ? withChanged : without };
    }
    return { body: {} };
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
}

function renderModal(over: Partial<Parameters<typeof RerunFailedModal>[0]> = {}) {
  const props = {
    runId: RUN_ID,
    driftLabel: null,
    serviceColors: SERVICE_COLORS,
    onClose: vi.fn(),
    onSubmitted: vi.fn(),
    ...over,
  };
  render(<RerunFailedModal {...props} />);
  return props;
}

function rerunButton() {
  return screen.getByRole('button', { name: /^Rerun/ });
}

function cancelButton() {
  return screen.getByRole('button', { name: /^Cancel$/ });
}

function includeToggle() {
  return screen.getByRole('checkbox', { name: /include changed upstream nodes/i });
}

function previewCalls(fetchMock: ReturnType<typeof apiFetch>): string[] {
  return fetchMock.mock.calls.map(c => String(c[0])).filter(u => u.startsWith(PREVIEW_PATH));
}

function postCalls(fetchMock: ReturnType<typeof apiFetch>) {
  return fetchMock.mock.calls.filter(c => (c[1] as RequestInit | undefined)?.method === 'POST');
}

async function chooseLatest(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByLabelText(/latest snapshot/i));
  await screen.findByText('Will run');
}

function willRunHeader(): HTMLElement {
  return screen.getByText('Will run').closest('.section-header') as HTMLElement;
}

function changedHeader(): HTMLElement {
  return screen.getByText('Changed upstream').closest('.section-header') as HTMLElement;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('RerunFailedModal — shell', () => {
  it('is a labelled wide modal dialog offering both snapshots, "This snapshot" first and chosen', () => {
    const fetchMock = apiFetch(previewing());
    vi.stubGlobal('fetch', fetchMock);
    renderModal();

    const dialog = screen.getByRole('dialog');
    expect(dialog).toHaveClass('dialog', 'dialog--wide');
    expect(dialog).toHaveAttribute('aria-modal', 'true');
    expect(dialog.parentElement).toHaveClass('dialog-overlay');
    const titleId = dialog.getAttribute('aria-labelledby');
    expect(titleId).toBeTruthy();
    expect(document.getElementById(titleId as string)).toHaveTextContent('Rerun failed nodes');
    expect(document.getElementById(titleId as string)).toHaveClass('dialog-title');

    expect(screen.getByLabelText(/this snapshot/i)).toBeChecked();
    expect(screen.getByLabelText(/latest snapshot/i)).not.toBeChecked();
    expect(screen.getByLabelText(/this snapshot/i).closest('.rerun-mode-option')).toBeTruthy();
    expect(screen.getByText(/reuse the image \+ manifest pinned/i)).toHaveClass('rerun-mode-option__desc');
    expect(screen.getByText(/use the current image \+ manifest/i)).toHaveClass('rerun-mode-option__desc');
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('shows the drift strip at the top only when driftLabel is set', () => {
    vi.stubGlobal('fetch', apiFetch(previewing()));
    renderModal({ driftLabel: 'source 1 release behind latest' });

    const strip = screen.getByText('source 1 release behind latest').closest('.info-strip') as HTMLElement;
    expect(strip).toHaveClass('info-strip--warning');
    expect(strip.compareDocumentPosition(screen.getByLabelText(/this snapshot/i)) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it('has no drift strip without driftLabel', () => {
    vi.stubGlobal('fetch', apiFetch(previewing()));
    renderModal();

    expect(document.querySelector('.info-strip')).toBeNull();
  });

  it('puts Cancel (.btn--secondary) before Rerun (.btn--primary) in the footer', () => {
    vi.stubGlobal('fetch', apiFetch(previewing()));
    renderModal();

    const footer = document.querySelector('.dialog-actions') as HTMLElement;
    expect(Array.from(footer.querySelectorAll('button')).map(b => b.textContent)).toEqual(['Cancel', 'Rerun']);
    expect(cancelButton()).toHaveClass('btn', 'btn--secondary');
    expect(rerunButton()).toHaveClass('btn', 'btn--primary');
    expect(rerunButton()).toBeEnabled();
  });

  it('closes on Cancel, Escape and an overlay click, but not on a click inside the dialog', async () => {
    vi.stubGlobal('fetch', apiFetch(previewing()));
    const user = userEvent.setup();
    const props = renderModal();

    await user.click(screen.getByRole('dialog'));
    expect(props.onClose).not.toHaveBeenCalled();

    await user.click(cancelButton());
    fireEvent.keyDown(document, { key: 'Escape' });
    fireEvent.click(document.querySelector('.dialog-overlay') as HTMLElement);
    expect(props.onClose).toHaveBeenCalledTimes(3);
  });
});

describe('RerunFailedModal — This snapshot', () => {
  it('posts an empty rerun and reports the submitted mode', async () => {
    const fetchMock = apiFetch(previewing());
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    const props = renderModal();

    await user.click(rerunButton());

    await waitFor(() => expect(props.onSubmitted).toHaveBeenCalledWith('this'));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(`/api/schedulers/${RUN_ID}/rerun`);
    expect(init).toMatchObject({ method: 'POST', body: '{}', headers: { 'Content-Type': 'application/json' } });
  });

  it('keeps a refusal inside the dialog as an error strip and lets the user try again', async () => {
    vi.stubGlobal('fetch', apiFetch(() => ({ status: 409, body: { error: 'schedule already has an active run' } })));
    const user = userEvent.setup();
    const props = renderModal();

    await user.click(rerunButton());

    const strip = (await screen.findByText('schedule already has an active run')).closest('.info-strip') as HTMLElement;
    expect(strip).toHaveClass('info-strip--error');
    expect(strip.closest('.dialog')).toBeTruthy();
    expect(props.onSubmitted).not.toHaveBeenCalled();
    expect(rerunButton()).toBeEnabled();
    expect(rerunButton()).not.toHaveClass('is-loading');
  });

  it('says the request failed when the network does', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => { throw new Error('offline'); }));
    const user = userEvent.setup();
    renderModal();

    await user.click(rerunButton());

    expect((await screen.findByText('Request failed — please try again')).closest('.info-strip--error')).toBeTruthy();
  });

  it('marks Rerun loading, disables both buttons, and ignores Escape and the overlay while in flight', async () => {
    vi.stubGlobal('fetch', apiFetch(method => (method === 'POST' ? 'pending' : { body: {} })));
    const user = userEvent.setup();
    const props = renderModal();

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

describe('RerunFailedModal — Latest snapshot preview', () => {
  it('says the preview is loading in a neutral strip and keeps Rerun disabled meanwhile', async () => {
    vi.stubGlobal('fetch', apiFetch((method, url) => (method === 'GET' && url.startsWith(PREVIEW_PATH) ? 'pending' : { body: {} })));
    const user = userEvent.setup();
    renderModal();

    await user.click(screen.getByLabelText(/latest snapshot/i));

    expect(screen.getByText(/loading what will run/i).closest('.info-strip')).toHaveClass('info-strip--neutral');
    expect(rerunButton()).toBeDisabled();
    expect(screen.queryByText('Will run')).toBeNull();
  });

  it('fetches the preview with changed upstream nodes included by default', async () => {
    const fetchMock = apiFetch(previewing());
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    renderModal();

    await chooseLatest(user);

    expect(previewCalls(fetchMock)).toEqual([`${PREVIEW_PATH}?include_changed_upstream=true`]);
    expect(includeToggle()).toBeChecked();
    expect(includeToggle().closest('.rerun-mode-option')).toBeTruthy();
    expect(screen.getByText(/^Unchecked, only the failed part re-runs; a failure caused by one of these changes fails again$/))
      .toHaveClass('rerun-mode-option__desc');
    expect(screen.queryByText(/loading what will run/i)).toBeNull();
    expect(rerunButton()).toBeEnabled();
  });

  it('counts how many nodes will run out of how many, with thousands separators, and why', async () => {
    vi.stubGlobal('fetch', apiFetch(previewing()));
    const user = userEvent.setup();
    renderModal();

    await chooseLatest(user);

    expect(within(willRunHeader()).getByText('41')).toHaveClass('section-header__count');
    expect(within(willRunHeader()).getByText('of 2,000 nodes · 1,959 inherited')).toHaveClass('section-header__sub');
    const reasons = document.querySelector('.preview-facts--summary') as HTMLElement;
    expect(Array.from(reasons.querySelectorAll(':scope > span:not(.preview-facts__sep)')).map(s => s.textContent))
      .toEqual(['3 failed or skipped', '1 new', '2 changed upstream', '35 downstream']);
    expect(reasons.querySelectorAll('.preview-facts__sep')).toHaveLength(3);
  });

  it('leaves a reason no node runs for out of the reasons line', async () => {
    vi.stubGlobal('fetch', apiFetch(previewing({ ...PREVIEW, new_nodes: 0, will_run: 40, inherited: 1960 })));
    const user = userEvent.setup();
    renderModal();

    await chooseLatest(user);

    const reasons = document.querySelector('.preview-facts--summary') as HTMLElement;
    expect(reasons.textContent).toBe('3 failed or skipped·2 changed upstream·35 downstream');
  });

  it('names each changed upstream node with its accent, schema.table id and only the facts it knows', async () => {
    vi.stubGlobal('fetch', apiFetch(previewing()));
    const user = userEvent.setup();
    renderModal();

    await chooseLatest(user);

    expect(within(changedHeader()).getByText('2')).toHaveClass('section-header__count');
    expect(changedHeader().querySelector('.section-header__sub')).toBeNull();
    const rows = Array.from(document.querySelectorAll<HTMLElement>('.preview-row'));
    expect(rows.map(r => r.querySelector('.preview-row__id')?.textContent)).toEqual(['stg.invoices', 'stg.orders']);
    expect(rows.map(r => r.querySelector('.preview-row__id')?.getAttribute('title')))
      .toEqual(['billing.stg.invoices', 'core.stg.orders']);
    expect(rows[0].querySelector('.preview-facts')?.textContent).toBe('+3 downstream');
    expect(rows[1].querySelector('.preview-facts')?.textContent).toBe('+18 downstream·last run 4m 12s');
    expect((rows[0].querySelector('.nodes-group-dot') as HTMLElement).style.background).toBe('rgb(14, 165, 233)');
    expect((rows[1].querySelector('.nodes-group-dot') as HTMLElement).style.background).toBe('rgb(245, 158, 11)');
    expect(rows.every(r => r.querySelector('button') === null)).toBe(true);
  });

  it('paints a service the page does not show in slate and reads a sub-second run as <1s', async () => {
    const quick: ChangedUpstreamNode = {
      service_name: 'ops', schema_name: 'raw', table_name: 'events', downstream: 1200, last_success_seconds: 0.4,
    };
    vi.stubGlobal('fetch', apiFetch(previewing({ ...PREVIEW, changed_upstream: 1, changed_nodes: [quick] })));
    const user = userEvent.setup();
    renderModal();

    await chooseLatest(user);

    const row = document.querySelector('.preview-row') as HTMLElement;
    expect(row.querySelector('.preview-facts')?.textContent).toBe('+1,200 downstream·last run <1s');
    expect((row.querySelector('.nodes-group-dot') as HTMLElement).style.background).toBe('rgb(148, 163, 184)');
  });

  it('previews again without changed upstream nodes when unchecked, and labels them left out', async () => {
    const fetchMock = apiFetch(previewing());
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    renderModal();
    await chooseLatest(user);

    await user.click(includeToggle());

    await waitFor(() => expect(within(willRunHeader()).getByText('21')).toBeInTheDocument());
    expect(within(willRunHeader()).getByText('of 2,000 nodes · 1,979 inherited')).toBeInTheDocument();
    expect(previewCalls(fetchMock)).toEqual([
      `${PREVIEW_PATH}?include_changed_upstream=true`,
      `${PREVIEW_PATH}?include_changed_upstream=false`,
    ]);
    expect(includeToggle()).not.toBeChecked();
    expect(within(changedHeader()).getByText(/left out of this rerun/i)).toHaveClass('section-header__sub');
    expect(document.querySelectorAll('.preview-row')).toHaveLength(2);

    await user.click(includeToggle());

    await waitFor(() => expect(within(willRunHeader()).getByText('41')).toBeInTheDocument());
    expect(previewCalls(fetchMock)).toHaveLength(3);
    expect(changedHeader().querySelector('.section-header__sub')).toBeNull();
  });

  it('keeps the last preview and the toggle on screen while the next one loads', async () => {
    const next = deferred<Reply>();
    vi.stubGlobal('fetch', apiFetch((method, url) => {
      if (method !== 'GET') return { body: {} };
      return url.endsWith('=true') ? { body: PREVIEW } : next.promise;
    }));
    const user = userEvent.setup();
    renderModal();
    await chooseLatest(user);

    await user.click(includeToggle());

    expect(screen.getByText(/loading what will run/i).closest('.info-strip')).toHaveClass('info-strip--neutral');
    expect(within(willRunHeader()).getByText('41')).toBeInTheDocument();
    expect(includeToggle()).not.toBeChecked();
    expect(rerunButton()).toBeDisabled();

    next.resolve({ body: WITHOUT_CHANGED });

    await waitFor(() => expect(within(willRunHeader()).getByText('21')).toBeInTheDocument());
    expect(rerunButton()).toBeEnabled();
  });

  it('discards a preview that answers after a newer one', async () => {
    const stale = deferred<Reply>();
    let withChanged = 0;
    vi.stubGlobal('fetch', apiFetch((method, url) => {
      if (method !== 'GET') return { body: {} };
      if (url.endsWith('=false')) return stale.promise;
      withChanged += 1;
      return { body: withChanged === 1 ? PREVIEW : { ...PREVIEW, will_run: 50, downstream: 44, inherited: 1950 } };
    }));
    const user = userEvent.setup();
    renderModal();
    await chooseLatest(user);

    await user.click(includeToggle());
    await user.click(includeToggle());
    await waitFor(() => expect(within(willRunHeader()).getByText('50')).toBeInTheDocument());

    stale.resolve({ body: WITHOUT_CHANGED });
    await new Promise(r => setTimeout(r, 0));

    expect(within(willRunHeader()).getByText('50')).toBeInTheDocument();
    expect(includeToggle()).toBeChecked();
    expect(changedHeader().querySelector('.section-header__sub')).toBeNull();
    expect(rerunButton()).toBeEnabled();
  });

  it('hides the toggle and the changed list when nothing upstream changed', async () => {
    vi.stubGlobal('fetch', apiFetch(previewing({ ...PREVIEW, changed_upstream: 0, changed_nodes: [] })));
    const user = userEvent.setup();
    renderModal();

    await chooseLatest(user);

    expect(screen.queryByRole('checkbox')).toBeNull();
    expect(screen.queryByText('Changed upstream')).toBeNull();
    expect(document.querySelector('.preview-list')).toBeNull();
  });

  it('groups more than twelve changed nodes by service, collapsed', async () => {
    const many: ChangedUpstreamNode[] = Array.from({ length: 13 }, (_, i) => ({
      service_name: i < 4 ? 'billing' : 'core', schema_name: 'stg', table_name: `t${String(i).padStart(2, '0')}`,
      downstream: 1, last_success_seconds: null,
    }));
    vi.stubGlobal('fetch', apiFetch(previewing({ ...PREVIEW, changed_upstream: 13, changed_nodes: many })));
    const user = userEvent.setup();
    renderModal();
    await chooseLatest(user);

    const headers = Array.from(document.querySelectorAll<HTMLElement>('.preview-group__header'));
    expect(headers.map(h => h.querySelector('.nodes-group-name')?.textContent)).toEqual(['billing', 'core']);
    expect(headers.map(h => h.querySelector('.nodes-group-count')?.textContent)).toEqual(['4', '9']);
    expect(headers.map(h => h.getAttribute('aria-expanded'))).toEqual(['false', 'false']);
    expect(document.querySelectorAll('.preview-row')).toHaveLength(0);

    await user.click(headers[0]);
    expect(document.querySelectorAll('.preview-row')).toHaveLength(4);
  });

  it('shows a failed preview as an error strip and keeps Rerun disabled', async () => {
    vi.stubGlobal('fetch', apiFetch((method, url) => (
      method === 'GET' && url.startsWith(PREVIEW_PATH)
        ? { status: 409, body: { error: 'a test run cannot be rebased' } }
        : { body: {} })));
    const user = userEvent.setup();
    renderModal();

    await user.click(screen.getByLabelText(/latest snapshot/i));

    const strip = (await screen.findByText('a test run cannot be rebased')).closest('.info-strip') as HTMLElement;
    expect(strip).toHaveClass('info-strip--error');
    expect(rerunButton()).toBeDisabled();
    expect(screen.queryByText('Will run')).toBeNull();
    expect(screen.queryByText(/loading what will run/i)).toBeNull();
  });

  it('treats an answer that is not a preview as a failed preview', async () => {
    vi.stubGlobal('fetch', apiFetch((method, url) => (
      method === 'GET' && url.startsWith(PREVIEW_PATH) ? { body: { scheduler: { status: 'failed' } } } : { body: {} })));
    const user = userEvent.setup();
    renderModal();

    await user.click(screen.getByLabelText(/latest snapshot/i));

    expect((await screen.findByText('Request failed — please try again')).closest('.info-strip--error')).toBeTruthy();
    expect(screen.queryByText('Will run')).toBeNull();
    expect(rerunButton()).toBeDisabled();
  });

  it('says the preview failed when the network does', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => { throw new Error('offline'); }));
    const user = userEvent.setup();
    renderModal();

    await user.click(screen.getByLabelText(/latest snapshot/i));

    expect((await screen.findByText('Request failed — please try again')).closest('.info-strip--error')).toBeTruthy();
    expect(rerunButton()).toBeDisabled();
  });

  it('drops the preview and enables Rerun when "This snapshot" is chosen again', async () => {
    vi.stubGlobal('fetch', apiFetch((method, url) => (
      method === 'GET' && url.startsWith(PREVIEW_PATH) ? { status: 500, body: { error: 'boom' } } : { body: {} })));
    const user = userEvent.setup();
    renderModal();
    await user.click(screen.getByLabelText(/latest snapshot/i));
    await screen.findByText('boom');

    await user.click(screen.getByLabelText(/this snapshot/i));

    expect(screen.queryByText('boom')).toBeNull();
    expect(rerunButton()).toBeEnabled();
  });
});

describe('RerunFailedModal — Latest snapshot rebase', () => {
  it('rebases on the previewed release with changed upstream nodes and reports the mode', async () => {
    const fetchMock = apiFetch(previewing());
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    const props = renderModal();
    await chooseLatest(user);

    await user.click(rerunButton());

    await waitFor(() => expect(props.onSubmitted).toHaveBeenCalledWith('latest'));
    const [[url, init]] = postCalls(fetchMock) as [string, RequestInit][];
    expect(url).toBe(`/api/schedulers/${RUN_ID}/rebase`);
    expect(init.headers).toEqual({ 'Content-Type': 'application/json' });
    expect(JSON.parse(String(init.body))).toEqual({ include_changed_upstream: true, expected_release_id: 'rel-7' });
  });

  it('rebases without changed upstream nodes when unchecked', async () => {
    const fetchMock = apiFetch(previewing(PREVIEW, { ...WITHOUT_CHANGED, release_id: 'rel-7' }));
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    const props = renderModal();
    await chooseLatest(user);
    await user.click(includeToggle());
    await waitFor(() => expect(rerunButton()).toBeEnabled());
    await screen.findByText('of 2,000 nodes · 1,979 inherited');

    await user.click(rerunButton());

    await waitFor(() => expect(props.onSubmitted).toHaveBeenCalledTimes(1));
    const [[, init]] = postCalls(fetchMock) as [string, RequestInit][];
    expect(JSON.parse(String(init.body))).toEqual({ include_changed_upstream: false, expected_release_id: 'rel-7' });
  });

  it('previews again and warns when the live release moved, then rebases on the new release', async () => {
    let previews = 0;
    let posts = 0;
    const fetchMock = apiFetch((method, url) => {
      if (method === 'GET' && url.startsWith(PREVIEW_PATH)) {
        previews += 1;
        return { body: previews === 1 ? PREVIEW : { ...PREVIEW, release_id: 'rel-8', promotion_seq: 8, will_run: 45, inherited: 1955 } };
      }
      posts += 1;
      return posts === 1
        ? { status: 409, body: { error: 'the live release changed; preview again', code: 'release_moved' } }
        : { body: {} };
    });
    vi.stubGlobal('fetch', fetchMock);
    const user = userEvent.setup();
    const props = renderModal();
    await chooseLatest(user);

    await user.click(rerunButton());

    const strip = (await screen.findByText('The live release changed; review the updated selection')).closest('.info-strip') as HTMLElement;
    expect(strip).toHaveClass('info-strip--warning');
    await waitFor(() => expect(within(willRunHeader()).getByText('45')).toBeInTheDocument());
    expect(previews).toBe(2);
    expect(previewCalls(fetchMock)[1]).toBe(`${PREVIEW_PATH}?include_changed_upstream=true`);
    expect(props.onSubmitted).not.toHaveBeenCalled();
    expect(document.querySelector('.info-strip--error')).toBeNull();
    await waitFor(() => expect(rerunButton()).toBeEnabled());

    await user.click(rerunButton());

    await waitFor(() => expect(props.onSubmitted).toHaveBeenCalledWith('latest'));
    const last = postCalls(fetchMock).at(-1) as [string, RequestInit];
    expect(JSON.parse(String(last[1].body)).expected_release_id).toBe('rel-8');
  });

  it('keeps any other refusal inside the dialog as an error strip', async () => {
    vi.stubGlobal('fetch', apiFetch((method, url) => (
      method === 'GET' && url.startsWith(PREVIEW_PATH)
        ? { body: PREVIEW }
        : { status: 409, body: { error: 'schedule already has an active run' } })));
    const user = userEvent.setup();
    const props = renderModal();
    await chooseLatest(user);

    await user.click(rerunButton());

    const strip = (await screen.findByText('schedule already has an active run')).closest('.info-strip') as HTMLElement;
    expect(strip).toHaveClass('info-strip--error');
    expect(strip.closest('.dialog')).toBeTruthy();
    expect(screen.queryByText(/the live release changed/i)).toBeNull();
    expect(props.onSubmitted).not.toHaveBeenCalled();
    expect(rerunButton()).toBeEnabled();
  });

  it('ignores Escape and the overlay while the rebase is in flight', async () => {
    vi.stubGlobal('fetch', apiFetch((method, url) => (
      method === 'GET' && url.startsWith(PREVIEW_PATH) ? { body: PREVIEW } : 'pending')));
    const user = userEvent.setup();
    const props = renderModal();
    await chooseLatest(user);

    await user.click(rerunButton());

    expect(rerunButton()).toHaveClass('is-loading');
    expect(cancelButton()).toBeDisabled();
    fireEvent.keyDown(document, { key: 'Escape' });
    fireEvent.click(document.querySelector('.dialog-overlay') as HTMLElement);
    expect(props.onClose).not.toHaveBeenCalled();
  });
});
