// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import CancelDialog from '../../src/client/CancelDialog';

const stubConfig = {
  cancellation_reasons: ['Broken data', 'Planned maintenance', 'Other'],
};

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
    ok: true,
    json: async () => stubConfig,
  }));
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('CancelDialog — buttons use .btn foundation', () => {
  it('Close button uses .btn.btn--secondary, no legacy class', async () => {
    render(<CancelDialog scheduleName="daily" onCancelled={vi.fn()} onClose={vi.fn()} />);
    // Wait for config to load so both action buttons appear
    await waitFor(() => expect(screen.queryByText('Loading…')).toBeNull());
    const close = screen.getByRole('button', { name: /close/i });
    expect(close.className).toMatch(/\bbtn\b/);
    expect(close.className).toMatch(/\bbtn--secondary\b/);
    expect(close.className).not.toMatch(/\bdialog-btn\b/);
  });

  it('Cancel run button uses .btn.btn--danger, no legacy class', async () => {
    render(<CancelDialog scheduleName="daily" onCancelled={vi.fn()} onClose={vi.fn()} />);
    await waitFor(() => expect(screen.queryByText('Loading…')).toBeNull());
    const cancelRun = screen.getByRole('button', { name: /cancel run/i });
    expect(cancelRun.className).toMatch(/\bbtn\b/);
    expect(cancelRun.className).toMatch(/\bbtn--danger\b/);
    expect(cancelRun.className).not.toMatch(/\bdialog-btn\b/);
  });

  it('config error renders info-strip--error, not dialog-config-error', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false }));
    render(<CancelDialog scheduleName="daily" onCancelled={vi.fn()} onClose={vi.fn()} />);
    await waitFor(() => expect(screen.queryByText(/could not load/i)).toBeInTheDocument());
    const errEl = screen.getByText(/could not load/i);
    expect(errEl.className).toMatch(/\binfo-strip\b/);
    expect(errEl.className).toMatch(/\binfo-strip--error\b/);
    expect(errEl.className).not.toMatch(/\bdialog-config-error\b/);
  });
});

describe('CancelDialog — submitting', () => {
  function stubCancel(cancelResponse: { ok: boolean; body: unknown }) {
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      const res = url.endsWith('/cancel')
        ? cancelResponse
        : { ok: true, body: stubConfig };
      return Promise.resolve({ ok: res.ok, json: async () => res.body } as Response);
    }));
  }

  async function submit(onCancelled: () => void, onClose: () => void) {
    render(<CancelDialog scheduleName="daily" onCancelled={onCancelled} onClose={onClose} />);
    await waitFor(() => expect(screen.queryByText('Loading…')).toBeNull());
    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'Broken data' } });
    fireEvent.click(screen.getByRole('button', { name: /cancel run/i }));
  }

  it('reports an accepted cancel through onCancelled, then closes', async () => {
    stubCancel({ ok: true, body: { schedule_id: 'run-1' } });
    const calls: string[] = [];
    await submit(() => calls.push('cancelled'), () => calls.push('close'));
    await waitFor(() => expect(calls).toEqual(['cancelled', 'close']));
  });

  it('does not report a refused cancel, and stays open with its error', async () => {
    stubCancel({ ok: false, body: { error: 'no active run' } });
    const onCancelled = vi.fn();
    const onClose = vi.fn();
    await submit(onCancelled, onClose);
    await waitFor(() => expect(screen.getByText('no active run')).toBeInTheDocument());
    expect(onCancelled).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
  });
});
