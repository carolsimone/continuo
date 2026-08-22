// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
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
    render(<CancelDialog scheduleName="daily" onClose={vi.fn()} />);
    // Wait for config to load so both action buttons appear
    await waitFor(() => expect(screen.queryByText('Loading…')).toBeNull());
    const close = screen.getByRole('button', { name: /close/i });
    expect(close.className).toMatch(/\bbtn\b/);
    expect(close.className).toMatch(/\bbtn--secondary\b/);
    expect(close.className).not.toMatch(/\bdialog-btn\b/);
  });

  it('Cancel run button uses .btn.btn--danger, no legacy class', async () => {
    render(<CancelDialog scheduleName="daily" onClose={vi.fn()} />);
    await waitFor(() => expect(screen.queryByText('Loading…')).toBeNull());
    const cancelRun = screen.getByRole('button', { name: /cancel run/i });
    expect(cancelRun.className).toMatch(/\bbtn\b/);
    expect(cancelRun.className).toMatch(/\bbtn--danger\b/);
    expect(cancelRun.className).not.toMatch(/\bdialog-btn\b/);
  });

  it('config error renders info-strip--error, not dialog-config-error', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false }));
    render(<CancelDialog scheduleName="daily" onClose={vi.fn()} />);
    await waitFor(() => expect(screen.queryByText(/could not load/i)).toBeInTheDocument());
    const errEl = screen.getByText(/could not load/i);
    expect(errEl.className).toMatch(/\binfo-strip\b/);
    expect(errEl.className).toMatch(/\binfo-strip--error\b/);
    expect(errEl.className).not.toMatch(/\bdialog-config-error\b/);
  });
});
