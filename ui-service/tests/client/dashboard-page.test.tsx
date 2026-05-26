// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import DashboardPage from '../../src/client/DashboardPage';

const fetchMock = vi.fn();

beforeEach(() => {
  fetchMock.mockReset();
  fetchMock.mockResolvedValue({
    ok: true,
    json: async () => ({ schedules: [] }),
  });
  vi.stubGlobal('fetch', fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function renderPage() {
  return render(
    <MemoryRouter>
      <DashboardPage />
    </MemoryRouter>,
  );
}

describe('DashboardPage — shell + Update Graph button', () => {
  it('renders inside the .page / .page-header foundation', async () => {
    const { container } = renderPage();
    await act(async () => { await Promise.resolve(); });

    expect(container.querySelector('.page')).toBeInTheDocument();
    expect(container.querySelector('.page-header')).toBeInTheDocument();
    expect(container.querySelector('.page-content--readable')).toBeInTheDocument();
    expect(container.querySelector('.app')).toBeNull();
    expect(container.querySelector('.app-header')).toBeNull();
  });

  it('Update Graph button uses .btn.btn--secondary', async () => {
    renderPage();
    await act(async () => { await Promise.resolve(); });
    const btn = screen.getByRole('button', { name: /update graph/i });
    expect(btn.className).toMatch(/\bbtn\b/);
    expect(btn.className).toMatch(/\bbtn--secondary\b/);
    expect(btn.className).not.toMatch(/\bupdate-graph-btn\b/);
  });

  it('shows past-tense "Updated" with .is-success after a successful click', async () => {
    fetchMock.mockImplementation((url: string) => {
      if (url === '/api/dashboard/graph-update') {
        return Promise.resolve({ ok: true, json: async () => ({}) });
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({ schedules: [] }),
      });
    });

    const user = userEvent.setup();
    renderPage();
    await act(async () => { await Promise.resolve(); });

    const btn = screen.getByRole('button', { name: /update graph/i });
    await user.click(btn);
    await waitFor(() => {
      expect(btn).toHaveTextContent(/^Updated$/);
      expect(btn.className).toMatch(/\bis-success\b/);
    });
  });

  it('renders graph error as .info-strip--error (not .error-banner)', async () => {
    fetchMock.mockImplementation((url: string) => {
      if (url === '/api/dashboard/graph-update') {
        return Promise.resolve({
          ok: false,
          json: async () => ({ error: 'boom' }),
        });
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({ schedules: [] }),
      });
    });
    const user = userEvent.setup();
    const { container } = renderPage();
    await act(async () => { await Promise.resolve(); });

    await user.click(screen.getByRole('button', { name: /update graph/i }));
    await waitFor(() => {
      const strip = container.querySelector('.info-strip--error');
      expect(strip).toBeInTheDocument();
      expect(strip?.textContent).toMatch(/boom/);
    });
    expect(container.querySelector('.error-banner')).toBeNull();
  });
});
