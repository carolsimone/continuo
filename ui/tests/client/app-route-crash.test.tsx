// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import App from '../../src/client/App';

vi.mock('../../src/client/DashboardPage', () => ({
  default: () => { throw new Error('boom'); },
}));

describe('App when a route crashes', () => {
  beforeEach(() => {
    window.history.replaceState({}, '', '/');
    // React logs every caught render error to the console.
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it('shows the failed page message and keeps the app shell instead of a blank page', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const body = String(input).includes('/auth/me')
        ? { userId: 'i|o', email: 'o@c.com', name: 'O', role: 'operator' }
        : { chatBridgeEnabled: false };
      return new Response(JSON.stringify(body), { status: 200 });
    }));
    const { container } = render(<App />);
    expect(await screen.findByText('This page failed to display.')).toBeInTheDocument();
    expect(container.querySelector('.app-shell')).toBeInTheDocument();
  });
});
