// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import App from '../../src/client/App';

function mockFetch(handlers: Record<string, { status: number; body: unknown }>) {
  return vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    for (const [prefix, h] of Object.entries(handlers)) {
      if (url.includes(prefix)) {
        return new Response(JSON.stringify(h.body), {
          status: h.status,
          headers: { 'content-type': 'application/json' },
        });
      }
    }
    return new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } });
  });
}

describe('App auth gate', () => {
  beforeEach(() => window.history.replaceState({}, '', '/'));
  afterEach(() => vi.unstubAllGlobals());

  it('renders the sign-in page when /auth/me is 401', async () => {
    vi.stubGlobal('fetch', mockFetch({ '/auth/me': { status: 401, body: {} } }));
    render(<App />);
    expect(await screen.findByRole('link', { name: /sign in/i })).toBeInTheDocument();
  });

  it('renders the app shell when /auth/me succeeds', async () => {
    vi.stubGlobal('fetch', mockFetch({
      '/auth/me': { status: 200, body: { userId: 'i|o', email: 'o@c.com', name: 'O', role: 'operator' } },
      '/api/features': { status: 200, body: { chatBridgeEnabled: false } },
    }));
    render(<App />);
    expect(await screen.findByText('o@c.com')).toBeInTheDocument(); // UserMenu in the dashboard header
    expect(screen.queryByRole('link', { name: /sign in/i })).not.toBeInTheDocument();
  });
});
