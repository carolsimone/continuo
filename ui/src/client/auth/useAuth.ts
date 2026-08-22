import { useEffect, useState } from 'react';

export interface AuthUser {
  userId: string;
  email: string;
  name: string;
  role: 'viewer' | 'operator';
}

export type AuthState =
  | { status: 'loading' }
  | { status: 'unauthenticated' }
  | { status: 'authenticated'; user: AuthUser };

// Bootstraps identity from /auth/me, and flips to unauthenticated when any
// later same-origin /api call returns 401 (the session expired server-side),
// so the user lands on the sign-in page instead of a half-broken view.
export function useAuth(): AuthState {
  const [state, setState] = useState<AuthState>({ status: 'loading' });

  useEffect(() => {
    let cancelled = false;
    fetch('/auth/me')
      .then((r) => (r.ok ? r.json() : null))
      .then((user) => {
        if (!cancelled) setState(user ? { status: 'authenticated', user } : { status: 'unauthenticated' });
      })
      .catch(() => {
        if (!cancelled) setState({ status: 'unauthenticated' });
      });

    const originalFetch = window.fetch.bind(window);
    window.fetch = async (...args: Parameters<typeof fetch>) => {
      const res = await originalFetch(...args);
      const url = typeof args[0] === 'string' ? args[0] : args[0] instanceof Request ? args[0].url : String(args[0]);
      if (res.status === 401 && url.includes('/api/')) {
        setState({ status: 'unauthenticated' });
      }
      return res;
    };
    return () => {
      cancelled = true;
      window.fetch = originalFetch;
    };
  }, []);

  return state;
}
