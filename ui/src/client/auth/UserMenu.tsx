import { useState } from 'react';
import { useCurrentUser } from './AuthContext';

export default function UserMenu() {
  const user = useCurrentUser();
  const [busy, setBusy] = useState(false);
  if (!user) return null;

  async function signOut() {
    setBusy(true);
    try {
      const res = await fetch('/auth/logout', { method: 'POST' });
      const body = res.ok ? await res.json() : { redirectTo: '/' };
      window.location.href = body.redirectTo || '/';
    } catch {
      window.location.href = '/';
    }
  }

  return (
    <div className="user-menu">
      <span className="user-menu__email">{user.email}</span>
      <button
        type="button"
        className={`btn btn--secondary${busy ? ' is-loading' : ''}`}
        disabled={busy}
        onClick={signOut}
      >
        {busy ? 'Signing out…' : 'Sign out'}
      </button>
    </div>
  );
}
