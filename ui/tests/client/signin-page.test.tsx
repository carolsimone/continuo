// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import SignInPage from '../../src/client/auth/SignInPage';

describe('SignInPage', () => {
  it('renders a sign-in link carrying the current path as returnTo', () => {
    window.history.replaceState({}, '', '/schedule/daily');
    render(<SignInPage />);
    const link = screen.getByRole('link', { name: /sign in/i });
    expect(link).toHaveAttribute('href', '/auth/login?returnTo=%2Fschedule%2Fdaily');
  });

  it('shows the no-role error strip for ?auth_error=no_role', () => {
    window.history.replaceState({}, '', '/?auth_error=no_role');
    render(<SignInPage />);
    expect(screen.getByText(/no continuo role/i)).toBeInTheDocument();
  });

  it('shows the generic error strip for ?auth_error=login_failed', () => {
    window.history.replaceState({}, '', '/?auth_error=login_failed');
    render(<SignInPage />);
    expect(screen.getByText(/sign-in failed/i)).toBeInTheDocument();
  });

  it('shows no error strip without the query parameter', () => {
    window.history.replaceState({}, '', '/');
    render(<SignInPage />);
    expect(screen.queryByText(/sign-in failed|no continuo role/i)).not.toBeInTheDocument();
  });
});
