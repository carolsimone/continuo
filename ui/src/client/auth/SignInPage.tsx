import Brand from '../Brand';

const ERROR_COPY: Record<string, string> = {
  no_role:
    'Your account has no continuo role assigned. Ask your administrator to map your group or email to a role.',
  login_failed: 'Sign-in failed. Please try again; if it persists, contact your administrator.',
};

export default function SignInPage() {
  const params = new URLSearchParams(window.location.search);
  const error = params.get('auth_error');
  const loginHref = `/auth/login?returnTo=${encodeURIComponent(window.location.pathname)}`;

  return (
    <div className="page">
      <header className="page-header">
        <Brand />
      </header>
      <main className="page-content">
        <div className="signin-card">
          <span className="section-header__title">Sign in</span>
          <p className="signin-card__hint">Use your organization account to continue.</p>
          {error && (
            <div className="info-strip info-strip--error">
              <span className="info-strip__icon">⚠</span>
              {ERROR_COPY[error] ?? ERROR_COPY.login_failed}
            </div>
          )}
          <a className="btn btn--primary" href={loginHref}>
            Sign in
          </a>
        </div>
      </main>
    </div>
  );
}
