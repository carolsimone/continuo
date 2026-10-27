import { useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router';
import PageHeader from './PageHeader';

// RunRedirect opens a run addressed by its id alone (/run/:id): it reads the
// run's schedule from state and replaces itself with that schedule's run page,
// opened on this run. A run that cannot be read is named in an error strip.
export default function RunRedirect() {
  const { id = '' } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setError(null);
    fetch(`/api/schedulers/${encodeURIComponent(id)}`)
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error(`HTTP ${response.status}`))))
      .then((run: { schedule_name?: string }) => {
        if (cancelled) return;
        if (!run.schedule_name) throw new Error('the run names no schedule');
        navigate(`/schedule/${encodeURIComponent(run.schedule_name)}`, {
          replace: true,
          state: { last_run_id: id },
        });
      })
      .catch((err: Error) => {
        if (!cancelled) setError(err.message);
      });
    return () => { cancelled = true; };
  }, [id, navigate]);

  return (
    <div className="page">
      <PageHeader>
        <div className="detail-page-title">Run {id.slice(0, 8)}</div>
      </PageHeader>
      <main className="page-content">
        {error ? (
          <div className="info-strip info-strip--error">
            <span className="info-strip__icon" aria-hidden="true">⚠</span>Run {id} could not be opened: {error}
          </div>
        ) : (
          <p className="empty">Opening run…</p>
        )}
      </main>
    </div>
  );
}
