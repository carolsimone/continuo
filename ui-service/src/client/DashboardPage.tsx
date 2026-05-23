import { useEffect, useState } from 'react';
import { ScheduleSummary, SchedulesResponse } from './types';
import SchedulerCard from './SchedulerCard';

export default function DashboardPage() {
  const [schedules, setSchedules] = useState<ScheduleSummary[]>([]);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [graphLoading, setGraphLoading] = useState(false);
  const [graphStatus, setGraphStatus] = useState<'idle' | 'success' | 'error'>('idle');
  const [graphError, setGraphError] = useState<string | null>(null);

  useEffect(() => {
    const fetch_ = () =>
      fetch('/api/schedules')
        .then(r => r.json())
        .then((data: SchedulesResponse) => {
          setSchedules(data.schedules || []);
          setLastUpdated(new Date());
          setError(null);
        })
        .catch(e => setError(e.message));

    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => clearInterval(id);
  }, []);

  const handleUpdateGraph = () => {
    setGraphLoading(true);
    setGraphStatus('idle');
    setGraphError(null);
    fetch('/api/graph/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source: 's3' }),
    })
      .then(async r => {
        if (!r.ok) {
          const body = await r.json().catch(() => ({}));
          throw new Error(body.error || `HTTP ${r.status}`);
        }
        setGraphStatus('success');
        setTimeout(() => setGraphStatus('idle'), 3000);
      })
      .catch(err => {
        setGraphError(err.message);
        setGraphStatus('error');
        setTimeout(() => {
          setGraphError(null);
          setGraphStatus('idle');
        }, 5000);
      })
      .finally(() => setGraphLoading(false));
  };

  const graphBtnLabel = graphLoading
    ? 'Updating…'
    : graphStatus === 'success'
    ? 'Updated'
    : 'Update Graph';

  const graphBtnClass = [
    'btn',
    'btn--secondary',
    graphLoading ? 'is-loading' : '',
    graphStatus === 'success' ? 'is-success' : '',
  ].filter(Boolean).join(' ');

  return (
    <div className="page">
      <header className="page-header">
        <h1>Continuo</h1>
        <div className="page-actions">
          <span className="live-badge">
            ● live{lastUpdated ? ` · ${lastUpdated.toLocaleTimeString()}` : ''}
          </span>
          <button
            className={graphBtnClass}
            disabled={graphLoading}
            onClick={handleUpdateGraph}
            title="Reload graph from S3 manifests"
          >
            {graphBtnLabel}
          </button>
        </div>
      </header>
      {graphError && <div className="info-strip info-strip--error">{graphError}</div>}
      {error && <div className="info-strip info-strip--error">{error}</div>}
      <main className="page-content page-content--readable">
        {schedules.length === 0 && !error && (
          <p className="empty">No schedules found.</p>
        )}
        {schedules.map(s => (
          <SchedulerCard key={s.schedule_name} schedule={s} />
        ))}
      </main>
    </div>
  );
}
