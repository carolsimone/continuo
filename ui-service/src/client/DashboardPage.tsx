import { useEffect, useState } from 'react';
import { ScheduleSummary } from './types';
import SchedulerCard from './SchedulerCard';

export default function DashboardPage() {
  const [schedules, setSchedules] = useState<ScheduleSummary[]>([]);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetch_ = () =>
      fetch('/api/schedules')
        .then(r => r.json())
        .then(data => {
          setSchedules(data.schedules || []);
          setLastUpdated(new Date());
          setError(null);
        })
        .catch(e => setError(e.message));

    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Continuo</h1>
        <span className="live-badge">
          ● live{lastUpdated ? ` · ${lastUpdated.toLocaleTimeString()}` : ''}
        </span>
      </header>
      {error && <div className="error-banner">{error}</div>}
      <main>
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
