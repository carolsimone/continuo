import { useEffect, useState } from 'react';
import { ScheduleSummary, SchedulesResponse, ScheduleTopologySummary, TopologyListResponse } from './types';
import SchedulerCard from './SchedulerCard';
import SnapshotTile from './SnapshotTile';
import Tabs, { useActiveTab } from './Tabs';

export default function DashboardPage() {
  const [schedules, setSchedules] = useState<ScheduleSummary[]>([]);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [graphLoading, setGraphLoading] = useState(false);
  const [graphStatus, setGraphStatus] = useState<'idle' | 'success' | 'error'>('idle');
  const [graphError, setGraphError] = useState<string | null>(null);
  const [topologies, setTopologies] = useState<ScheduleTopologySummary[]>([]);
  const [topologiesError, setTopologiesError] = useState<string | null>(null);

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

  useEffect(() => {
    const fetch_ = () =>
      fetch('/api/topology/schedules')
        .then(r => r.json())
        .then((data: TopologyListResponse) => {
          setTopologies(data.schedules || []);
          setTopologiesError(null);
        })
        .catch(e => setTopologiesError(e.message));

    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => clearInterval(id);
  }, []);

  const handleUpdateGraph = () => {
    setGraphLoading(true);
    setGraphStatus('idle');
    setGraphError(null);
    fetch('/api/dashboard/graph-update', {
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

  const activeTab = useActiveTab('tab', 'runs', ['runs', 'topology']);

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
      <main className="page-content page-content--readable">
        <Tabs
          param="tab"
          defaultSlug="runs"
          tabs={[
            { slug: 'runs', label: 'Runs', count: schedules.length },
            { slug: 'topology', label: 'Topology', count: topologies.length },
          ]}
        />
        {activeTab === 'runs' && (
          <>
            {error && <div className="info-strip info-strip--error">{error}</div>}
            {schedules.length === 0 && !error && (
              <p className="empty">No schedules found.</p>
            )}
            {schedules.map(s => (
              <SchedulerCard key={s.schedule_name} schedule={s} />
            ))}
          </>
        )}
        {activeTab === 'topology' && (
          <>
            {topologiesError && (
              <div className="info-strip info-strip--error">
                <span className="info-strip__icon">⚠</span> {topologiesError}
              </div>
            )}
            {!topologiesError && topologies.length === 0 && (
              <div className="info-strip info-strip--neutral">
                <span className="info-strip__icon">ⓘ</span>
                No topology loaded yet — push a dbt manifest to populate.
              </div>
            )}
            {!topologiesError && topologies.length > 0 && (
              <div className="snapshot-tile-grid">
                {topologies.map((s) => (
                  <SnapshotTile key={s.schedule_name} summary={s} />
                ))}
              </div>
            )}
          </>
        )}
      </main>
    </div>
  );
}
