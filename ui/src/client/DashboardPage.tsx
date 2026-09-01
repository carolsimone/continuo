import { useEffect, useState } from 'react';
import { ScheduleSummary, SchedulesResponse, ScheduleTopologySummary, TopologyListResponse } from './types';
import SchedulerCard from './SchedulerCard';
import SnapshotTile from './SnapshotTile';
import Tabs, { useActiveTab } from './Tabs';
import ReleasesPanel from './ReleasesPanel';
import NodesCatalogPanel from './NodesCatalogPanel';
import RemediationPanel from './RemediationPanel';
import UserMenu from './auth/UserMenu';
import { fetchProposals } from './remediation-api';
import Brand from './Brand';

export default function DashboardPage() {
  const [schedules, setSchedules] = useState<ScheduleSummary[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [topologies, setTopologies] = useState<ScheduleTopologySummary[]>([]);
  const [topologiesError, setTopologiesError] = useState<string | null>(null);
  const [nodeTotal, setNodeTotal] = useState(0);
  const [openPrCount, setOpenPrCount] = useState(0);

  useEffect(() => {
    const fetch_ = () =>
      fetch('/api/schedules')
        .then(r => r.json())
        .then((data: SchedulesResponse) => {
          setSchedules(data.schedules || []);
          setLoaded(true);
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

  const tabSpecs = [
    { slug: 'runs', label: 'Runs', count: schedules.length },
    { slug: 'topology', label: 'Topology', count: topologies.length },
    { slug: 'releases', label: 'Releases' },
    { slug: 'nodes', label: 'Nodes', count: nodeTotal },
    { slug: 'remediation', label: 'Remediation', count: openPrCount > 0 ? openPrCount : undefined },
  ];
  const activeTab = useActiveTab('tab', 'runs', tabSpecs.map(t => t.slug));

  // Nodes-tab count badge: fetched on mount and whenever a tab is opened.
  // No recurring poll — the full catalog aggregation must not run every 5s.
  useEffect(() => {
    fetch('/api/nodes?limit=1')
      .then(r => r.json())
      .then((data: { total_count?: number }) => setNodeTotal(data.total_count || 0))
      .catch(() => {});
  }, [activeTab]);

  // Remediation-tab count badge: open remediation PRs — proposals whose PR is
  // up on GitHub and awaiting a human (pr_state='open'). The server filters, so
  // the count is the returned list length. The pill is omitted when it is zero.
  // Fetched on mount only; no poll — open-PR counts change only when the
  // close-loop reconciler runs, and proposal arrivals are infrequent.
  useEffect(() => {
    fetchProposals({ pr_state: 'open' })
      .then(proposals => setOpenPrCount((proposals || []).length))
      .catch(() => {});
  }, []);

  // Connection liveness, not a wall clock: reflects whether the dashboard is
  // still receiving data from the schedules poll. 'live' once a poll has
  // succeeded, 'reconnecting' after a failed poll, 'connecting' before the
  // first response lands.
  const liveState = error ? 'reconnecting' : loaded ? 'live' : 'connecting';
  const liveLabel =
    liveState === 'live' ? 'Live'
      : liveState === 'reconnecting' ? 'Reconnecting…'
      : 'Connecting…';

  return (
    <div className="page">
      <header className="page-header">
        <h1><Brand /></h1>
        <div className="page-actions">
          <span className={`live-badge live-badge--${liveState}`}>
            ● {liveLabel}
          </span>
          <UserMenu />
        </div>
      </header>
      <main className="page-content page-content--readable">
        <Tabs param="tab" defaultSlug="runs" tabs={tabSpecs} />
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
        {activeTab === 'releases' && <ReleasesPanel />}
        {activeTab === 'nodes' && <NodesCatalogPanel />}
        {activeTab === 'remediation' && <RemediationPanel />}
      </main>
    </div>
  );
}
