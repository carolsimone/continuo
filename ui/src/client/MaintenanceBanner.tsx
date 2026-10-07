// MaintenanceBanner tells everyone using the dashboard that continuo is in
// maintenance mode, so a refused trigger or release is expected, not a fault.
export default function MaintenanceBanner() {
  return (
    <div className="info-strip info-strip--warning maintenance-banner" role="status">
      <span className="info-strip__icon" aria-hidden="true">⚠</span>
      <span>
        continuo is in maintenance mode: new runs, triggers, releases and remediation retries are
        refused until it is turned off. Work already running will finish.
      </span>
    </div>
  );
}
