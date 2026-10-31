import { formatDate, formatStatusLabel } from '../detail-page-helpers';

// The strip shown while a past run is chosen: which snapshot the page shows
// and the way back to the latest run.
export default function SnapshotStrip({ run, liveRunExists, onReturn }: {
  run: { completed_at: string | null; created_at: string | null; status: string | null };
  liveRunExists: boolean;
  onReturn: () => void;
}) {
  return (
    <div className="info-strip info-strip--warning">
      <span className="info-strip__icon">⚠</span>
      <span>
        Viewing snapshot from {formatDate(run.completed_at ?? run.created_at)}.
      </span>
      <span>Status: {formatStatusLabel(run.status)}</span>
      <button
        type="button"
        className="btn btn--secondary info-strip__action"
        onClick={onReturn}
      >
        {liveRunExists ? 'Return to live run' : 'Back to latest run'}
      </button>
    </div>
  );
}
