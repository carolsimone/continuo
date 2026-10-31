import PageHeader from '../PageHeader';
import { formatStatusLabel, headerDriftLabel, pillClass, type DetailMode } from '../detail-page-helpers';

// The page header: back link, schedule name, the status pill of the run the
// page shows, and the notes about it (why it failed, how far behind the live
// topology it ran, the live promotion in latest mode).
export default function DetailHeader({
  name, status, failureLabel, driftLabel, mode, promotionSeq, onBack,
}: {
  name: string | undefined;
  status: string;
  failureLabel: string | null;
  driftLabel: string | null;
  mode: DetailMode;
  promotionSeq: number;
  onBack: () => void;
}) {
  return (
    <PageHeader>
      <button className="detail-back-link" onClick={onBack}>
        ← Back
      </button>
      <div className="detail-scheduler-name">{name ?? 'Loading…'}</div>
      <span className={`pill ${pillClass(status)}`}>{formatStatusLabel(status)}</span>
      {failureLabel && (
        <span className="info-strip info-strip--error info-strip--inline" aria-label="run failure reason">
          {failureLabel}
        </span>
      )}
      {headerDriftLabel({ mode, driftLabel }) && (
        <span className="info-strip info-strip--warning info-strip--inline">
          <span className="info-strip__icon">⚠</span>
          {driftLabel}
        </span>
      )}
      {mode === 'latest' && promotionSeq > 0 && (
        <span className="info-strip info-strip--info info-strip--inline" aria-label="live promotion seq">
          <span className="info-strip__icon">ⓘ</span>
          promotion {promotionSeq}
        </span>
      )}
    </PageHeader>
  );
}
