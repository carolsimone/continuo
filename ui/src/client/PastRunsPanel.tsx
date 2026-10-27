import { RunSummary } from './types';
import type { RunStatus } from '../server/generated/vocabulary.gen';

interface Props {
  runs: RunSummary[];
  liveRunId: string | null;
  liveStatus: string | null;
  selectedRunId: string | null;
  onSelectRun: (runId: string | null) => void;
  // Paging is optional: the pager renders only when the parent supplies all
  // four and the runs do not fit on one page.
  page?: number;
  pageSize?: number;
  total?: number;
  onPageChange?: (page: number) => void;
}

function formatDate(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime())
    ? iso
    : d.toLocaleString('en-GB', {
        day: '2-digit',
        month: 'short',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      }).replace(',', '');
}

function formatDuration(createdAt: string | null, completedAt: string | null): string {
  if (!createdAt || !completedAt) return '—';
  const ms = new Date(completedAt).getTime() - new Date(createdAt).getTime();
  if (Number.isNaN(ms) || ms < 0) return '—';
  const totalSec = Math.floor(ms / 1000);
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

function pillClass(status: RunStatus): string {
  switch (status) {
    case 'succeeded': return 'pill-sm--succeeded';
    case 'failed': return 'pill-sm--failed';
    case 'cancelling': return 'pill-sm--cancelling';
    case 'cancelled': return 'pill-sm--cancelled';
    case 'skipped': return 'pill-sm--skipped';
    default: return 'pill-sm--pending';
  }
}

function dotClass(status: RunStatus): string {
  switch (status) {
    case 'succeeded': return 'run-dot--ok';
    case 'failed': return 'run-dot--fail';
    case 'skipped': return 'run-dot--skipped';
    default: return 'run-dot--current';
  }
}

export default function PastRunsPanel({
  runs, liveRunId, liveStatus, selectedRunId, onSelectRun, page, pageSize, total, onPageChange,
}: Props) {
  const isLiveSelected = selectedRunId === null;
  const paged = page !== undefined && pageSize !== undefined && total !== undefined && onPageChange !== undefined;
  const showPager = paged && total > pageSize;

  return (
    <>
    <div className="runs-list">
      {liveRunId && (
        <button
          type="button"
          className={`run-row${isLiveSelected ? ' run-row--selected' : ''}`}
          onClick={() => onSelectRun(null)}
        >
          <div className="run-dot run-dot--current" />
          <div className="run-meta">
            <div className="run-date">
              Current run&nbsp;
              <span style={{ color: '#94a3b8', fontSize: 11, fontWeight: 400 }}>
                {liveStatus ?? 'in progress'}
              </span>
            </div>
          </div>
          <span className="pill-sm pill-sm--current">current</span>
        </button>
      )}

      {runs.map(r => {
        const isSelected = selectedRunId === r.run_id;
        return (
          <button
            type="button"
            key={r.run_id}
            className={`run-row${isSelected ? ' run-row--selected' : ''}`}
            onClick={() => onSelectRun(isSelected ? null : r.run_id)}
          >
            <div className={`run-dot ${dotClass(r.status)}`} />
            <div className="run-meta">
              <div className="run-date">{formatDate(r.created_at)}</div>
              <div className="run-duration">{formatDuration(r.created_at, r.completed_at)}</div>
            </div>
            <span className={`pill-sm ${pillClass(r.status)}`}>
              {r.status}
            </span>
          </button>
        );
      })}

      {!liveRunId && runs.length === 0 && (
        <p style={{ padding: 16, color: '#94a3b8', fontSize: 12, textAlign: 'center', margin: 0 }}>
          No runs yet.
        </p>
      )}
    </div>
    {showPager && (
      <div className="runs-pager">
        <button type="button" className="btn btn--secondary" disabled={page === 0}
                onClick={() => onPageChange(page - 1)}>
          Newer
        </button>
        <span className="runs-pager__range">
          {page * pageSize + 1}–{Math.min((page + 1) * pageSize, total)} of {total}
        </span>
        <button type="button" className="btn btn--secondary" disabled={(page + 1) * pageSize >= total}
                onClick={() => onPageChange(page + 1)}>
          Older
        </button>
      </div>
    )}
    </>
  );
}
