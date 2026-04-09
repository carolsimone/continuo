import { RunSummary } from './types';

interface Props {
  runs: RunSummary[];
  liveRunId: string | null;
  liveStatus: string | null;
  selectedRunId: string | null;
  onSelectRun: (runId: string | null) => void;
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

function pillClass(status: string): string {
  const s = status.toLowerCase().replace(/\s+/g, '-');
  if (s.includes('succeed')) return 'pill-sm--succeeded';
  if (s.includes('fail')) return 'pill-sm--failed';
  if (s.includes('cancel')) return 'pill-sm--cancelled';
  return 'pill-sm--pending';
}

function dotClass(status: string): string {
  const s = status.toLowerCase();
  if (s.includes('succeed')) return 'run-dot--ok';
  if (s.includes('fail')) return 'run-dot--fail';
  return 'run-dot--current';
}

export default function PastRunsPanel({ runs, liveRunId, liveStatus, selectedRunId, onSelectRun }: Props) {
  const isLiveSelected = selectedRunId === null;

  return (
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
            <div className={`run-dot ${dotClass(r.terminal_status)}`} />
            <div className="run-meta">
              <div className="run-date">{formatDate(r.created_at)}</div>
              <div className="run-duration">{formatDuration(r.created_at, r.completed_at)}</div>
            </div>
            <span className={`pill-sm ${pillClass(r.terminal_status)}`}>
              {r.terminal_status.toLowerCase()}
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
  );
}
