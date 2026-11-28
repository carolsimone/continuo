import { useNavigate } from 'react-router';
import { ScheduleTopologySummary } from './types';

interface Props {
  summary: ScheduleTopologySummary;
  now?: Date;
}

function relativeTime(iso: string | null, now: Date): string {
  if (!iso) return '—';
  const then = new Date(iso).getTime();
  const ms = now.getTime() - then;
  if (ms < 0) return 'just now';
  const m = Math.floor(ms / 60000);
  if (m < 1) return 'just now';
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  const d = Math.floor(h / 24);
  return `${d}d ago`;
}

export default function ScheduleTile({ summary, now = new Date() }: Props) {
  const navigate = useNavigate();
  return (
    <button
      type="button"
      className="schedule-tile"
      onClick={() => navigate(`/schedule/${encodeURIComponent(summary.schedule_name)}/latest`)}
    >
      <div className="schedule-tile__name">{summary.schedule_name}</div>
      <div className="schedule-tile__meta">
        {summary.node_count} nodes · updated {relativeTime(summary.last_updated_at, now)}
      </div>
    </button>
  );
}
