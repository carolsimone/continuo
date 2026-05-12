import type { NodeRun } from './types';

export function kindLabel(kind: string): string {
  switch (kind) {
    case 'cron':            return 'Scheduled';
    case 'trigger':         return 'Manual trigger';
    case 'rerun':           return 'Manual rerun';
    case 'rebase':          return 'Manual rebase';
    case 'single_node_run': return 'Manual node run';
    default:                return kind;
  }
}

export interface NodeStats {
  total: number;
  successRatePct: number | null;
  avgDurationSec: number | null;
}

function isTerminal(r: NodeRun): boolean {
  const s = r.task_status;
  return s === 'succeeded' || s === 'failed' || s === 'cancelled';
}

export function computeNodeStats(runs: NodeRun[]): NodeStats {
  if (runs.length === 0) return { total: 0, successRatePct: null, avgDurationSec: null };

  const terminal = runs.filter(isTerminal);
  if (terminal.length === 0) {
    return { total: runs.length, successRatePct: null, avgDurationSec: null };
  }

  const succeeded = terminal.filter(r => r.task_status === 'succeeded').length;
  const successRatePct = Math.round((succeeded / terminal.length) * 100);

  let durSum = 0;
  let durCount = 0;
  for (const r of terminal) {
    if (!r.started_at || !r.completed_at) continue;
    const ms = new Date(r.completed_at).getTime() - new Date(r.started_at).getTime();
    if (Number.isNaN(ms) || ms < 0) continue;
    durSum += ms;
    durCount++;
  }
  const avgDurationSec = durCount === 0 ? null : Math.round(durSum / durCount / 1000);

  return { total: runs.length, successRatePct, avgDurationSec };
}

export function formatDuration(sec: number | null): string {
  if (sec === null || sec === undefined) return '—';
  if (sec < 60) return `${sec}s`;
  if (sec < 3600) {
    const m = Math.floor(sec / 60);
    const s = sec % 60;
    return `${m}m ${s}s`;
  }
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  return `${h}h ${m}m`;
}
