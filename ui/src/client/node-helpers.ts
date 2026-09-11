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
  p95DurationSec: number | null;
  flakyRatePct: number;
  lastStatus: string | null;
  lastRunAt: string | null;
}

function isTerminal(r: NodeRun): boolean {
  const s = r.task_status;
  return s === 'succeeded' || s === 'failed' || s === 'cancelled' || s === 'skipped';
}

function durationSec(r: NodeRun): number | null {
  if (!r.started_at || !r.completed_at) return null;
  const ms = new Date(r.completed_at).getTime() - new Date(r.started_at).getTime();
  if (Number.isNaN(ms) || ms < 0) return null;
  return ms / 1000;
}

// Linear-interpolation percentile matching Postgres PERCENTILE_CONT, so the
// node-detail header agrees with the catalog list (which computes p95 in SQL).
function percentileCont(sortedAsc: number[], p: number): number {
  if (sortedAsc.length === 1) return sortedAsc[0];
  const pos = p * (sortedAsc.length - 1);
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sortedAsc[lo] + (pos - lo) * (sortedAsc[hi] - sortedAsc[lo]);
}

export function computeNodeStats(runs: NodeRun[]): NodeStats {
  if (runs.length === 0) {
    return { total: 0, successRatePct: null, avgDurationSec: null,
             p95DurationSec: null, flakyRatePct: 0, lastStatus: null, lastRunAt: null };
  }

  const byRecent = [...runs].sort(
    (a, b) => new Date(b.created_at ?? 0).getTime() - new Date(a.created_at ?? 0).getTime());
  const last = byRecent[0];

  const terminal = runs.filter(isTerminal);
  const successRatePct = terminal.length === 0
    ? null
    : Math.round(terminal.filter(r => r.task_status === 'succeeded').length / terminal.length * 100);

  const durs = terminal.map(durationSec).filter((d): d is number => d !== null).sort((a, b) => a - b);
  const avgDurationSec = durs.length === 0 ? null : Math.round(durs.reduce((a, b) => a + b, 0) / durs.length);
  const p95DurationSec = durs.length === 0 ? null : Math.round(percentileCont(durs, 0.95));

  const flakyRatePct = Math.round(runs.filter(r => r.retry_count > 0).length / runs.length * 100);

  return {
    total: runs.length, successRatePct, avgDurationSec, p95DurationSec, flakyRatePct,
    lastStatus: last.task_status || null, lastRunAt: last.created_at,
  };
}

// One distinct past snapshot the node can be re-run against — an
// (image_tag, manifest_version) pair, plus the runs that used it.
export interface SnapshotGroup {
  imageTag: string;
  manifestVersion: string;
  runCount: number;             // eligible runs sharing this snapshot
  representativeRunId: string;  // the run the trigger executes against
  status: string;               // task_status of the representative run
  lastRunAt: string | null;     // created_at of the representative run
}

// Stale-mode (snapshot_of_run) eligibility mirrors state.TriggerSingleNodeRun's
// validation: the source RUN must be terminal (scheduler-level status), not the
// per-task status on this node. A FAILED run where this node stayed PENDING is a
// valid source; an in-flight run where this node already succeeded is NOT.
export function isSnapshotSourceEligible(r: NodeRun): boolean {
  const s = r.terminal_status;
  return s === 'succeeded' || s === 'failed' || s === 'cancelled';
}

// Collapse a node's runs into the distinct snapshots it can be re-run against.
// Runs that share an (image_tag, manifest_version) pair are one snapshot; the
// snapshot's representative is its most-recent terminal run, which is the run the
// stale-mode trigger executes against. Groups come back newest-snapshot-first.
export function groupRunsBySnapshot(runs: NodeRun[]): SnapshotGroup[] {
  const eligible = runs.filter(isSnapshotSourceEligible);
  const byRecent = [...eligible].sort(
    (a, b) => new Date(b.created_at ?? 0).getTime() - new Date(a.created_at ?? 0).getTime());

  const groups = new Map<string, SnapshotGroup>();
  for (const r of byRecent) {
    // A JSON tuple keys the map so distinct (tag, version) pairs never collide.
    const key = JSON.stringify([r.image_tag, r.manifest_version]);
    const existing = groups.get(key);
    if (existing) {
      existing.runCount += 1;
      continue;
    }
    // byRecent is newest-first, so the first run seen for a key is its representative.
    groups.set(key, {
      imageTag: r.image_tag,
      manifestVersion: r.manifest_version,
      runCount: 1,
      representativeRunId: r.run_id,
      status: r.task_status,
      lastRunAt: r.created_at,
    });
  }
  return [...groups.values()];
}

export function formatRelative(iso: string | null, now: Date = new Date()): string {
  if (!iso) return '—';
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return '—';
  const sec = Math.floor((now.getTime() - t) / 1000);
  if (sec < 60) return 'now';
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
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
