import { describe, it, expect } from 'vitest';
import { kindLabel, computeNodeStats, formatDuration } from '../../src/client/node-helpers';
import type { NodeRun } from '../../src/client/types';

const mkRun = (over: Partial<NodeRun>): NodeRun => ({
  run_id: 'r', schedule_name: 's', kind: 'cron',
  terminal_status: 'succeeded', task_id: 't',
  task_status: 'succeeded', retry_count: 0,
  image_tag: 'v1', manifest_version: 'm1',
  created_at: '2026-05-10T10:00:00Z',
  started_at: '2026-05-10T10:00:05Z',
  completed_at: '2026-05-10T10:01:00Z',
  error_message: null, log_s3_key: null,
  ...over,
});

describe('kindLabel', () => {
  it.each([
    ['cron', 'Scheduled'],
    ['trigger', 'Manual trigger'],
    ['rerun', 'Manual rerun'],
    ['rebase', 'Manual rebase'],
    ['single_node_run', 'Manual node run'],
  ])('%s → %s', (kind, expected) => {
    expect(kindLabel(kind)).toBe(expected);
  });

  it('unknown kind falls back to the raw string', () => {
    expect(kindLabel('weird')).toBe('weird');
  });
});

describe('computeNodeStats', () => {
  it('returns 0/null stats when given an empty array', () => {
    expect(computeNodeStats([])).toEqual({ total: 0, successRatePct: null, avgDurationSec: null });
  });

  it('success rate ignores non-terminal task_status', () => {
    const runs = [
      mkRun({ task_status: 'succeeded' }),
      mkRun({ task_status: 'succeeded' }),
      mkRun({ task_status: 'failed' }),
      mkRun({ task_status: 'running', completed_at: null }),
    ];
    const stats = computeNodeStats(runs);
    expect(stats.total).toBe(4);
    expect(stats.successRatePct).toBe(67);
  });

  it('avg duration prefers started_at → completed_at', () => {
    const runs = [
      mkRun({
        started_at: '2026-05-10T10:00:00Z',
        completed_at: '2026-05-10T10:01:00Z',
      }),
      mkRun({
        started_at: '2026-05-10T10:00:00Z',
        completed_at: '2026-05-10T10:03:00Z',
      }),
      mkRun({ task_status: 'running', started_at: null, completed_at: null }),
    ];
    expect(computeNodeStats(runs).avgDurationSec).toBe(120);
  });
});

describe('formatDuration', () => {
  it.each([
    [0, '0s'],
    [45, '45s'],
    [90, '1m 30s'],
    [3661, '1h 1m'],
    [null, '—'],
  ])('%s → %s', (sec, expected) => {
    expect(formatDuration(sec as number | null)).toBe(expected);
  });
});
