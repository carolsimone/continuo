import { describe, it, expect } from 'vitest';
import { kindLabel, computeNodeStats, formatDuration, formatRelative } from '../../src/client/node-helpers';
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
    expect(computeNodeStats([])).toEqual({
      total: 0, successRatePct: null, avgDurationSec: null,
      p95DurationSec: null, flakyRatePct: 0, lastStatus: null, lastRunAt: null,
    });
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

describe('formatRelative', () => {
  const now = new Date('2026-06-08T12:00:00Z');
  it('buckets seconds/minutes/hours/days', () => {
    expect(formatRelative('2026-06-08T11:59:30Z', now)).toBe('now');
    expect(formatRelative('2026-06-08T11:48:00Z', now)).toBe('12m ago');
    expect(formatRelative('2026-06-08T09:00:00Z', now)).toBe('3h ago');
    expect(formatRelative('2026-06-06T12:00:00Z', now)).toBe('2d ago');
  });
  it('returns dash for null/invalid', () => {
    expect(formatRelative(null, now)).toBe('—');
    expect(formatRelative('nonsense', now)).toBe('—');
  });
});

describe('computeNodeStats — extended fields', () => {
  const mkRunExt = (over: Partial<NodeRun>): NodeRun => ({
    run_id: 'r', schedule_name: 's', kind: 'cron', terminal_status: '',
    task_id: Math.random().toString(), task_status: 'succeeded', retry_count: 0,
    image_tag: '', manifest_version: '', created_at: '2026-06-08T11:00:00Z',
    started_at: '2026-06-08T11:00:00Z', completed_at: '2026-06-08T11:00:10Z',
    error_message: null, log_s3_key: null, ...over,
  });
  it('computes p95, flaky rate, last status/run', () => {
    const runs = [
      mkRunExt({ task_status: 'failed', retry_count: 2, created_at: '2026-06-08T11:30:00Z',
              started_at: '2026-06-08T11:30:00Z', completed_at: '2026-06-08T11:30:30Z' }),
      mkRunExt({ task_status: 'succeeded', retry_count: 0 }),
    ];
    const s = computeNodeStats(runs);
    expect(s.total).toBe(2);
    expect(s.successRatePct).toBe(50);
    expect(s.p95DurationSec).toBe(30);
    expect(s.flakyRatePct).toBe(50);
    expect(s.lastStatus).toBe('failed');             // most recent by created_at
    expect(s.lastRunAt).toBe('2026-06-08T11:30:00Z');
  });
  it('empty runs -> null stats', () => {
    const s = computeNodeStats([]);
    expect(s.total).toBe(0);
    expect(s.successRatePct).toBeNull();
    expect(s.p95DurationSec).toBeNull();
    expect(s.flakyRatePct).toBe(0);
    expect(s.lastStatus).toBeNull();
  });
});
