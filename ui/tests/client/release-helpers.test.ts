import { describe, it, expect } from 'vitest';
import { releasePillClass, reasonLabel, upcomingStages, shortSha } from '../../src/client/release-helpers';

describe('releasePillClass', () => {
  it('maps release lifecycle statuses to pill variants', () => {
    expect(releasePillClass('promoted')).toBe('pill--succeeded');
    expect(releasePillClass('rejected')).toBe('pill--failed');
    expect(releasePillClass('validating')).toBe('pill--running');
    expect(releasePillClass('parsing')).toBe('pill--running');
    expect(releasePillClass('compiling')).toBe('pill--running');
    expect(releasePillClass('seed_building')).toBe('pill--running');
    expect(releasePillClass('received')).toBe('pill--pending');
    expect(releasePillClass('superseded')).toBe('pill--cancelled');
  });

  it('maps passed (a verification run that judged its candidate good) to succeeded, not the pending fallback', () => {
    expect(releasePillClass('passed')).not.toBe('pill--pending');
    expect(releasePillClass('passed')).toBe('pill--succeeded');
  });

  it('maps per-node validation statuses (ok / failed) to pill variants', () => {
    expect(releasePillClass('ok')).toBe('pill--succeeded');
    expect(releasePillClass('failed')).toBe('pill--failed');
  });

  it('maps run-style keyword statuses', () => {
    expect(releasePillClass('succeeded')).toBe('pill--succeeded');
    expect(releasePillClass('running')).toBe('pill--running');
    expect(releasePillClass('cancelled')).toBe('pill--cancelled');
    expect(releasePillClass('pending')).toBe('pill--pending');
  });

  it('falls back to pending for unknown statuses', () => {
    expect(releasePillClass('weird')).toBe('pill--pending');
  });
});

describe('reasonLabel', () => {
  it('humanizes reject_reason stage tokens', () => {
    expect(reasonLabel('compile_failed')).toBe('Compilation');
    expect(reasonLabel('seed_build_failed')).toBe('Seed build');
    expect(reasonLabel('validation_failed')).toBe('Validation');
  });
  it('maps duplicate_table to prose, even though it carries no _failed suffix to strip', () => {
    expect(reasonLabel('duplicate_table')).toBe('Duplicate table');
  });
  it('falls back to the raw value for an unknown reason', () => {
    expect(reasonLabel('meteor_strike')).toBe('meteor_strike');
  });
});

describe('upcomingStages', () => {
  const at = '2026-09-05T14:25:36Z';
  it('lists the pipeline stages a run has not reached yet, in order', () => {
    expect(upcomingStages([{ to: 'received', at }, { to: 'compiling', at }]))
      .toEqual(['parsing', 'seed_building', 'validating']);
  });
  it('is empty once the run has left the pipeline (candidate or verification)', () => {
    expect(upcomingStages([{ to: 'received', at }, { to: 'validating', at }, { to: 'rejected', at }])).toEqual([]);
    expect(upcomingStages([{ to: 'received', at }, { to: 'validating', at }, { to: 'passed', at }])).toEqual([]);
    expect(upcomingStages([{ to: 'received', at }, { to: 'superseded', at }])).toEqual([]);
  });
  it('is empty when nothing has been recorded yet', () => {
    expect(upcomingStages([])).toEqual([]);
  });
});

describe('shortSha', () => {
  it('keeps the first seven characters', () => {
    expect(shortSha('abcdef1234567')).toBe('abcdef1');
    expect(shortSha('abc')).toBe('abc');
  });
});
