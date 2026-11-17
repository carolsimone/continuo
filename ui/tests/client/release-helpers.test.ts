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
  const t = (...to: string[]) => to.map(stage => ({ to: stage, at }));

  it('previews the rest of a dbt release, naming the condition on the stages that may not run', () => {
    expect(upcomingStages(t('received', 'compiling'), { manifestKind: 'dbt' })).toEqual([
      { stage: 'parsing' },
      { stage: 'seed_building', condition: 'if seeds changed' },
      { stage: 'validating', condition: 'unless nothing changed' },
    ]);
  });
  it.each(['python', 'python-odcs'])('skips the compile leg and the seed build for a %s release', manifestKind => {
    expect(upcomingStages(t('received'), { manifestKind })).toEqual([
      { stage: 'parsing' },
      { stage: 'validating', condition: 'unless nothing changed' },
    ]);
  });
  it.each([
    ['dbt', ['compiling', 'parsing', 'seed_building', 'validating']],
    ['python', ['parsing', 'validating']],
    ['python-odcs', ['parsing', 'validating']],
    ['an-unknown-kind', ['compiling', 'parsing', 'seed_building', 'validating']],
  ])('lists the stages ahead of a received %s release', (manifestKind, stages) => {
    expect(upcomingStages(t('received'), { manifestKind }).map(s => s.stage)).toEqual(stages);
  });
  it.each([
    ['dbt', ['compiling', 'parsing']],
    ['python', ['parsing']],
    ['python-odcs', ['parsing']],
    ['an-unknown-kind', ['compiling', 'parsing']],
  ])('ends a bootstrap %s release at parsing, which promotes without validating', (manifestKind, stages) => {
    expect(upcomingStages(t('received'), { manifestKind, bootstrap: true }).map(s => s.stage)).toEqual(stages);
  });
  it('treats an unknown kind as dbt, the common one', () => {
    expect(upcomingStages(t('received', 'compiling', 'parsing'), {})).toEqual([
      { stage: 'seed_building', condition: 'if seeds changed' },
      { stage: 'validating', condition: 'unless nothing changed' },
    ]);
    expect(upcomingStages(t('received', 'compiling', 'parsing'), { manifestKind: 'an-unknown-kind' })).toEqual([
      { stage: 'seed_building', condition: 'if seeds changed' },
      { stage: 'validating', condition: 'unless nothing changed' },
    ]);
  });
  it('is empty once the run has left the pipeline (candidate or verification)', () => {
    expect(upcomingStages(t('received', 'validating', 'rejected'), { manifestKind: 'dbt' })).toEqual([]);
    expect(upcomingStages(t('received', 'validating', 'passed'), { manifestKind: 'dbt' })).toEqual([]);
    expect(upcomingStages(t('received', 'superseded'), { manifestKind: 'dbt' })).toEqual([]);
  });
  it('is empty when nothing has been recorded yet', () => {
    expect(upcomingStages([], { manifestKind: 'dbt' })).toEqual([]);
  });
});

describe('shortSha', () => {
  it('keeps the first seven characters', () => {
    expect(shortSha('abcdef1234567')).toBe('abcdef1');
    expect(shortSha('abc')).toBe('abc');
  });
});
