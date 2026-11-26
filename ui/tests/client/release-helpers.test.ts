import { describe, it, expect } from 'vitest';
import { releasePillClass, reasonLabel, upcomingStages, parseLines, shortSha } from '../../src/client/release-helpers';

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

  it('maps passed (a release check that judged its candidate good) to succeeded, not the pending fallback', () => {
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
  it('skips the compile leg and the seed build for a python release', () => {
    expect(upcomingStages(t('received'), { manifestKind: 'python' })).toEqual([
      { stage: 'parsing' },
      { stage: 'validating', condition: 'unless nothing changed' },
    ]);
  });
  it.each([
    ['dbt', ['compiling', 'parsing', 'seed_building', 'validating']],
    ['python', ['parsing', 'validating']],
    ['an-unknown-kind', ['compiling', 'parsing', 'seed_building', 'validating']],
  ])('lists the stages ahead of a received %s release', (manifestKind, stages) => {
    expect(upcomingStages(t('received'), { manifestKind }).map(s => s.stage)).toEqual(stages);
  });
  it.each([
    ['dbt', ['compiling', 'parsing']],
    ['python', ['parsing']],
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
  it('is empty once the release check has ended (a candidate release\'s or a fix\'s)', () => {
    expect(upcomingStages(t('received', 'validating', 'rejected'), { manifestKind: 'dbt' })).toEqual([]);
    expect(upcomingStages(t('received', 'validating', 'passed'), { manifestKind: 'dbt' })).toEqual([]);
    expect(upcomingStages(t('received', 'superseded'), { manifestKind: 'dbt' })).toEqual([]);
  });
  it('is empty when nothing has been recorded yet', () => {
    expect(upcomingStages([], { manifestKind: 'dbt' })).toEqual([]);
  });
});

describe('parseLines', () => {
  it('names the parse and the contract generation, with the contracts counted by family', () => {
    expect(parseLines({ parse_ms: 2140, contracts_ms: 610, contracts_by_family: { dbt: 45, python: 3 } })).toEqual([
      { label: 'parsing', text: 'parsing 2.1 s' },
      { label: 'generating contracts', text: 'generating contracts 0.6 s · 48\u00a0contracts (dbt 45, python 3)' },
    ]);
  });

  it('has no lines for a run that reported none of the three fields', () => {
    expect(parseLines({})).toEqual([]);
    expect(parseLines({ manifestKind: 'dbt', bootstrap: false })).toEqual([]);
  });

  it('names only the family that has contracts, and never prints a family with none', () => {
    expect(parseLines({ parse_ms: 900, contracts_ms: 300, contracts_by_family: { dbt: 45 } })[1].text)
      .toBe('generating contracts 0.3 s · 45\u00a0contracts (dbt 45)');
    expect(parseLines({ parse_ms: 900, contracts_ms: 300, contracts_by_family: { python: 3 } })[1].text)
      .toBe('generating contracts 0.3 s · 3\u00a0contracts (python 3)');
  });

  it('says 0 contracts, with no family list, for a release that has none', () => {
    expect(parseLines({ parse_ms: 900, contracts_ms: 300, contracts_by_family: {} })[1].text)
      .toBe('generating contracts 0.3 s · 0\u00a0contracts');
  });

  it('says 1 contract, not 1 contracts, for a release that has one', () => {
    expect(parseLines({ parse_ms: 900, contracts_ms: 300, contracts_by_family: { python: 1 } })[1].text)
      .toBe('generating contracts 0.3 s · 1\u00a0contract (python 1)');
  });

  it('shows the durations alone, with no count, when the parse did not count its contracts', () => {
    expect(parseLines({ parse_ms: 2140, contracts_ms: 610 })).toEqual([
      { label: 'parsing', text: 'parsing 2.1 s' },
      { label: 'generating contracts', text: 'generating contracts 0.6 s' },
    ]);
  });

  it('shows a line only for the duration the run reported', () => {
    expect(parseLines({ parse_ms: 2140 }).map(l => l.text)).toEqual(['parsing 2.1 s']);
    expect(parseLines({ contracts_ms: 610 }).map(l => l.text)).toEqual(['generating contracts 0.6 s']);
  });

  it('rounds seconds to one decimal, half up, whatever the binary form of the fraction', () => {
    const parsed = (ms: number) => parseLines({ parse_ms: ms })[0].text;
    expect(parsed(2140)).toBe('parsing 2.1 s');
    expect(parsed(2149)).toBe('parsing 2.1 s');
    // 2.15 and 2.05 are not exact in binary, so Number.toFixed(1) would round them down.
    expect(parsed(2150)).toBe('parsing 2.2 s');
    expect(parsed(2050)).toBe('parsing 2.1 s');
    expect(parsed(50)).toBe('parsing 0.1 s');
    expect(parsed(49)).toBe('parsing 0.0 s');
    expect(parsed(0)).toBe('parsing 0.0 s');
  });

  it('counts a long parse in seconds, not minutes', () => {
    expect(parseLines({ parse_ms: 125400 })[0].text).toBe('parsing 125.4 s');
    expect(parseLines({ contracts_ms: 3_600_000 })[0].text).toBe('generating contracts 3600.0 s');
  });
});

describe('shortSha', () => {
  it('keeps the first seven characters', () => {
    expect(shortSha('abcdef1234567')).toBe('abcdef1');
    expect(shortSha('abc')).toBe('abc');
  });
});
