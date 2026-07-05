import { describe, it, expect } from 'vitest';
import { firstInFlight, IN_FLIGHT_STATUSES, releasePillClass, reasonLabel } from '../../src/client/release-helpers';
import { ReleaseListItem } from '../../src/client/types';

const mk = (id: string, status: string): ReleaseListItem => ({
  release_id: id, status, created_at: '', resolved_at: null, node_count: 0, bootstrap: false,
});

describe('firstInFlight', () => {
  it('returns the first non-terminal release', () => {
    const list = [mk('a', 'promoted'), mk('b', 'validating'), mk('c', 'received')];
    expect(firstInFlight(list)?.release_id).toBe('b');
  });
  it('returns null when all terminal', () => {
    expect(firstInFlight([mk('a', 'promoted'), mk('b', 'rejected')])).toBeNull();
  });
  it('IN_FLIGHT_STATUSES covers every active pipeline state in lifecycle order', () => {
    expect(IN_FLIGHT_STATUSES).toEqual(['received', 'compiling', 'parsing', 'seed_building', 'validating']);
  });
  it('treats compiling and seed_building as in flight', () => {
    expect(firstInFlight([mk('a', 'promoted'), mk('b', 'compiling')])?.release_id).toBe('b');
    expect(firstInFlight([mk('a', 'promoted'), mk('b', 'seed_building')])?.release_id).toBe('b');
  });
});

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
  it('falls back to the raw value for an unknown reason', () => {
    expect(reasonLabel('meteor_strike')).toBe('meteor_strike');
  });
});
