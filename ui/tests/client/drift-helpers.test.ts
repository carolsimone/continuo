import { describe, it, expect } from 'vitest';
import { getDriftState, getDriftBadge } from '../../src/client/drift-helpers';

describe('getDriftState', () => {
  it('returns "fresh" when the run seq equals the live seq', () => {
    expect(getDriftState(7, 7)).toBe('fresh');
  });

  it('returns "stale" when the run seq is below the live seq', () => {
    expect(getDriftState(5, 7)).toBe('stale');
  });

  it('returns "unknown" when the run seq is 0 (the run carries no seq)', () => {
    expect(getDriftState(0, 7)).toBe('unknown');
  });

  it('returns "unknown" when both seqs are 0', () => {
    expect(getDriftState(0, 0)).toBe('unknown');
  });

  it('returns "fresh" when the run seq exceeds the live seq (defensive against invariant violation)', () => {
    expect(getDriftState(8, 7)).toBe('fresh');
  });

  it('returns "unknown" when the run seq is undefined', () => {
    expect(getDriftState(undefined, 7)).toBe('unknown');
  });

  it('returns "unknown" when the run seq is null', () => {
    expect(getDriftState(null, 7)).toBe('unknown');
  });

  it('returns "unknown" when the run seq is NaN', () => {
    expect(getDriftState(NaN, 7)).toBe('unknown');
  });

  it('returns "fresh" when the live seq is null and the run seq is positive (live coerces to 0)', () => {
    expect(getDriftState(5, null)).toBe('fresh');
  });
});

describe('getDriftBadge', () => {
  it('counts the releases between the run and the live seq', () => {
    expect(getDriftBadge('stale', 5, 8)).toBe('source 3 releases behind latest');
  });

  it('uses the singular for one release', () => {
    expect(getDriftBadge('stale', 7, 8)).toBe('source 1 release behind latest');
  });
});
