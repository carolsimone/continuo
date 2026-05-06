import { describe, it, expect } from 'vitest';
import { getDriftState } from '../../src/client/drift-helpers';

describe('getDriftState', () => {
  it('returns "fresh" when run gen equals latest gen', () => {
    expect(getDriftState(7, 7)).toBe('fresh');
  });

  it('returns "stale" when run gen is below latest gen', () => {
    expect(getDriftState(5, 7)).toBe('stale');
  });

  it('returns "unknown" when run gen is 0 (pre-tracking run)', () => {
    expect(getDriftState(0, 7)).toBe('unknown');
  });

  it('returns "unknown" when both run gen and latest gen are 0', () => {
    expect(getDriftState(0, 0)).toBe('unknown');
  });

  it('returns "fresh" when run gen exceeds latest gen (defensive against invariant violation)', () => {
    expect(getDriftState(8, 7)).toBe('fresh');
  });

  it('returns "unknown" when run gen is undefined', () => {
    expect(getDriftState(undefined, 7)).toBe('unknown');
  });

  it('returns "unknown" when run gen is null', () => {
    expect(getDriftState(null, 7)).toBe('unknown');
  });

  it('returns "unknown" when run gen is NaN', () => {
    expect(getDriftState(NaN, 7)).toBe('unknown');
  });

  it('returns "fresh" when latest gen is null and run gen is positive (latest coerces to 0)', () => {
    expect(getDriftState(5, null)).toBe('fresh');
  });
});
