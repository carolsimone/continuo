import { describe, it, expect } from 'vitest';
import { getDriftState, getDriftMessage } from '../../src/client/drift-helpers';

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

describe('getDriftMessage — stale', () => {
  it('strip line shows run gen and latest gen', () => {
    expect(getDriftMessage('stale', 5, 7).stripLine).toBe(
      'Stale — pinned to topology gen 5; latest is 7. Rerun will use the older snapshot.',
    );
  });

  it('modal title is "Stale topology"', () => {
    expect(getDriftMessage('stale', 5, 7).modalTitle).toBe('Stale topology');
  });

  it('modal chip shows the gen delta arrow', () => {
    expect(getDriftMessage('stale', 5, 7).modalChip).toBe('5 → 7');
  });

  it('modal body uses singular "1 time" when delta is 1', () => {
    expect(getDriftMessage('stale', 6, 7).modalBody).toContain('re-ingested 1 time since this run started');
  });

  it('modal body uses plural "2 times" when delta is 2', () => {
    expect(getDriftMessage('stale', 5, 7).modalBody).toContain('re-ingested 2 times since this run started');
  });

  it('modal body includes the escape hatch sentence', () => {
    expect(getDriftMessage('stale', 5, 7).modalBody).toContain(
      'To run with the latest topology, trigger a fresh schedule run from the dashboard instead.',
    );
  });

  it('modal cta is "Rerun with old snapshot"', () => {
    expect(getDriftMessage('stale', 5, 7).modalCta).toBe('Rerun with old snapshot');
  });
});

describe('getDriftMessage — unknown', () => {
  it('strip line is the unknown copy', () => {
    expect(getDriftMessage('unknown', 0, 7).stripLine).toBe(
      'Topology version unknown for this run. May not match the current topology.',
    );
  });

  it('modal title is "Topology unknown"', () => {
    expect(getDriftMessage('unknown', 0, 7).modalTitle).toBe('Topology unknown');
  });

  it('modal chip is "no version recorded"', () => {
    expect(getDriftMessage('unknown', 0, 7).modalChip).toBe('no version recorded');
  });

  it('modal body explains pre-tracking and includes the escape hatch', () => {
    const body = getDriftMessage('unknown', 0, 7).modalBody;
    expect(body).toContain('This run started before topology tracking existed');
    expect(body).toContain('To guarantee a current run, trigger a fresh schedule run instead.');
  });

  it('modal cta is "Rerun anyway"', () => {
    expect(getDriftMessage('unknown', 0, 7).modalCta).toBe('Rerun anyway');
  });
});
