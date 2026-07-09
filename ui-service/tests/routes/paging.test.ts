import { describe, it, expect } from 'vitest';
import { parseLimit, parseOffset } from '../../src/server/routes/paging';

describe('parseLimit', () => {
  const opts = { def: 200, max: 500 };

  it('returns the parsed value when in range', () => {
    expect(parseLimit('50', opts)).toBe(50);
  });

  it('clamps a value above max down to max', () => {
    expect(parseLimit('9999', opts)).toBe(500);
  });

  it('returns the default for a non-numeric value', () => {
    expect(parseLimit('abc', opts)).toBe(200);
  });

  it('returns the default for a negative value', () => {
    expect(parseLimit('-1', opts)).toBe(200);
  });

  it('returns the default for zero', () => {
    expect(parseLimit('0', opts)).toBe(200);
  });

  it('returns the default when absent', () => {
    expect(parseLimit(undefined, opts)).toBe(200);
  });

  it('accepts a value exactly at max', () => {
    expect(parseLimit('500', opts)).toBe(500);
  });
});

describe('parseOffset', () => {
  it('returns the parsed value', () => {
    expect(parseOffset('100')).toBe(100);
  });

  it('returns 0 for a negative value', () => {
    expect(parseOffset('-5')).toBe(0);
  });

  it('returns 0 for a non-numeric value', () => {
    expect(parseOffset('abc')).toBe(0);
  });

  it('returns 0 when absent', () => {
    expect(parseOffset(undefined)).toBe(0);
  });

  it('accepts zero', () => {
    expect(parseOffset('0')).toBe(0);
  });
});
