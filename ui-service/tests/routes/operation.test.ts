import { describe, it, expect } from 'vitest';
import { parseOperation, parseNodeOperation } from '../../src/server/routes/operation';

describe('parseOperation', () => {
  it('maps missing / run to empty', () => {
    expect(parseOperation(undefined)).toBe('');
    expect(parseOperation('')).toBe('');
    expect(parseOperation('run')).toBe('');
  });
  it('passes test and build through', () => {
    expect(parseOperation('test')).toBe('test');
    expect(parseOperation('build')).toBe('build');
  });
  it('rejects anything else with null', () => {
    expect(parseOperation('drop')).toBeNull();
    expect(parseOperation(7)).toBeNull();
  });
});

describe('parseNodeOperation', () => {
  it('maps missing / empty / run to "run"', () => {
    expect(parseNodeOperation(undefined)).toBe('run');
    expect(parseNodeOperation('')).toBe('run');
    expect(parseNodeOperation('run')).toBe('run');
  });
  it('passes test and build through', () => {
    expect(parseNodeOperation('test')).toBe('test');
    expect(parseNodeOperation('build')).toBe('build');
  });
  it('rejects anything else with null', () => {
    expect(parseNodeOperation('drop')).toBeNull();
    expect(parseNodeOperation(7)).toBeNull();
  });
});
