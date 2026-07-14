import { describe, it, expect } from 'vitest';
import { parseOperation } from '../../src/server/routes/operation';

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
