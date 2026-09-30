import { describe, it, expect } from 'vitest';
import { parseOperation, parseNodeOperation, parseNodeRunOperation } from '../../src/server/routes/operation';
import { NODE_RUN_OPERATIONS, RUN_OPERATIONS, isNodeOperation } from '../../src/server/shared/operation';

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

describe('parseNodeRunOperation', () => {
  it('accepts every single-node operation, full_refresh included', () => {
    expect(parseNodeRunOperation(undefined)).toBe('');
    expect(parseNodeRunOperation('run')).toBe('');
    expect(parseNodeRunOperation('test')).toBe('test');
    expect(parseNodeRunOperation('build')).toBe('build');
    expect(parseNodeRunOperation('full_refresh')).toBe('full_refresh');
  });
  it('rejects anything else with null', () => {
    expect(parseNodeRunOperation('FULL_REFRESH')).toBeNull();
    expect(parseNodeRunOperation(1)).toBeNull();
  });
});

describe('parseOperation keeps full_refresh out of schedule triggers', () => {
  it('rejects full_refresh', () => {
    expect(parseOperation('full_refresh')).toBeNull();
  });
});

describe('shared operation lists', () => {
  it('declares the run operations and the single-node operations once', () => {
    expect(RUN_OPERATIONS).toEqual(['run', 'test', 'build']);
    expect(NODE_RUN_OPERATIONS).toEqual(['run', 'test', 'build', 'full_refresh']);
  });
  it('isNodeOperation accepts exactly the single-node operations', () => {
    for (const op of NODE_RUN_OPERATIONS) expect(isNodeOperation(op)).toBe(true);
    for (const bad of ['', 'FULL_REFRESH', 'drop', undefined, null, 7]) expect(isNodeOperation(bad)).toBe(false);
  });
});
