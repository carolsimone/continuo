import { describe, it, expect } from 'vitest';
import { computeDepth, intraServiceOrder } from '../../src/client/run-graph-helpers';
import type { GraphEdge } from '../../src/client/types';

const e = (from: string, to: string): GraphEdge => ({ from_node_id: from, to_node_id: to });

describe('computeDepth', () => {
  it('chains deepen by one', () => {
    const d = computeDepth(['a', 'b', 'c'], [e('a', 'b'), e('b', 'c')]);
    expect(d).toEqual({ a: 0, b: 1, c: 2 });
  });

  it('a service that depends on downstream output lands at the deeper depth', () => {
    // core.seed (0) -> fin.rev (1) -> core.rollup (2): "core" spans depths 0 and 2
    const ids = ['core.a.seed', 'fin.a.rev', 'core.a.rollup'];
    const d = computeDepth(ids, [e('core.a.seed', 'fin.a.rev'), e('fin.a.rev', 'core.a.rollup')]);
    expect(d['core.a.seed']).toBe(0);
    expect(d['core.a.rollup']).toBe(2);
  });

  it('takes the longest path into a diamond', () => {
    const d = computeDepth(['a', 'b', 'c', 'd'], [e('a', 'b'), e('a', 'c'), e('b', 'd'), e('c', 'd')]);
    expect(d).toEqual({ a: 0, b: 1, c: 1, d: 2 });
  });

  it('ignores edges referencing unknown nodes', () => {
    const d = computeDepth(['a'], [e('ghost', 'a')]);
    expect(d).toEqual({ a: 0 });
  });

  it('terminates on an accidental cycle', () => {
    const d = computeDepth(['a', 'b'], [e('a', 'b'), e('b', 'a')]);
    expect(Number.isFinite(d.a)).toBe(true);
    expect(Number.isFinite(d.b)).toBe(true);
  });
});

describe('intraServiceOrder', () => {
  it('orders by dependency depth', () => {
    const ids = ['s.a.c', 's.a.b', 's.a.a'];
    const order = intraServiceOrder(ids, [e('s.a.a', 's.a.b'), e('s.a.b', 's.a.c')], {});
    expect(order).toEqual(['s.a.a', 's.a.b', 's.a.c']);
  });

  it('breaks ties (same depth) by started_at then id', () => {
    const ids = ['s.a.y', 's.a.x'];
    const order = intraServiceOrder(ids, [], { 's.a.x': '2026-01-01T00:00:02Z', 's.a.y': '2026-01-01T00:00:01Z' });
    expect(order).toEqual(['s.a.y', 's.a.x']); // y started first
  });

  it('places not-yet-started (null started_at) after started ones at the same depth', () => {
    const ids = ['s.a.p', 's.a.q'];
    const order = intraServiceOrder(ids, [], { 's.a.p': null, 's.a.q': '2026-01-01T00:00:01Z' });
    expect(order).toEqual(['s.a.q', 's.a.p']);
  });
});
