import { describe, it, expect } from 'vitest';
import { computeDepth, intraServiceOrder, buildSwimlaneLayout, laneLabelColumnWidth } from '../../src/client/run-graph-helpers';
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

describe('buildSwimlaneLayout', () => {
  const order = ['core', 'finance'];
  // core.seed(0) -> finance.rev(1) -> core.rollup(2)
  const ids = ['core.a.seed', 'finance.a.rev', 'core.a.rollup'];
  const edges = [e('core.a.seed', 'finance.a.rev'), e('finance.a.rev', 'core.a.rollup')];

  it('places a node at column = its depth in its service lane', () => {
    const L = buildSwimlaneLayout(ids, edges, order, new Set(), { col: 100, laneLeft: 100 });
    const rollup = L.nodes.find((n) => n.nodeId === 'core.a.rollup')!;
    expect(rollup.service).toBe('core');
    expect(rollup.depth).toBe(2);
    expect(rollup.x).toBe(100 + 2 * 100); // laneLeft + depth*col
  });

  it('a service spanning depths occupies multiple columns in one lane', () => {
    const L = buildSwimlaneLayout(ids, edges, order, new Set());
    const coreDepths = L.nodes.filter((n) => n.service === 'core').map((n) => n.depth).sort();
    expect(coreDepths).toEqual([0, 2]);
  });

  it('stacks two nodes in the same (service, depth) cell on different rows', () => {
    const twoIds = ['core.a.s1', 'core.a.s2'];
    const L = buildSwimlaneLayout(twoIds, [], order, new Set(), { row: 40 });
    const ys = L.nodes.map((n) => n.y);
    expect(new Set(ys).size).toBe(2); // distinct rows
  });

  it('a collapsed lane is shorter than an expanded one', () => {
    const expanded = buildSwimlaneLayout(ids, edges, order, new Set());
    const collapsed = buildSwimlaneLayout(ids, edges, order, new Set(['core']));
    const hExpanded = expanded.bands.find((b) => b.service === 'core')!.height;
    const hCollapsed = collapsed.bands.find((b) => b.service === 'core')!.height;
    expect(hCollapsed).toBeLessThan(hExpanded);
  });

  it('gives collapsed same-cell nodes distinct x positions so they do not overlap', () => {
    const twoIds = ['core.a.s1', 'core.a.s2']; // same service, same depth 0, no edges
    const L = buildSwimlaneLayout(twoIds, [], order, new Set(['core']));
    const coreNodes = L.nodes.filter((n) => n.service === 'core');
    expect(new Set(coreNodes.map((n) => n.y)).size).toBe(1); // collapsed → single row
    expect(new Set(coreNodes.map((n) => n.x)).size).toBe(2); // but distinct x, not stacked at one point
  });
});

describe('laneLabelColumnWidth', () => {
  it('grows with the longest service name so nodes never start under a label', () => {
    const short = laneLabelColumnWidth([{ service: 'core', done: 8, total: 8 }]);
    const long = laneLabelColumnWidth([{ service: 'service-py', done: 2, total: 2 }]);
    expect(long).toBeGreaterThan(short);
  });

  it('never shrinks below the default lane-left offset', () => {
    expect(laneLabelColumnWidth([{ service: 'a', done: 0, total: 1 }])).toBeGreaterThanOrEqual(104);
    expect(laneLabelColumnWidth([])).toBeGreaterThanOrEqual(104);
  });

  it('is driven by the widest label, not the first', () => {
    const w = laneLabelColumnWidth([
      { service: 'core', done: 8, total: 8 },
      { service: 'a-much-longer-service-name', done: 10, total: 12 },
    ]);
    expect(w).toBe(laneLabelColumnWidth([{ service: 'a-much-longer-service-name', done: 10, total: 12 }]));
  });
});
