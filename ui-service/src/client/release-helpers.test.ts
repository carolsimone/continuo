import { describe, it, expect } from 'vitest';
import { groupByStage, stageLabel, proposalKey } from './release-helpers';
import { NodeValidationResult } from './types';

const n = (stage: string, node_id: string): NodeValidationResult => ({
  stage, node_id, status: 'failed',
});

describe('groupByStage', () => {
  it('orders compile → seed_build → validation and preserves node order within a stage', () => {
    const groups = groupByStage([
      n('validation', 'v1'), n('compile', 'c1'), n('validation', 'v2'), n('seed_build', 's1'),
    ]);
    expect(groups.map(g => g.stage)).toEqual(['compile', 'seed_build', 'validation']);
    expect(groups[2].nodes.map(x => x.node_id)).toEqual(['v1', 'v2']);
  });

  it('emits a section only for stages that have results', () => {
    expect(groupByStage([n('compile', 'c')]).map(g => g.stage)).toEqual(['compile']);
    expect(groupByStage([n('validation', 'v')]).map(g => g.stage)).toEqual(['validation']);
  });

  it('appends unknown stages after the known ones, first-seen order', () => {
    expect(groupByStage([n('mystery', 'm'), n('compile', 'c')]).map(g => g.stage))
      .toEqual(['compile', 'mystery']);
  });
});

describe('stageLabel', () => {
  it('maps known literals to display labels and falls back to the raw value', () => {
    expect(stageLabel('compile')).toBe('Compilation');
    expect(stageLabel('seed_build')).toBe('Seed');
    expect(stageLabel('validation')).toBe('Validation');
    expect(stageLabel('mystery')).toBe('mystery');
  });
});

describe('proposalKey', () => {
  it('joins stage and node_id', () => {
    expect(proposalKey('compile', 'svc')).toBe('compile|svc');
    expect(proposalKey('validation', 'model.svc.x')).toBe('validation|model.svc.x');
  });
});
