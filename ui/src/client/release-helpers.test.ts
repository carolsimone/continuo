import { describe, it, expect } from 'vitest';
import {
  groupByStage, stageLabel, reasonLabel, proposalKey,
  proposalNodeIds, proposalStatusForNode, proposalReasonForNode,
  releasePillClass, verificationPhase, verificationRunPhase, verificationRunIds,
} from './release-helpers';
import { NodeValidationResult } from './types';
import type { ProposalDTO } from './types';

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
    expect(stageLabel('seed_build')).toBe('Seed build');
    expect(stageLabel('validation')).toBe('Validation');
    expect(stageLabel('mystery')).toBe('mystery');
  });
});

describe('reasonLabel', () => {
  it('drops the _failed suffix and reuses the stage-label vocabulary', () => {
    expect(reasonLabel('compile_failed')).toBe('Compilation');
    expect(reasonLabel('seed_build_failed')).toBe('Seed build');
    expect(reasonLabel('validation_failed')).toBe('Validation');
  });

  it('maps duplicate_table to prose, even though it carries no _failed suffix', () => {
    expect(reasonLabel('duplicate_table')).toBe('Duplicate table');
  });

  it('falls back to the raw token for an unrecognized reason', () => {
    expect(reasonLabel('mystery_failed')).toBe('mystery');
  });
});

describe('proposalKey', () => {
  it('joins stage and node_id', () => {
    expect(proposalKey('compile', 'svc')).toBe('compile|svc');
    expect(proposalKey('validation', 'model.svc.x')).toBe('validation|model.svc.x');
  });
});

const base = {
  id: 'p', source: 'validation', release_id: 'r', node_id: 's.a', error_signature: 's', attempt: 1,
  status: 'verifying', confidence: 'high', rationale: 'overall', proposed_sql_uri: '', diff_uri: '', candidate_fix_sql_uri: '',
  candidate_fix_diff_uri: '', source_resolved: true, repo: '', commit_sha: '', file_path: '', model: '', created_at: '',
  pr_url: '', pr_number: 0, pr_state: '', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '', verification_run_id: '', verify_error: '',
} as ProposalDTO;

describe('batched proposal helpers', () => {
  it('lists every resolved node, falling back to node_id for a legacy row', () => {
    expect(proposalNodeIds({ ...base, resolved_node_ids: ['s.a', 's.b'] })).toEqual(['s.a', 's.b']);
    expect(proposalNodeIds(base)).toEqual(['s.a']);
  });

  it('reads a node-specific status and reason, falling back to the proposal', () => {
    const p = { ...base, resolved_node_ids: ['s.a', 's.b'], node_outcomes: { 's.b': { status: 'skipped', reason: 'no source' } } };
    expect(proposalStatusForNode(p, 's.a')).toBe('verifying');
    expect(proposalStatusForNode(p, 's.b')).toBe('skipped');
    expect(proposalReasonForNode(p, 's.b')).toBe('no source');
    expect(proposalReasonForNode(p, 's.a')).toBe('overall');
  });
});

describe('verificationPhase', () => {
  const p = (phases: string[]): ProposalDTO => ({
    ...base, status: 'verifying',
    verifications: phases.map((phase, i) => ({ service: `s${i}`, kind: 'dbt', run_id: `verify-${i}`, phase, activated_at: '', error: '' } as any)),
  });
  it('is running when any run is running', () => {
    expect(verificationPhase(p(['queued', 'running']))).toBe('running');
  });
  it('is queued when every known phase is queued', () => {
    expect(verificationPhase(p(['queued', 'queued']))).toBe('queued');
  });
  it('is undefined without positive evidence', () => {
    expect(verificationPhase(p(['', '']))).toBeUndefined();
    expect(verificationPhase(p(['passed', 'failed']))).toBeUndefined();
    expect(verificationPhase({ ...base, verifications: [] })).toBeUndefined();
  });
});

describe('verificationRunPhase', () => {
  it('maps run statuses onto phases', () => {
    expect(verificationRunPhase('received')).toBe('queued');
    for (const s of ['compiling', 'parsing', 'seed_building', 'validating']) expect(verificationRunPhase(s)).toBe('running');
    expect(verificationRunPhase('passed')).toBe('passed');
    expect(verificationRunPhase('failed')).toBe('failed');
  });
});

describe('verificationRunIds', () => {
  it('lists one run id per verification, dropping empty ids', () => {
    const p = {
      ...base,
      verification_run_id: 'verify-legacy',
      verifications: [
        { service: 'svc_a', kind: 'dbt', run_id: 'verify-a', phase: '', activated_at: '', error: '' },
        { service: 'svc_b', kind: 'dbt', run_id: '', phase: '', activated_at: '', error: '' },
        { service: 'svc_c', kind: 'python', run_id: 'verify-c', phase: '', activated_at: '', error: '' },
      ],
    } as ProposalDTO;
    expect(verificationRunIds(p)).toEqual(['verify-a', 'verify-c']);
  });

  it('falls back to the singular verification_run_id when there are no verifications', () => {
    expect(verificationRunIds({ ...base, verification_run_id: 'verify-legacy' })).toEqual(['verify-legacy']);
  });

  it('is empty when an attempt was judged without any verification run', () => {
    expect(verificationRunIds(base)).toEqual([]);
  });
});

describe('releasePillClass for verification statuses', () => {
  it('colours passed as succeeded, failed as failed, queued as pending', () => {
    expect(releasePillClass('passed')).toBe('pill--succeeded');
    expect(releasePillClass('failed')).toBe('pill--failed');
    expect(releasePillClass('queued')).toBe('pill--pending');
  });
});
