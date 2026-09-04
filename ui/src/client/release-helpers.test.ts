import { describe, it, expect } from 'vitest';
import {
  groupByStage, stageLabel, reasonLabel, proposalKey,
  proposalNodeIds, proposalStatusForNode, proposalReasonForNode,
  releasePillClass, verificationPhase, verificationRunPhase, verificationRunIds,
  effectiveRound, groupProposals, proposalPillClass,
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

describe('effectiveRound', () => {
  const p = (round?: number): ProposalDTO => ({ remediation_round: round } as ProposalDTO);
  it('treats a missing round as 1', () => expect(effectiveRound(p(undefined))).toBe(1));
  it('treats an explicit 0 as 1 (proto3 default over the wire)', () => expect(effectiveRound(p(0))).toBe(1));
  it('passes a positive round through', () => expect(effectiveRound(p(3))).toBe(3));
});

describe('groupProposals', () => {
  // A minimal proposal carrying only the fields groupProposals reads.
  const mk = (o: Partial<ProposalDTO>): ProposalDTO => ({
    id: o.id ?? 'x',
    release_id: o.release_id ?? 'rel-1',
    remediation_round: o.remediation_round,
    created_at: o.created_at ?? '2026-09-03T10:00:00Z',
    attempt: o.attempt ?? 1,
    status: o.status ?? 'failed',
    node_id: o.node_id ?? 'n0',
    resolved_node_ids: o.resolved_node_ids,
    pr_services: o.pr_services,
    services: o.services,
    pull_requests: o.pull_requests,
    pr_url: o.pr_url ?? '',
    pr_state: o.pr_state ?? '',
  } as ProposalDTO);

  it('buckets by (release_id, round); same release+round is one group, different round is two', () => {
    const groups = groupProposals([
      mk({ id: 'a', release_id: 'r1', remediation_round: 1, created_at: '2026-09-03T10:00:00Z' }),
      mk({ id: 'b', release_id: 'r1', remediation_round: 1, created_at: '2026-09-03T11:00:00Z' }),
      mk({ id: 'c', release_id: 'r1', remediation_round: 2, created_at: '2026-09-03T12:00:00Z' }),
    ]);
    expect(groups).toHaveLength(2);
    const round1 = groups.find(g => g.round === 1)!;
    expect(round1.attempts.map(a => a.id)).toEqual(['b', 'a']); // newest first
    expect(round1.latest.id).toBe('b');
  });

  it('orders groups newest first by their latest attempt', () => {
    const groups = groupProposals([
      mk({ id: 'old', release_id: 'r1', remediation_round: 1, created_at: '2026-09-01T10:00:00Z' }),
      mk({ id: 'new', release_id: 'r2', remediation_round: 1, created_at: '2026-09-05T10:00:00Z' }),
    ]);
    expect(groups.map(g => g.releaseId)).toEqual(['r2', 'r1']);
  });

  it('tie-breaks same created_at by higher attempt number for the latest', () => {
    const groups = groupProposals([
      mk({ id: 'a1', release_id: 'r1', remediation_round: 1, attempt: 1, created_at: '2026-09-03T10:00:00Z' }),
      mk({ id: 'a2', release_id: 'r1', remediation_round: 1, attempt: 2, created_at: '2026-09-03T10:00:00Z' }),
    ]);
    expect(groups[0].latest.id).toBe('a2');
  });

  it('unions the services each attempt carries, sorted and de-duplicated', () => {
    const groups = groupProposals([
      mk({ id: 'a', services: ['ledger', 'analytics'] }),
      mk({ id: 'b', services: ['analytics'] }),
    ]);
    expect(groups[0].services).toEqual(['analytics', 'ledger']);
  });

  it('never parses a service out of a node id — a remediation node id is "<schema>.<table>"', () => {
    const groups = groupProposals([
      mk({ id: 'a', node_id: 'e2e_schema.table_a', resolved_node_ids: ['e2e_schema.table_a', 'e2e_schema.table_b'], services: ['service-1'] }),
    ]);
    expect(groups[0].services).toEqual(['service-1']);
  });

  it('falls back to the non-empty pr_services of an attempt that carries no services', () => {
    // A proposal served by an agent-remediation that predates the services
    // field arrives with it absent; the owning services its pull requests
    // split into are the nearest thing it does carry.
    const groups = groupProposals([
      mk({ id: 'a', pr_services: ['shared', 'analytics'] }),
      mk({ id: 'b', pr_services: [''] }),
    ]);
    expect(groups[0].services).toEqual(['analytics', 'shared']);
  });

  it('yields no services for a legacy attempt carrying neither services nor split pull requests', () => {
    const groups = groupProposals([mk({ id: 'a', node_id: 'core_schema.t', pr_services: [''] })]);
    expect(groups[0].services).toEqual([]);
  });

  it('unions resolved node ids across attempts, sorted', () => {
    const groups = groupProposals([
      mk({ id: 'a', resolved_node_ids: ['svc.b', 'svc.a'] }),
      mk({ id: 'b', resolved_node_ids: ['svc.c', 'svc.a'] }),
    ]);
    expect(groups[0].nodeIds).toEqual(['svc.a', 'svc.b', 'svc.c']);
  });

  it('picks the newest attempt that has a pull request as latestPrProposal', () => {
    const groups = groupProposals([
      mk({ id: 'older', created_at: '2026-09-03T10:00:00Z', pr_state: 'merged' }),
      mk({ id: 'newer', created_at: '2026-09-03T12:00:00Z' }), // no PR
    ]);
    expect(groups[0].latestPrProposal?.id).toBe('older');
  });

  it('sets latestPrProposal to null when no attempt has a pull request', () => {
    const groups = groupProposals([mk({ id: 'a' }), mk({ id: 'b' })]);
    expect(groups[0].latestPrProposal).toBeNull();
  });
});

describe('proposalPillClass', () => {
  it.each([
    ['proposed',   'pill--succeeded'],
    ['verifying',  'pill--running'],
    ['generating', 'pill--pending'],
    ['failed',     'pill--failed'],
    ['escalated',  'pill--failed'],
    ['skipped',    'pill--skipped'],
  ])('maps the %s attempt status to %s', (status, cls) => {
    expect(proposalPillClass(status)).toBe(cls);
  });

  it('falls back to the release vocabulary for an unknown status', () => {
    expect(proposalPillClass('cancelled_by_operator')).toBe('pill--cancelled');
    expect(proposalPillClass('whatever')).toBe('pill--pending');
  });
});
