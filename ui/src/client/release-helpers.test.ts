import { describe, it, expect } from 'vitest';
import {
  groupByStage, stageLabel, reasonLabel, fixKey,
  fixNodeIds, fixStatusForNode, fixReasonForNode, fixCodeForNode,
  releasePillClass, tryReleaseCheckPhase, releaseCheckPhase,
  effectiveRound, groupFixes, fixPillClass, upcomingStages,
  tryLabel, tryBefore, HAND_OVER_TRY,
} from './release-helpers';
import { HANDOVER_TEXT } from './handover-text';
import { HANDOVER_REASONS } from '../server/generated/vocabulary.gen';
import { NodeValidationResult } from './types';
import type { FixDTO } from './types';
import { REJECT_REASONS } from '../server/generated/vocabulary.gen';

const n = (stage: string, node_id: string): NodeValidationResult => ({
  stage, node_id, status: 'failed',
});

describe('groupByStage', () => {
  it('orders parse → compile → seed_build → validation and preserves node order within a stage', () => {
    const groups = groupByStage([
      n('validation', 'v1'), n('compile', 'c1'), n('validation', 'v2'), n('seed_build', 's1'), n('parse', 'p1'),
    ]);
    expect(groups.map(g => g.stage)).toEqual(['parse', 'compile', 'seed_build', 'validation']);
    expect(groups[3].nodes.map(x => x.node_id)).toEqual(['v1', 'v2']);
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

  it('says an unchanged upstream could not be built from production', () => {
    expect(reasonLabel('upstream_build_from_prod_failed')).toBe('Could not build an upstream from production');
  });

  it('falls back to the raw token for an unrecognized reason', () => {
    expect(reasonLabel('mystery_failed')).toBe('mystery');
  });
});

// Every value of the `reject_reason` vocabulary comes from the file generated
// from pkg/streams/contract.yaml, so a reason added there without a label here
// fails this test instead of showing the operator a raw token.
describe('reject_reason vocabulary coverage', () => {
  it.each(REJECT_REASONS)('labels %s as prose, not as a raw token', (reason) => {
    const label = reasonLabel(reason);
    expect(label).not.toBe(reason);
    expect(label).not.toContain('_');
    expect(label[0]).toBe(label[0].toUpperCase());
  });
});

describe('parse stage vocabulary', () => {
  it('labels the parse stage and its reasons', () => {
    expect(stageLabel('parse')).toBe('Parse');
    expect(reasonLabel('invalid_sql')).toBe('Invalid SQL');
    expect(reasonLabel('unqualified_reference')).toBe('Unqualified reference');
    expect(reasonLabel('invalid_artifact')).toBe('Invalid artifact');
    expect(reasonLabel('invalid_contract')).toBe('Invalid contract');
    expect(reasonLabel('internal_error')).toBe('Internal error');
    expect(reasonLabel('compile_failed')).toBe('Compilation');
  });
});

describe('fixKey', () => {
  it('joins stage and node_id', () => {
    expect(fixKey('compile', 'svc')).toBe('compile|svc');
    expect(fixKey('validation', 'model.svc.x')).toBe('validation|model.svc.x');
  });
});

const base = {
  id: 'p', source: 'validation', release_id: 'r', node_id: 's.a', error_signature: 's', try: 1,
  status: 'verifying', confidence: 'high', rationale: 'overall', proposed_sql_uri: '', diff_uri: '', candidate_fix_sql_uri: '',
  candidate_fix_diff_uri: '', source_resolved: true, repo: '', commit_sha: '', file_path: '', model: '', created_at: '',
  pr_url: '', pr_number: 0, pr_state: '', pr_opened_at: '', pr_opened_by: '', pr_closed_at: '', release_check_id: '', verify_error: '',
} as FixDTO;

describe('batched fix helpers', () => {
  it('lists every resolved node, falling back to node_id for a legacy row', () => {
    expect(fixNodeIds({ ...base, resolved_node_ids: ['s.a', 's.b'] })).toEqual(['s.a', 's.b']);
    expect(fixNodeIds(base)).toEqual(['s.a']);
  });

  it('reads a node-specific status and reason, falling back to the fix', () => {
    const p = { ...base, resolved_node_ids: ['s.a', 's.b'], node_outcomes: { 's.b': { status: 'skipped', reason: 'no source' } } };
    expect(fixStatusForNode(p, 's.a')).toBe('verifying');
    expect(fixStatusForNode(p, 's.b')).toBe('skipped');
    expect(fixReasonForNode(p, 's.b')).toBe('no source');
    expect(fixReasonForNode(p, 's.a')).toBe('overall');
  });
});

describe('fixCodeForNode', () => {
  const withOutcome = (outcome: object): FixDTO => ({
    ...base, resolved_node_ids: ['s.a'], node_outcomes: { 's.a': outcome as any },
  });

  it('reads the hand-over code of the node\'s own outcome', () => {
    expect(fixCodeForNode(withOutcome({ status: 'failed', reason: 'x', code: 'tries_spent' }), 's.a')).toBe('tries_spent');
  });

  it('is undefined for the empty code the server sends when an outcome has none', () => {
    expect(fixCodeForNode(withOutcome({ status: 'failed', reason: 'x', code: '' }), 's.a')).toBeUndefined();
  });

  it('is undefined when the outcome omits the code field', () => {
    expect(fixCodeForNode(withOutcome({ status: 'failed', reason: 'x' }), 's.a')).toBeUndefined();
  });

  it('is undefined for a value that is not a known reason', () => {
    expect(fixCodeForNode(withOutcome({ status: 'failed', reason: 'x', code: 'something_new' }), 's.a')).toBeUndefined();
  });

  it('is undefined for a node with no outcome of its own, with no fix-level fallback', () => {
    const p = withOutcome({ status: 'failed', reason: 'x', code: 'declined' });
    expect(fixCodeForNode(p, 's.b')).toBeUndefined();
    expect(fixCodeForNode(base, 's.a')).toBeUndefined();
  });
});

describe('HANDOVER_TEXT', () => {
  it('every hand-over reason has a text', () => {
    expect(Object.keys(HANDOVER_TEXT).sort()).toEqual([...HANDOVER_REASONS].sort());
    for (const reason of HANDOVER_REASONS) {
      expect(HANDOVER_TEXT[reason].trim()).not.toBe('');
    }
  });
});

describe('tryReleaseCheckPhase', () => {
  const p = (phases: string[]): FixDTO => ({
    ...base, status: 'verifying',
    release_checks: phases.map((phase, i) => ({ project: `s${i}`, kind: 'dbt', check_id: `verify-${i}`, phase, activated_at: '', error: '' } as any)),
  });
  it('is running when any release check is running', () => {
    expect(tryReleaseCheckPhase(p(['queued', 'running']))).toBe('running');
  });
  it('is queued when every known phase is queued', () => {
    expect(tryReleaseCheckPhase(p(['queued', 'queued']))).toBe('queued');
  });
  it('is undefined without positive evidence', () => {
    expect(tryReleaseCheckPhase(p(['', '']))).toBeUndefined();
    expect(tryReleaseCheckPhase(p(['passed', 'failed']))).toBeUndefined();
    expect(tryReleaseCheckPhase({ ...base, release_checks: [] })).toBeUndefined();
  });
});

describe('releaseCheckPhase', () => {
  it('maps release check statuses onto phases', () => {
    expect(releaseCheckPhase('received')).toBe('queued');
    for (const s of ['compiling', 'parsing', 'seed_building', 'validating']) expect(releaseCheckPhase(s)).toBe('running');
    expect(releaseCheckPhase('passed')).toBe('passed');
    expect(releaseCheckPhase('failed')).toBe('failed');
  });
});

describe('releasePillClass for release check statuses', () => {
  it('colours passed as succeeded, failed as failed, queued as pending', () => {
    expect(releasePillClass('passed')).toBe('pill--succeeded');
    expect(releasePillClass('failed')).toBe('pill--failed');
    expect(releasePillClass('queued')).toBe('pill--pending');
  });
});

describe('effectiveRound', () => {
  const p = (round?: number): FixDTO => ({ round: round } as FixDTO);
  it('treats a missing round as 1', () => expect(effectiveRound(p(undefined))).toBe(1));
  it('treats an explicit 0 as 1 (proto3 default over the wire)', () => expect(effectiveRound(p(0))).toBe(1));
  it('passes a positive round through', () => expect(effectiveRound(p(3))).toBe(3));
});

describe('groupFixes', () => {
  // A minimal fix carrying only the fields groupFixes reads.
  const mk = (o: Partial<FixDTO>): FixDTO => ({
    id: o.id ?? 'x',
    release_id: o.release_id ?? 'rel-1',
    round: o.round,
    created_at: o.created_at ?? '2026-09-03T10:00:00Z',
    try: o.try ?? 1,
    status: o.status ?? 'failed',
    node_id: o.node_id ?? 'n0',
    resolved_node_ids: o.resolved_node_ids,
    pr_projects: o.pr_projects,
    projects: o.projects,
    pull_requests: o.pull_requests,
    pr_url: o.pr_url ?? '',
    pr_state: o.pr_state ?? '',
  } as FixDTO);

  it('buckets by (release_id, round); same release+round is one group, different round is two', () => {
    const groups = groupFixes([
      mk({ id: 'a', release_id: 'r1', round: 1, created_at: '2026-09-03T10:00:00Z' }),
      mk({ id: 'b', release_id: 'r1', round: 1, created_at: '2026-09-03T11:00:00Z' }),
      mk({ id: 'c', release_id: 'r1', round: 2, created_at: '2026-09-03T12:00:00Z' }),
    ]);
    expect(groups).toHaveLength(2);
    const round1 = groups.find(g => g.round === 1)!;
    expect(round1.tries.map(a => a.id)).toEqual(['b', 'a']); // newest first
    expect(round1.latest.id).toBe('b');
  });

  it('orders groups newest first by their latest try', () => {
    const groups = groupFixes([
      mk({ id: 'old', release_id: 'r1', round: 1, created_at: '2026-09-01T10:00:00Z' }),
      mk({ id: 'new', release_id: 'r2', round: 1, created_at: '2026-09-05T10:00:00Z' }),
    ]);
    expect(groups.map(g => g.releaseId)).toEqual(['r2', 'r1']);
  });

  it('tie-breaks same created_at by higher try number for the latest', () => {
    const groups = groupFixes([
      mk({ id: 'a1', release_id: 'r1', round: 1, try: 1, created_at: '2026-09-03T10:00:00Z' }),
      mk({ id: 'a2', release_id: 'r1', round: 1, try: 2, created_at: '2026-09-03T10:00:00Z' }),
    ]);
    expect(groups[0].latest.id).toBe('a2');
  });

  it('tie-breaks same created_at with the round\'s hand-over row after its tries', () => {
    const groups = groupFixes([
      mk({ id: 'h', release_id: 'r1', round: 1, try: HAND_OVER_TRY, status: 'handed_over', created_at: '2026-09-03T10:00:00Z' }),
      mk({ id: 't3', release_id: 'r1', round: 1, try: 3, created_at: '2026-09-03T10:00:00Z' }),
    ]);
    expect(groups[0].latest.id).toBe('h');
    expect(groups[0].tries.map(t => t.id)).toEqual(['h', 't3']);
  });

  it('unions the projects each try carries, sorted and de-duplicated', () => {
    const groups = groupFixes([
      mk({ id: 'a', projects: ['ledger', 'analytics'] }),
      mk({ id: 'b', projects: ['analytics'] }),
    ]);
    expect(groups[0].projects).toEqual(['analytics', 'ledger']);
  });

  it('never parses a project out of a node id — a remediation node id is "<schema>.<table>"', () => {
    const groups = groupFixes([
      mk({ id: 'a', node_id: 'e2e_schema.table_a', resolved_node_ids: ['e2e_schema.table_a', 'e2e_schema.table_b'], projects: ['service-1'] }),
    ]);
    expect(groups[0].projects).toEqual(['service-1']);
  });

  it('falls back to the non-empty pr_projects of a try that carries no projects', () => {
    // A fix served by an agent-remediation that predates the projects
    // field arrives with it absent; the owning projects its pull requests
    // split into are the nearest thing it does carry.
    const groups = groupFixes([
      mk({ id: 'a', pr_projects: ['shared', 'analytics'] }),
      mk({ id: 'b', pr_projects: [''] }),
    ]);
    expect(groups[0].projects).toEqual(['analytics', 'shared']);
  });

  it('yields no projects for a legacy try carrying neither projects nor split pull requests', () => {
    const groups = groupFixes([mk({ id: 'a', node_id: 'core_schema.t', pr_projects: [''] })]);
    expect(groups[0].projects).toEqual([]);
  });

  it('unions resolved node ids across tries, sorted', () => {
    const groups = groupFixes([
      mk({ id: 'a', resolved_node_ids: ['svc.b', 'svc.a'] }),
      mk({ id: 'b', resolved_node_ids: ['svc.c', 'svc.a'] }),
    ]);
    expect(groups[0].nodeIds).toEqual(['svc.a', 'svc.b', 'svc.c']);
  });

  it('picks the newest try that has a pull request as latestPrFix', () => {
    const groups = groupFixes([
      mk({ id: 'older', created_at: '2026-09-03T10:00:00Z', pr_state: 'merged' }),
      mk({ id: 'newer', created_at: '2026-09-03T12:00:00Z' }), // no PR
    ]);
    expect(groups[0].latestPrFix?.id).toBe('older');
  });

  it('sets latestPrFix to null when no try has a pull request', () => {
    const groups = groupFixes([mk({ id: 'a' }), mk({ id: 'b' })]);
    expect(groups[0].latestPrFix).toBeNull();
  });
});

describe('fixPillClass', () => {
  it.each([
    ['proposed',   'pill--succeeded'],
    ['verifying',  'pill--running'],
    ['generating', 'pill--pending'],
    ['failed',     'pill--failed'],
    ['handed_over', 'pill--failed'],
    ['skipped',    'pill--skipped'],
  ])('maps the %s try status to %s', (status, cls) => {
    expect(fixPillClass(status)).toBe(cls);
  });

  it('falls back to the release vocabulary for an unknown status', () => {
    expect(fixPillClass('cancelled_by_operator')).toBe('pill--cancelled');
    expect(fixPillClass('whatever')).toBe('pill--pending');
  });
});

describe('upcomingStages', () => {
  const at = '2026-11-20T10:00:00Z';
  const reached = (...stages: string[]) => stages.map(to => ({ to, at }));
  const stagesOf = (to: string, manifestKind?: string) => upcomingStages(reached(to), { manifestKind }).map(s => s.stage);

  it('takes a dbt release through compiling, parsing, seed building and validating', () => {
    expect(stagesOf('received', 'dbt')).toEqual(['compiling', 'parsing', 'seed_building', 'validating']);
  });

  it('takes a release of an unknown kind down the dbt path', () => {
    expect(stagesOf('received', 'something-new')).toEqual(['compiling', 'parsing', 'seed_building', 'validating']);
    expect(stagesOf('received')).toEqual(['compiling', 'parsing', 'seed_building', 'validating']);
  });

  it('shows no compiling and no seed building for a python release', () => {
    expect(stagesOf('received', 'python')).toEqual(['parsing', 'validating']);
  });

  it('shows no compiling and no seed building for an ontology release', () => {
    expect(stagesOf('received', 'ontology')).toEqual(['parsing', 'validating']);
    expect(stagesOf('parsing', 'ontology')).toEqual(['validating']);
    expect(stagesOf('validating', 'ontology')).toEqual([]);
  });

  it('shows no validating for a bootstrap ontology release, which promotes after parsing', () => {
    const run = { manifestKind: 'ontology', bootstrap: true };
    expect(upcomingStages(reached('received'), run).map(s => s.stage)).toEqual(['parsing']);
  });

  it('lists nothing for a run that has finished or recorded no transition', () => {
    expect(upcomingStages(reached('received', 'promoted'), { manifestKind: 'ontology' })).toEqual([]);
    expect(upcomingStages([], { manifestKind: 'ontology' })).toEqual([]);
  });
});

describe('tryLabel', () => {
  it('names a try by its round and number', () => expect(tryLabel(2, 1)).toBe('round 2, try 1'));
  it('reads a round of 0 or none as round 1', () => {
    expect(tryLabel(0, 3)).toBe('round 1, try 3');
    expect(tryLabel(undefined, 3)).toBe('round 1, try 3');
  });
  it('names the round\'s hand-over row, which is no try', () =>
    expect(tryLabel(2, HAND_OVER_TRY)).toBe('round 2, hand-over'));
});

describe('tryBefore', () => {
  const t = (round: number, tryNumber: number) => ({ round, try: tryNumber });
  it.each([
    ['an earlier try of the round', t(1, 1), t(1, 2)],
    ['an earlier round\'s last try', t(1, 3), t(2, 1)],
    ['a try before its round\'s hand-over row', t(1, 3), t(1, HAND_OVER_TRY)],
    ['a round\'s hand-over row before the next round', t(1, HAND_OVER_TRY), t(2, 1)],
  ])('orders %s first', (_name, a, b) => {
    expect(tryBefore(a, b)).toBe(true);
    expect(tryBefore(b, a)).toBe(false);
  });
  it('does not order a try before itself', () => expect(tryBefore(t(2, 1), t(2, 1))).toBe(false));
});
