import { NodeValidationResult, ProposalDTO, PullRequestDTO, ReleaseListItem } from './types';

// In-flight = a release actively moving through the pipeline, in lifecycle order:
// received -> compiling -> parsing -> seed_building -> validating.
export const IN_FLIGHT_STATUSES = ['received', 'compiling', 'parsing', 'seed_building', 'validating'];

// firstInFlight returns the newest non-terminal release (the candidate currently
// moving through the pipeline), or null if every listed release is terminal.
export function firstInFlight(items: ReleaseListItem[]): ReleaseListItem | null {
  return items.find(r => IN_FLIGHT_STATUSES.includes(r.status)) ?? null;
}

// releasePillClass maps a status to a design-system pill variant. It handles
// release lifecycle statuses (promoted/rejected/validating/…) and per-node
// validation statuses (`ok`/`failed`), and falls back to run-style keyword
// matching, defaulting to pending when nothing matches.
export function releasePillClass(status: string): string {
  switch (status) {
    case 'promoted':   return 'pill--succeeded';
    case 'validated':  return 'pill--succeeded';
    case 'ok':         return 'pill--succeeded';
    case 'rejected':   return 'pill--failed';
    case 'validating':
    case 'seed_building':
    case 'compiling':
    case 'parsing':    return 'pill--running';
    case 'received':   return 'pill--pending';
    case 'superseded': return 'pill--cancelled';
  }
  if (status.includes('succeed')) return 'pill--succeeded';
  if (status.includes('fail'))    return 'pill--failed';
  if (status.includes('cancel'))  return 'pill--cancelled';
  if (status.includes('run'))     return 'pill--running';
  return 'pill--pending';
}

// Fixed display order of release failure stages. Matches the pipeline order the
// release-controller runs them in (compile → seed_build → validation).
export const STAGE_ORDER = ['compile', 'seed_build', 'validation'] as const;

const STAGE_LABELS: Record<string, string> = {
  compile: 'Compilation',
  seed_build: 'Seed build',
  validation: 'Validation',
  duplicate_table: 'Duplicate table',
};

// stageLabel maps a raw stage literal to its section display label, falling back
// to the raw value for an unrecognized stage.
export function stageLabel(stage: string): string {
  return STAGE_LABELS[stage] ?? stage;
}

// reasonLabel humanizes a release reject_reason token (e.g. "compile_failed")
// by dropping the "_failed" suffix and reusing the stage-label vocabulary, so
// the Releases list and the release detail page speak the same words. An
// unrecognized token falls through to its raw value.
export function reasonLabel(reason: string): string {
  return stageLabel(reason.replace(/_failed$/, ''));
}

// groupByStage buckets per-node results by stage and returns the buckets in
// STAGE_ORDER, omitting any stage with no results. Node order within a stage is
// preserved. Unknown stages follow the known ones in first-seen order.
export function groupByStage(
  perNode: NodeValidationResult[],
): { stage: string; nodes: NodeValidationResult[] }[] {
  const byStage = new Map<string, NodeValidationResult[]>();
  for (const node of perNode) {
    const list = byStage.get(node.stage);
    if (list) list.push(node);
    else byStage.set(node.stage, [node]);
  }
  const ordered: { stage: string; nodes: NodeValidationResult[] }[] = [];
  for (const stage of STAGE_ORDER) {
    const nodes = byStage.get(stage);
    if (nodes) {
      ordered.push({ stage, nodes });
      byStage.delete(stage);
    }
  }
  for (const [stage, nodes] of byStage) ordered.push({ stage, nodes });
  return ordered;
}

// proposalKey is the join key between a per-node result and a remediation
// proposal. A node_id alone is ambiguous across stages (a compile node_id is a
// service name that can collide with a validation node_id), so the key is
// (stage, node_id); stage and proposal.source share the same literals.
export function proposalKey(stage: string, nodeId: string): string {
  return `${stage}|${nodeId}`;
}

// proposalNodeIds lists every failing node one remediation attempt addresses.
// A batched attempt carries resolved_node_ids; a legacy single-node proposal
// (or one from before batching existed) has none, so its representative
// node_id is the sole member.
export function proposalNodeIds(p: ProposalDTO): string[] {
  return p.resolved_node_ids && p.resolved_node_ids.length > 0 ? p.resolved_node_ids : [p.node_id];
}

// proposalStatusForNode reads how a batched attempt ended for one specific
// node, falling back to the proposal's overall status for a legacy row or a
// node the attempt did not record a separate outcome for.
export function proposalStatusForNode(p: ProposalDTO, nodeId: string): string {
  return p.node_outcomes?.[nodeId]?.status ?? p.status;
}

// proposalReasonForNode reads why a batched attempt ended that way for one
// specific node, falling back to the proposal's overall rationale.
export function proposalReasonForNode(p: ProposalDTO, nodeId: string): string {
  return p.node_outcomes?.[nodeId]?.reason ?? p.rationale;
}

// proposalPullRequests lists every pull request opened from this proposal.
// A proposal split across owning services carries one entry per service in
// pull_requests; a legacy row (or one from before the per-service split
// existed) has none, so its singular pr_* fields — which the server mirrors
// from pull_requests[0] for exactly this reason — are synthesized into the
// one (service '') entry they describe. A proposal with no pull request at
// all (pr_url and pr_state both empty) yields an empty list either way.
export function proposalPullRequests(p: ProposalDTO): PullRequestDTO[] {
  if (p.pull_requests && p.pull_requests.length > 0) return p.pull_requests;
  if (!p.pr_url && !p.pr_state) return [];
  return [
    {
      service: '',
      repo: p.repo,
      branch: '',
      pr_url: p.pr_url,
      pr_number: p.pr_number,
      pr_state: p.pr_state,
      pr_opened_at: p.pr_opened_at,
      pr_opened_by: p.pr_opened_by,
      pr_closed_at: p.pr_closed_at,
    },
  ];
}

// proposalPrServices lists the owning-service groups this proposal's pull
// requests split into. Absent or empty on a legacy row (or one predating the
// per-service split), in which case it is a single group named '' — the same
// default agent-remediation reports for such a row.
export function proposalPrServices(p: ProposalDTO): string[] {
  return p.pr_services && p.pr_services.length > 0 ? p.pr_services : [''];
}

// proposalPrStateForService reads the pr_state of one owning-service group's
// pull request, '' when that service has none yet.
export function proposalPrStateForService(p: ProposalDTO, service: string): string {
  return proposalPullRequests(p).find((pr) => pr.service === service)?.pr_state ?? '';
}
