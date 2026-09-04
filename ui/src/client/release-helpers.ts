import { NodeValidationResult, ProposalDTO, PullRequestDTO } from './types';
import { serviceOfNode } from './service-helpers';

// releasePillClass maps a status to a design-system pill variant. It handles
// release lifecycle statuses (promoted/rejected/validating/…), verification-run
// statuses/phases (passed/queued/…), and per-node validation statuses
// (`ok`/`failed`), and falls back to run-style keyword matching, defaulting to
// pending when nothing matches.
export function releasePillClass(status: string): string {
  switch (status) {
    case 'promoted':   return 'pill--succeeded';
    case 'passed':     return 'pill--succeeded';
    case 'ok':         return 'pill--succeeded';
    case 'rejected':   return 'pill--failed';
    case 'validating':
    case 'seed_building':
    case 'compiling':
    case 'parsing':    return 'pill--running';
    case 'received':   return 'pill--pending';
    case 'queued':     return 'pill--pending';
    case 'superseded': return 'pill--cancelled';
  }
  if (status.includes('succeed')) return 'pill--succeeded';
  if (status.includes('fail'))    return 'pill--failed';
  if (status.includes('cancel'))  return 'pill--cancelled';
  if (status.includes('run'))     return 'pill--running';
  return 'pill--pending';
}

// proposalPillClass maps a remediation attempt's lifecycle status to a pill
// variant. The attempt vocabulary (proposed/verifying/generating/failed/
// escalated/skipped) is distinct from the release one, so each status is
// mapped on purpose: a proposed fix is the good outcome (green), an attempt
// still being verified is in progress (the running hue), one still being
// generated is waiting (grey), and both failed and escalated are dead ends
// that need a human (red). Anything else falls back to releasePillClass.
export function proposalPillClass(status: string): string {
  switch (status) {
    case 'proposed':   return 'pill--succeeded';
    case 'verifying':  return 'pill--running';
    case 'generating': return 'pill--pending';
    case 'failed':     return 'pill--failed';
    case 'escalated':  return 'pill--failed';
    case 'skipped':    return 'pill--skipped';
  }
  return releasePillClass(status);
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

// verificationRunPhase reads a verification run's status as the phase an
// operator cares about: still queued behind other runs, running a leg, or
// its verdict.
export function verificationRunPhase(status: string): 'queued' | 'running' | 'passed' | 'failed' {
  switch (status) {
    case 'received': return 'queued';
    case 'passed':   return 'passed';
    case 'failed':   return 'failed';
    default:         return 'running';
  }
}

// verificationRunIds lists the verification runs that judged one attempt —
// one per edited service, or the legacy singular verification_run_id.
export function verificationRunIds(p: ProposalDTO): string[] {
  if (p.verifications && p.verifications.length > 0) {
    return p.verifications.map(v => v.run_id).filter(Boolean);
  }
  return p.verification_run_id ? [p.verification_run_id] : [];
}

// verificationPhase decides whether an attempt in 'verifying' is being run
// now or is still waiting its turn, from the phases agent-remediation
// recorded on its verifications. Running wins. Queued needs positive
// evidence. Anything else (no phase recorded yet, or only verdicts) is
// undefined so the chip keeps its flat wording rather than claiming a wait.
export function verificationPhase(p: ProposalDTO): 'queued' | 'running' | undefined {
  let sawQueued = false;
  for (const v of p.verifications ?? []) {
    if (v.phase === 'running') return 'running';
    if (v.phase === 'queued') sawQueued = true;
  }
  return sawQueued ? 'queued' : undefined;
}

// effectiveRound is the remediation round a proposal belongs to. The gRPC
// client (ui/src/server/remediation-client.ts) loads the proto with
// `defaults: true`, so a proposal recorded before remediation_round existed
// arrives over the wire as 0, not undefined — a plain `?? 1` fallback would
// not catch it. Both a missing field and an explicit 0 read as round 1.
export function effectiveRound(p: ProposalDTO): number {
  return p.remediation_round && p.remediation_round > 0 ? p.remediation_round : 1;
}

// ProposalGroup aggregates every remediation attempt for one (release, round):
// the attempts newest first, the union of the services they touch and the
// nodes they resolve, the newest attempt's status, and the newest attempt
// that carries a pull request.
export interface ProposalGroup {
  key: string;
  releaseId: string;
  round: number;
  attempts: ProposalDTO[];       // newest first
  latest: ProposalDTO;           // attempts[0]
  services: string[];            // see proposalServices; union, sorted
  nodeIds: string[];             // resolved-node union, sorted
  latestPrProposal: ProposalDTO | null;
}

// proposalServices lists the services one attempt touched: the service of
// every failing node it resolves (a node id is "{service}.{schema}.{table}";
// a compile-stage node id is the bare service name, so the service is the
// first dotted segment either way) plus every owning service its pull
// requests split into. This is the same set the server's `service` list
// filter matches a proposal on, so the Services column and the filter agree.
// The legacy '' pr_services sentinel is dropped.
export function proposalServices(p: ProposalDTO): string[] {
  const set = new Set<string>();
  for (const n of proposalNodeIds(p)) {
    const s = serviceOfNode(n);
    if (s !== '') set.add(s);
  }
  for (const s of proposalPrServices(p)) if (s !== '') set.add(s);
  return Array.from(set).sort();
}

// groupProposals buckets proposals by (release_id, remediation round), newest
// group first by its latest attempt. Within a group the attempts are newest
// first (created_at, then attempt number). `services` unions each attempt's
// proposalServices; `nodeIds` unions their resolved nodes; `latestPrProposal`
// is the newest attempt that has opened a pull request, or null when none
// has.
export function groupProposals(proposals: ProposalDTO[]): ProposalGroup[] {
  const buckets = new Map<string, ProposalDTO[]>();
  for (const p of proposals) {
    const key = `${p.release_id} ${effectiveRound(p)}`;
    const list = buckets.get(key);
    if (list) list.push(p);
    else buckets.set(key, [p]);
  }

  const groups: ProposalGroup[] = [];
  for (const [key, attempts] of buckets) {
    attempts.sort((a, b) => {
      if (a.created_at !== b.created_at) return a.created_at < b.created_at ? 1 : -1;
      return b.attempt - a.attempt;
    });
    const latest = attempts[0];

    const serviceSet = new Set<string>();
    for (const p of attempts) for (const s of proposalServices(p)) serviceSet.add(s);
    const nodeSet = new Set<string>();
    for (const p of attempts) for (const n of proposalNodeIds(p)) nodeSet.add(n);

    groups.push({
      key,
      releaseId: latest.release_id,
      round: effectiveRound(latest),
      attempts,
      latest,
      services: Array.from(serviceSet).sort(),
      nodeIds: Array.from(nodeSet).sort(),
      latestPrProposal: attempts.find(p => proposalPullRequests(p).some(pr => pr.pr_state)) ?? null,
    });
  }

  groups.sort((a, b) => {
    if (a.latest.created_at !== b.latest.created_at) return a.latest.created_at < b.latest.created_at ? 1 : -1;
    if (a.releaseId !== b.releaseId) return a.releaseId < b.releaseId ? -1 : 1;
    return b.round - a.round;
  });
  return groups;
}
