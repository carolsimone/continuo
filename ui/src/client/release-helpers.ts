import { HANDOVER_REASONS, type HandoverReason } from '../server/generated/vocabulary.gen';
import { ApproverDTO, NodeValidationResult, FixDTO, PullRequestDTO, ReleaseTransition } from './types';

// releasePillClass maps a status to a design-system pill variant. It handles
// release lifecycle statuses (promoted/rejected/validating/…), release-check
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

// fixPillClass maps a remediation try's lifecycle status to a pill
// variant. The try vocabulary (proposed/verifying/generating/failed/
// escalated/skipped) is distinct from the release one, so each status is
// mapped on purpose: a proposed fix is the good outcome (green), a try
// still being verified is in progress (the running hue), one still being
// generated is waiting (grey), and both failed and escalated are dead ends
// that need a human (red). Anything else falls back to releasePillClass.
export function fixPillClass(status: string): string {
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

// Fixed display order of release failure stages. Matches the order
// release-controller runs a release check's stages in (parse → compile →
// seed_build → validation).
export const STAGE_ORDER = ['parse', 'compile', 'seed_build', 'validation'] as const;

const STAGE_LABELS: Record<string, string> = {
  parse: 'Parse',
  compile: 'Compilation',
  seed_build: 'Seed build',
  validation: 'Validation',
  duplicate_table: 'Duplicate table',
  invalid_sql: 'Invalid SQL',
  unqualified_reference: 'Unqualified reference',
  invalid_contract: 'Invalid contract',
  invalid_artifact: 'Invalid artifact',
  internal_error: 'Internal error',
  parse_rehearsal: 'Parse rehearsal',
  artifact_upload: 'Artifact upload',
  nothing_to_validate: 'Nothing to validate',
  unbuildable_cross_project_upstream: 'Unbuildable cross-project upstream',
  upgrade_interrupted: 'Upgrade interrupted',
  // upstream_clone_failed, after reasonLabel drops its "_failed" suffix.
  upstream_clone: 'Could not copy an upstream from production',
};

// stageLabel maps a raw stage literal to its section display label, falling back
// to the raw value for an unrecognized stage.
export function stageLabel(stage: string): string {
  return STAGE_LABELS[stage] ?? stage;
}

// reasonLabel humanizes a release reject_reason token: a leg reason (e.g.
// "compile_failed") drops its "_failed" suffix and reuses the stage label; a
// parse reason has its own entry. Every value of the `reject_reason` vocabulary
// in pkg/streams/contract.yaml resolves to prose, pinned by
// release-helpers.test.ts. An unrecognized token falls through to its raw
// value.
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

// fixKey is the join key between a per-node result and a remediation
// fix. A node_id alone is ambiguous across stages (a compile node_id is a
// project name that can collide with a validation node_id), so the key is
// (stage, node_id); stage and fix.source share the same literals.
export function fixKey(stage: string, nodeId: string): string {
  return `${stage}|${nodeId}`;
}

// fixNodeIds lists every failing node one remediation try addresses.
// A batched try carries resolved_node_ids; a legacy single-node fix
// (or one from before batching existed) has none, so its representative
// node_id is the sole member.
export function fixNodeIds(p: FixDTO): string[] {
  return p.resolved_node_ids && p.resolved_node_ids.length > 0 ? p.resolved_node_ids : [p.node_id];
}

// fixStatusForNode reads how a batched try ended for one specific
// node, falling back to the fix's overall status for a legacy row or a
// node the try did not record a separate outcome for.
export function fixStatusForNode(p: FixDTO, nodeId: string): string {
  return p.node_outcomes?.[nodeId]?.status ?? p.status;
}

// fixReasonForNode reads why a batched try ended that way for one
// specific node, falling back to the fix's overall rationale.
export function fixReasonForNode(p: FixDTO, nodeId: string): string {
  return p.node_outcomes?.[nodeId]?.reason ?? p.rationale;
}

// fixCodeForNode reads the handover_reason a batched try handed one
// specific node over for. It reads only that node's own outcome: a fix
// describes several nodes, so no fix-level value stands in for it. An
// absent or empty code, and a value that is not a known reason, read as
// undefined.
export function fixCodeForNode(p: FixDTO, nodeId: string): HandoverReason | undefined {
  const code = p.node_outcomes?.[nodeId]?.code;
  return code && (HANDOVER_REASONS as readonly string[]).includes(code) ? code : undefined;
}

// fixApproverForNode reads who must approve what a batched try did
// with one specific node. It reads only that node's own entry: a try row
// with no remediation id records none, and a node the try recorded no
// approver for has none either, which says the owner was not recorded, not that
// the node has no owner (an entry with an empty owner says that).
export function fixApproverForNode(p: FixDTO, nodeId: string): ApproverDTO | undefined {
  return p.approvers?.find(a => a.node_id === nodeId);
}

// fixPullRequests lists every pull request opened from this fix.
// A fix split across owning projects carries one entry per project in
// pull_requests; a legacy row (or one from before the per-project split
// existed) has none, so its singular pr_* fields — which the server mirrors
// from pull_requests[0] for exactly this reason — are synthesized into the
// one (project '') entry they describe. A fix with no pull request at
// all (pr_url and pr_state both empty) yields an empty list either way.
export function fixPullRequests(p: FixDTO): PullRequestDTO[] {
  if (p.pull_requests && p.pull_requests.length > 0) return p.pull_requests;
  if (!p.pr_url && !p.pr_state) return [];
  return [
    {
      project: '',
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

// fixPrProjects lists the owning-project groups this fix's pull
// requests split into. Absent or empty on a legacy row (or one predating the
// per-project split), in which case it is a single group named '' — the same
// default agent-remediation reports for such a row.
export function fixPrProjects(p: FixDTO): string[] {
  return p.pr_projects && p.pr_projects.length > 0 ? p.pr_projects : [''];
}

// fixPrStateForProject reads the pr_state of one owning-project group's
// pull request, '' when that project has none yet.
export function fixPrStateForProject(p: FixDTO, project: string): string {
  return fixPullRequests(p).find((pr) => pr.project === project)?.pr_state ?? '';
}

// releaseCheckPhase reads a release check's status as the phase an
// operator cares about: still queued behind other release checks, running a leg, or
// its verdict.
export function releaseCheckPhase(status: string): 'queued' | 'running' | 'passed' | 'failed' {
  switch (status) {
    case 'received': return 'queued';
    case 'passed':   return 'passed';
    case 'failed':   return 'failed';
    default:         return 'running';
  }
}

// tryReleaseCheckPhase decides whether the release check of a try
// in 'verifying' is running now or still waiting its turn, from the phases
// agent-remediation recorded on its release checks. Running wins. Queued needs positive
// evidence. Anything else (no phase recorded yet, or only verdicts) is
// undefined so the chip keeps its flat wording rather than claiming a wait.
export function tryReleaseCheckPhase(p: FixDTO): 'queued' | 'running' | undefined {
  let sawQueued = false;
  for (const v of p.release_checks ?? []) {
    if (v.phase === 'running') return 'running';
    if (v.phase === 'queued') sawQueued = true;
  }
  return sawQueued ? 'queued' : undefined;
}

// effectiveRound is the remediation round a fix belongs to. The gRPC
// client (ui/src/server/remediation-client.ts) loads the proto with
// `defaults: true`, so a fix recorded before remediation_round existed
// arrives over the wire as 0, not undefined — a plain `?? 1` fallback would
// not catch it. Both a missing field and an explicit 0 read as round 1.
export function effectiveRound(p: FixDTO): number {
  return p.remediation_round && p.remediation_round > 0 ? p.remediation_round : 1;
}

// HAND_OVER_TRY is the try number of a round's hand-over row: the escalated
// row written once the round's tries are spent, which is no try of its own.
export const HAND_OVER_TRY = 0;

// tryLabel names a try by its round and its number in that round,
// "round 2, try 1". A round of 0 or none reads as round 1 (effectiveRound),
// and the round's hand-over row reads "round 2, hand-over".
export function tryLabel(round: number | undefined, tryNumber: number): string {
  const r = round && round > 0 ? round : 1;
  return tryNumber === HAND_OVER_TRY ? `round ${r}, hand-over` : `round ${r}, try ${tryNumber}`;
}

// tryBefore reports whether try a was made before try b: an earlier round, or
// an earlier try of the same round, the round's hand-over row coming after
// every try of its round.
export function tryBefore(
  a: { remediation_round?: number; try: number },
  b: { remediation_round?: number; try: number },
): boolean {
  const ra = a.remediation_round && a.remediation_round > 0 ? a.remediation_round : 1;
  const rb = b.remediation_round && b.remediation_round > 0 ? b.remediation_round : 1;
  if (ra !== rb) return ra < rb;
  const rank = (t: number) => (t === HAND_OVER_TRY ? Number.POSITIVE_INFINITY : t);
  return rank(a.try) < rank(b.try);
}

// FixGroup aggregates every remediation try for one (release, round):
// the tries newest first, the union of the projects they touch and the
// nodes they resolve, the newest try's status, and the newest try
// that carries a pull request.
export interface FixGroup {
  key: string;
  releaseId: string;
  round: number;
  tries: FixDTO[];               // newest first
  latest: FixDTO;                // tries[0]
  projects: string[];            // see fixProjects; union, sorted
  nodeIds: string[];             // resolved-node union, sorted
  latestPrFix: FixDTO | null;
}

// fixProjects lists the projects one try touched, as recorded by
// agent-remediation on the fix itself: the failing nodes' projects plus
// the edited ones — the same set the server's `project` list filter matches
// a fix on, so the Projects column and the filter agree. A remediation
// node id is "<schema>.<table>" and names no project, so nothing is ever
// parsed out of resolved_node_ids. A fix served by an agent-remediation
// that predates the field carries no projects; the owning projects its pull
// requests split into (the legacy '' sentinel dropped) are the nearest thing
// it does carry, and a legacy fix with neither yields none.
export function fixProjects(p: FixDTO): string[] {
  const carried = (p.projects ?? []).filter((s) => s !== '');
  if (carried.length > 0) return Array.from(new Set(carried)).sort();
  return Array.from(new Set(fixPrProjects(p).filter((s) => s !== ''))).sort();
}

// groupFixes buckets fixes by (release_id, remediation round), newest
// group first by its latest try. Within a group the tries are newest
// first (created_at, then tryBefore). `projects` unions each try's
// fixProjects; `nodeIds` unions their resolved nodes; `latestPrFix`
// is the newest try that has opened a pull request, or null when none
// has.
export function groupFixes(fixes: FixDTO[]): FixGroup[] {
  const buckets = new Map<string, FixDTO[]>();
  for (const p of fixes) {
    const key = `${p.release_id} ${effectiveRound(p)}`;
    const list = buckets.get(key);
    if (list) list.push(p);
    else buckets.set(key, [p]);
  }

  const groups: FixGroup[] = [];
  for (const [key, tries] of buckets) {
    tries.sort((a, b) => {
      if (a.created_at !== b.created_at) return a.created_at < b.created_at ? 1 : -1;
      return tryBefore(a, b) ? 1 : tryBefore(b, a) ? -1 : 0;
    });
    const latest = tries[0];

    const projectSet = new Set<string>();
    for (const p of tries) for (const s of fixProjects(p)) projectSet.add(s);
    const nodeSet = new Set<string>();
    for (const p of tries) for (const n of fixNodeIds(p)) nodeSet.add(n);

    groups.push({
      key,
      releaseId: latest.release_id,
      round: effectiveRound(latest),
      tries,
      latest,
      projects: Array.from(projectSet).sort(),
      nodeIds: Array.from(nodeSet).sort(),
      latestPrFix: tries.find(p => fixPullRequests(p).some(pr => pr.pr_state)) ?? null,
    });
  }

  groups.sort((a, b) => {
    if (a.latest.created_at !== b.latest.created_at) return a.latest.created_at < b.latest.created_at ? 1 : -1;
    if (a.releaseId !== b.releaseId) return a.releaseId < b.releaseId ? -1 : 1;
    return b.round - a.round;
  });
  return groups;
}

// The stages a release check can pass through, in order, before it ends with
// a terminal status (promoted/rejected/superseded for a candidate release's,
// passed/failed for a fix's).
const RELEASE_CHECK_STAGES = ['received', 'compiling', 'parsing', 'seed_building', 'validating'];

// A stage a release check has not reached yet, with the condition on it when
// it does not run on every path.
export interface UpcomingStage {
  stage: string;
  condition?: string;
}

// What the timeline reads of a release check: what decides its path through
// the stages (how its artifact is parsed, 'dbt', 'python' or 'ontology', and
// whether it is a bootstrap release) and what its parse reported (see
// parseLines). The parse fields are absent until the parse reports them.
export interface TimelineCheck {
  manifestKind?: string;
  bootstrap?: boolean;
  // Milliseconds the parse took without its contract stages, and milliseconds
  // those stages took.
  parse_ms?: number;
  contracts_ms?: number;
  // How many contracts the parse wrote, by family (`dbt`, `python`); a family
  // with none has no key.
  contracts_by_family?: Record<string, number>;
}

// The manifest kinds whose release skips the compile leg and the seed build:
// CI builds and uploads the artifact before the release is posted, and only a
// dbt release compiles and builds seeds. release-controller's
// ManifestKind.HasCompileLeg decides the same.
const KINDS_WITHOUT_COMPILE = ['python', 'ontology'];

// releaseCheckPath lists the stages a release check of this kind passes
// through, in order, naming the condition on each stage that does not run on
// every path:
// - compiling: dbt only — the CI of any other kind builds and uploads its
//   artifact (a Python project's contract bundle, the ontology file) before
//   the release is posted, so activation goes straight to parsing;
// - parsing: every release check;
// - seed_building: dbt only, and only when the validation set holds a new or
//   changed seed;
// - validating: unless nothing changed against the live release (then a candidate
//   release promotes and a fix's release check ends at once), and never for
//   a bootstrap release, which promotes right after parsing.
// An unknown kind takes the dbt path, the common one.
function releaseCheckPath(check: TimelineCheck): UpcomingStage[] {
  const compiles = !KINDS_WITHOUT_COMPILE.includes(check.manifestKind ?? '');
  const path: UpcomingStage[] = [{ stage: 'received' }];
  if (compiles) path.push({ stage: 'compiling' });
  path.push({ stage: 'parsing' });
  if (check.bootstrap) return path;
  if (compiles) path.push({ stage: 'seed_building', condition: 'if seeds changed' });
  path.push({ stage: 'validating', condition: 'unless nothing changed' });
  return path;
}

// upcomingStages lists the stages a release check has not reached yet on the
// path its kind takes, so the timeline can show where an in-flight check is
// going without promising a stage it will never enter. Empty once the latest
// transition is not a stage — the check has finished — or when nothing has
// been recorded.
export function upcomingStages(transitions: ReleaseTransition[], check: TimelineCheck = {}): UpcomingStage[] {
  const last = transitions[transitions.length - 1];
  if (!last) return [];
  const reached = RELEASE_CHECK_STAGES.indexOf(last.to);
  if (reached === -1) return [];
  return releaseCheckPath(check).filter(s => RELEASE_CHECK_STAGES.indexOf(s.stage) > reached);
}

const PARSING_LABEL = 'parsing';
const CONTRACTS_LABEL = 'generating contracts';

// seconds writes a duration in milliseconds as seconds with one decimal. It
// rounds the integer tenths half up, so the text does not depend on the binary
// form of the fraction (Number.toFixed turns 2.05 into 2.0), and a long
// duration stays in seconds.
function seconds(ms: number): string {
  const tenths = Math.round(ms / 100);
  return `${Math.floor(tenths / 10)}.${tenths % 10}`;
}

// contractCount writes the tail of the contract generation line: the contracts
// written, then the families they belong to. A parse that did not count its
// contracts leaves it empty, and a release with none says `0 contracts` with
// no families. The count and its noun are joined by a no-break space, so a
// narrow line never ends on the number.
function contractCount(byFamily: Record<string, number> | undefined): string {
  if (!byFamily) return '';
  const families = Object.keys(byFamily).sort();
  const total = families.reduce((sum, family) => sum + byFamily[family], 0);
  const noun = total === 1 ? 'contract' : 'contracts';
  const split = families.map(family => `${family} ${byFamily[family]}`).join(', ');
  return ` · ${total}\u00a0${noun}${split ? ` (${split})` : ''}`;
}

// parseLines lists the lines the timeline shows under the parsing step: how
// long the parse took and how long it took to generate the contracts, with how
// many it wrote by family. A release check gets a line for each duration it
// reported, so one that reported none has no line.
export function parseLines(check: TimelineCheck): { label: string; text: string }[] {
  const lines: { label: string; text: string }[] = [];
  if (typeof check.parse_ms === 'number') {
    lines.push({ label: PARSING_LABEL, text: `${PARSING_LABEL} ${seconds(check.parse_ms)} s` });
  }
  if (typeof check.contracts_ms === 'number') {
    lines.push({
      label: CONTRACTS_LABEL,
      text: `${CONTRACTS_LABEL} ${seconds(check.contracts_ms)} s${contractCount(check.contracts_by_family)}`,
    });
  }
  return lines;
}

// shortSha abbreviates a commit to the seven characters git itself shows.
export function shortSha(sha: string): string {
  return sha.slice(0, 7);
}
