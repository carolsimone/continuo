export interface ScheduleSummary {
  schedule_name: string;
  cron_expression: string;
  description: string;
  timezone: string;
  is_running: boolean;
  last_run_at: string | null;
  last_run_status: string;  // "succeeded"/"failed"/"cancelled"/""
  last_run_id: string | null;  // null means never run
}

export interface SchedulesResponse {
  schedules: ScheduleSummary[];
}

export interface Scheduler {
  schedule_id: string;
  schedule_name: string;
  status: string;
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  cancelled_at: string | null;
  cancelled_by: string;
}

export interface Task {
  task_id: string;
  service_name: string;
  schema_name: string;
  table_name: string;
  job_name: string;
  status: string;
  retry_count: number;
  max_retries: number;
  created_at: string | null;
}

export interface GraphNode {
  node_id: string;     // "{service}.{schema}.{table}"
  node_type: string;   // "dbt-model" | "dbt-seed" | "dbt-snapshot" | "python-model" | "python-csv"
  schedule_name: string;
  status?: string | null;
}

export interface GraphEdge {
  from_node_id: string;
  to_node_id: string;
}

export interface ScheduleGraph {
  nodes: GraphNode[];
  edges: GraphEdge[];
  topology_generation?: number;
}

export interface RunSummary {
  run_id: string;
  schedule_name: string;
  terminal_status: string;
  created_at: string | null;
  completed_at: string | null;
}

export interface RunGraphNode {
  node_id: string;
  node_type: string;
  schedule_name: string;
  status: string | null;
}

export interface RunGraph {
  nodes: RunGraphNode[];
  edges: GraphEdge[];
  // Drift fields from /api/runs/:run_id/graph (populated by orchestrator
  // RunQueryService.GetRunGraph). 0 means "drift unknown" — see drift-helpers.
  run_topology_generation: number;
  latest_topology_generation: number;
}

export interface TaskExecution {
  id: string;
  task_id: string;
  error_message: string | null;
  execution_time_seconds: number | null;
  started_at: string | null;
  completed_at: string | null;
  log_s3_key: string | null;
  run_results_uri: string | null;
}

export interface NodeRun {
  run_id: string;
  schedule_name: string;
  kind: string;             // cron | trigger | rerun | rebase | single_node_run
  terminal_status: string;  // "" if in flight
  task_id: string;
  task_status: string;      // pending | running | succeeded | failed | cancelled
  retry_count: number;
  image_tag: string;
  manifest_version: string;
  operation: string;        // run | test | build
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
  log_s3_key: string | null;
  run_results_uri: string | null;
}

export interface NodeRunsResponse {
  runs: NodeRun[];
}

export interface ScheduleTopologySummary {
  schedule_name: string;
  node_count: number;
  last_updated_at: string | null;
}

export interface TopologyListResponse {
  schedules: ScheduleTopologySummary[];
}

// Who authored the release's commit. Resolved server-side from (repo, commit_sha)
// via GitHub. `login` (with `avatar_url` and profile `html_url`) is present when
// the commit email is linked to a GitHub account; otherwise only `name` — the
// git commit author metadata — is available. Absent when GitHub is not
// configured or the commit could not be resolved.
export interface ReleaseAuthor {
  login?: string;
  name?: string;
  avatar_url?: string;
  html_url?: string;
}

export interface ReleaseListItem {
  release_id: string;
  status: string; // received|compiling|parsing|seed_building|validating|promoted|rejected|superseded
  created_at: string;
  resolved_at: string | null;
  node_count: number;
  bootstrap: boolean;
  reject_reason?: string;
  repo?: string;
  commit_sha?: string;
  author?: ReleaseAuthor;
}

export interface ReleasesListResponse {
  releases: ReleaseListItem[];
  next_cursor: string;
}

export interface CurrentProd {
  current_prod_release_id: string;
  node_count: number;
  updated_at: string;
}

export interface NodeValidationResult {
  node_id: string;
  status: string;
  stage: string;          // "compile" | "seed_build" | "validation"
  file_path?: string;     // offending source path; present for compile/seed
  dbt_log_uri?: string;
  duration_ms?: number;
}

export interface ReleaseTransition {
  to: string;
  at: string;
}

export interface ReleaseDetail {
  release_id: string;
  status: string;
  transitions: ReleaseTransition[];
  validation_node_ids: string[] | null;
  reject_reason: string;
  reject_detail?: string;
  failing_nodes: string[] | null;
  per_node_results: NodeValidationResult[] | null;
  image_tags: Record<string, string>;
  bootstrap: boolean;
  // How many remediation rounds have run for this release. 1 for every release
  // that has not been retried; "Try again" increments it up to the 3-round cap.
  remediation_round: number;
  // The one service this candidate changes; every other entry in image_tags is
  // carried over from prod. Empty when the release row records none.
  changed_service?: string;
  // Where the change came from: the GitHub owner/name repo and the commit, and
  // the commit's page under the install's GitHub host, attached by the ui
  // server when both are recorded.
  repo?: string;
  commit_sha?: string;
  commit_url?: string;
  // How the release's artifact is parsed: 'dbt' or 'python'. Decides the
  // pipeline path — a python release has no compile leg.
  manifest_kind?: string;
}

export interface NodeSummary {
  service_name: string;
  schema_name: string;
  table_name: string;
  run_count: number;
  success_rate_pct: number | null;
  avg_duration_sec: number | null;
  p95_duration_sec: number | null;
  flaky_rate_pct: number;
  last_status: string | null;
  last_run_at: string | null;
  operation: string;        // run | test | build
}

export interface NodesResponse {
  total_count: number;
  nodes: NodeSummary[];
}

// The distinct active service names, from GET /api/nodes/services. Feeds the
// catalog and Remediation-tab service filters.
export interface ServicesResponse {
  services: string[];
}

// Where the user arrived at /node/:fqn from — drives the back link.
export type NodeDetailFrom =
  | { type: 'schedule'; name: string; mode: 'run' | 'latest' }
  | { type: 'nodes' };

// One file a proposal changes. content_uri and diff_uri address S3 objects
// holding the full corrected file and its unified diff. target_node_id names
// the specific resolved node this edit fixes, when a batched proposal's
// edits map one-to-one to the nodes it resolves; absent when the edit is not
// attributable to a single node (e.g. a shared contract file).
export interface FileEditDTO {
  path: string;
  content_uri: string;
  diff_uri: string;
  target_node_id?: string;
}

// How a batched remediation attempt ended for one specific node it resolved.
export interface NodeOutcomeDTO {
  status: string;
  reason: string;
}

// One pull request opened from a proposal. A proposal splits into one entry
// per owning service; service === '' is the legacy whole-proposal group.
export interface PullRequestDTO {
  service: string;
  repo: string;
  branch: string;
  pr_url: string;
  pr_number: number;
  // pr_state is the terminal state from GitHub: "merged" or "rejected", or
  // "open"/"opening" while the PR has not yet closed, "" or "failed" while
  // no live PR exists for this service.
  pr_state: string;
  pr_opened_at: string;
  pr_opened_by: string;
  pr_closed_at: string;
}

// One fix-verification run a batched attempt ran for one edited service, with
// the durable summary agent-remediation keeps of how it went. phase is ''
// until the reconciler first reads the run, then queued | running | passed |
// failed. activated_at is when the run left the pipeline's queue ('' while
// queued); error is the named per-node errors of a failed run.
export interface VerificationDTO {
  service: string;
  kind: string;
  run_id: string;
  phase: '' | 'queued' | 'running' | 'passed' | 'failed';
  activated_at: string;
  error: string;
}

export interface ProposalDTO {
  id: string;
  source: string;
  release_id: string;
  node_id: string;
  error_signature: string;
  attempt: number;
  // Lifecycle of one fix attempt. Two are in flight and carry no reviewable
  // fix yet: 'generating' while the fix is being produced, and 'verifying'
  // while a verification run judges the produced fix through the full
  // validation pipeline. The rest are terminal: 'proposed' (a fix ready for
  // review), 'skipped', 'failed', 'escalated'.
  status: string;
  confidence: string;
  rationale: string;
  proposed_sql_uri: string;
  diff_uri: string;
  candidate_fix_sql_uri: string;
  candidate_fix_diff_uri: string;
  source_resolved: boolean;
  repo: string;
  commit_sha: string;
  file_path: string;
  model: string;
  created_at: string;
  pr_url: string;
  pr_number: number;
  pr_state: string;
  pr_opened_at: string;
  pr_opened_by: string;
  pr_closed_at: string;
  // verification_run_id is the run id of the first verification; the
  // single-run view of verifications. Set while the attempt is 'verifying'
  // and still set on the 'proposed' or 'failed' row it became, so the run
  // that decided an attempt is always reachable from it. Empty on an attempt
  // judged without one.
  verification_run_id: string;
  // verify_error is why that run rejected the fix — the reason a python
  // contract attempt reached 'failed'. Empty unless verification failed.
  verify_error: string;
  // Every file this proposal changes. Absent or empty on a proposal that has
  // no real repository source to edit — a candidate-only fix — in which case
  // the single-file diff_uri above still points at a previewable diff.
  edits?: FileEditDTO[];
  // remediation_round is the release's remediation round this attempt
  // belongs to. Absent on a proposal from before rounds existed, which the
  // reader treats the same as round 1.
  remediation_round?: number;
  // resolved_node_ids lists every failing node this attempt addresses. Absent
  // or empty on a legacy single-node proposal (or one from before batching
  // existed), in which case node_id — the representative (first resolved)
  // node — is the attempt's sole member. See proposalNodeIds.
  resolved_node_ids?: string[];
  // node_outcomes carries how this attempt ended for each node it resolved,
  // keyed by node id. Absent or missing an entry for a legacy row or a node
  // the attempt did not record separately, in which case the proposal's own
  // status/rationale describe that node too. See proposalStatusForNode /
  // proposalReasonForNode.
  node_outcomes?: Record<string, NodeOutcomeDTO>;
  // verifications lists the verification runs this attempt ran, one per
  // edited service that needed verification. Empty on a proposal judged
  // without one, or a legacy row that only ever tracked a single
  // verification_run_id.
  verifications?: VerificationDTO[];
  // pull_requests is one entry per (proposal, service) pull request; absent
  // or empty on a proposal that never entered the PR lifecycle, or one from
  // before the per-service split existed — in which case the singular pr_*
  // fields above describe its one (legacy, service '') pull request. See
  // proposalPullRequests.
  pull_requests?: PullRequestDTO[];
  // pr_services is the sorted owning-service groups this proposal's pull
  // requests split into; absent, or [''], for a legacy (unsplit) proposal.
  // See proposalPrServices.
  pr_services?: string[];
  // services is every service this attempt touched — the failing nodes'
  // services plus the edited ones, sorted — the same set the server's
  // `service` list filter matches a proposal on. Absent from a proposal
  // served by an agent-remediation that predates the field. See
  // proposalServices.
  services?: string[];
}

// A verification run judges one candidate against the full validation
// pipeline without becoming prod: a fix-verification run for a remediation
// attempt, or a standalone pipeline check. verifies_release_id names the
// release the run judges a fix for.
export interface VerificationRunDetail {
  run_id: string;
  status: string; // received|compiling|parsing|seed_building|validating|passed|failed
  changed_service: string;
  verifies_release_id: string;
  attempt: number;
  created_at: string;
  activated_at: string;
  finished_at: string;
  transitions: ReleaseTransition[];
  validation_node_ids: string[] | null;
  failing_nodes: string[] | null;
  fail_reason: string;
  fail_detail: string;
  per_node_results: NodeValidationResult[] | null;
  image_tags: Record<string, string>;
  manifest_kind: string;
}

// One row of a release's verification-run list, newest first.
export interface VerificationRunSummary {
  run_id: string;
  status: string;
  service: string;
  attempt: number;
  created_at: string;
  activated_at: string;
  finished_at: string;
  fail_reason?: string;
}

// The one run — a release candidate or a verification run — currently
// occupying the pipeline's single slot. verifies_release_id and attempt are
// present only when run_kind is 'verification'.
export interface PipelineActive {
  run_id: string;
  run_kind: 'candidate' | 'verification';
  status: string;
  service: string;
  since: string;
  verifies_release_id?: string;
  attempt?: number;
}

export interface PipelineResponse {
  active: PipelineActive | null;
}
