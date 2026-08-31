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
  status: string; // received|compiling|parsing|seed_building|validating|promoted|validated|rejected|superseded
  created_at: string;
  resolved_at: string | null;
  node_count: number;
  bootstrap: boolean;
  shadow: boolean;
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
  shadow: boolean;
  // How many remediation rounds have run for this release. 1 for every release
  // that has not been retried; "Try again" increments it up to the 3-round cap.
  remediation_round: number;
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

// One shadow release a batched attempt ran to verify its fix for one edited
// service. A multi-service proposal can carry several: one per service whose
// edits needed a shadow release to judge.
export interface VerificationDTO {
  service: string;
  kind: string;
  shadow_release_id: string;
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
  // while a shadow release runs the produced fix through the full validation
  // pipeline. The rest are terminal: 'proposed' (a fix ready for review),
  // 'skipped', 'failed', 'escalated'.
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
  // shadow_release_id names the release that ran this attempt's fix through
  // the whole validation pipeline to decide whether it holds. Set while the
  // attempt is 'verifying' and still set on the 'proposed' or 'failed' row it
  // became, so the release that decided an attempt is always reachable from
  // it. Empty on an attempt judged without one.
  shadow_release_id: string;
  // verify_error is why that release rejected the fix — the reason a python
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
  // verifications lists the shadow releases this attempt ran, one per edited
  // service that needed verification. Empty on a proposal judged without one,
  // or a legacy row that only ever tracked a single shadow_release_id.
  verifications?: VerificationDTO[];
}
