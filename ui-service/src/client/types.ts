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
  node_type: string;   // "dbt-model" | "dbt-seed" | "dbt-snapshot"
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
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
  log_s3_key: string | null;
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

export interface ReleaseListItem {
  release_id: string;
  status: string; // received|parsing|validating|promoted|rejected|superseded
  created_at: string;
  resolved_at: string | null;
  node_count: number;
  bootstrap: boolean;
  reject_reason?: string;
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
  failing_nodes: string[] | null;
  per_node_results: NodeValidationResult[] | null;
  image_tags: Record<string, string>;
  manifests_uri: string;
  bootstrap: boolean;
}
