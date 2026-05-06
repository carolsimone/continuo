export interface ScheduleSummary {
  schedule_name: string;
  cron_expression: string;
  description: string;
  timezone: string;
  is_running: boolean;
  last_run_at: string | null;
  last_run_status: string;  // "succeeded"/"failed"/"cancelled"/""
  last_run_id: string | null;  // null means never run
  // Drift fields populated by orchestrator.ListActiveRunDrifts via
  // ui-service /api/schedules. Both null when the schedule has no in-flight run.
  active_run_topology_generation: number | null;
  active_run_id: string | null;
}

// Top-level shape of the /api/schedules response. The route returns
// `latest_topology_generation` alongside the schedules array; capture both.
export interface SchedulesResponse {
  schedules: ScheduleSummary[];
  latest_topology_generation: number;
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
