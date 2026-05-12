-- V18: composite index for per-node history lookups.
-- state.ListNodeRuns filters task_tracker by (service_name, schema_name,
-- table_name) and joins to scheduler_tracker + task_execution. Without this
-- index the predicate requires a sequential scan that grows with total task
-- history. The composite supports equality on all three columns; ordering
-- matches the most-selective-first heuristic (service tends to dominate).
CREATE INDEX idx_task_tracker_node_identity
    ON task_tracker (service_name, schema_name, table_name);
