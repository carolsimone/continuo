-- Add task_max_retries to k8s_status_outbox so k8s-controller can decide
-- permanent failure vs. retry without calling state gRPC.
-- DEFAULT 3: matches the application default and covers existing in-flight rows.
ALTER TABLE k8s_status_outbox ADD COLUMN task_max_retries INT NOT NULL DEFAULT 3;
