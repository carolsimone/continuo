-- Add task_retry_count to deployment_outbox to carry the task-level retry count
-- through to node.deployed:v1 so k8s-controller no longer needs to call state gRPC.
-- DEFAULT 0: existing in-flight rows are initial attempts (retry count = 0).
ALTER TABLE deployment_outbox ADD COLUMN task_retry_count INT NOT NULL DEFAULT 0;
