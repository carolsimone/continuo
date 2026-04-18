-- Add task_max_retries to deployment_outbox to carry the maximum allowed retries
-- through to node.deployed:v1 so k8s-controller can enforce the correct limit.
-- DEFAULT 3: matches the hard-coded default used before this column existed.
ALTER TABLE deployment_outbox ADD COLUMN task_max_retries INT NOT NULL DEFAULT 3;
