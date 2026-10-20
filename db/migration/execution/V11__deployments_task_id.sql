-- A failed attempt's next attempt is looked up by task and attempt number
-- (DeploymentRepository.HasAttemptFrom), so a task never gets two attempts
-- of one number, whatever their Jobs are named.
CREATE INDEX idx_deployments_task_id ON deployments (task_id);
