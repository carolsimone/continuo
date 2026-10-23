-- Which attempt of its task a production deployment runs, as state numbered it
-- on the dispatch; NULL on a release leg's row. Each attempt of a task is
-- accepted once: a repeated dispatch finds the row and is answered from it.
ALTER TABLE deployments
    ADD COLUMN attempt integer GENERATED ALWAYS AS ((job_params ->> 'attempt')::integer) STORED;
CREATE UNIQUE INDEX uq_deployments_task_attempt ON deployments (task_id, attempt)
    WHERE mode = 'production' AND attempt IS NOT NULL;

-- The unique index serves every lookup of a task's deployments.
DROP INDEX idx_deployments_task_id;

-- The task.status.updated:v2 payload execution-controller last emitted for the
-- attempt; a repeated dispatch of the attempt is answered with it.
ALTER TABLE deployments ADD COLUMN last_report jsonb NULL;
