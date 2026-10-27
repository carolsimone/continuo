-- A deployment is keyed by the run it belongs to.
ALTER TABLE deployments RENAME COLUMN schedule_id TO run_id;

-- Serves the stop of a cancelled run: the rows of one run that still hold work.
DROP INDEX idx_deployments_open_by_run;
CREATE INDEX idx_deployments_open_by_run ON deployments (run_id)
    WHERE mode = 'production' AND status IN ('pending','reserved','starting','running','terminating');

-- A production deployment's job_params (the JSON of a DeployTask) carries the
-- run's id under the key run_id. A row queued before this migration holds it
-- under schedule_id and would decode with an empty run id, so move the key.
-- Only the key moves: a candidate row's job_params has no such key and is left
-- as it is, and re-running the statement is a no-op.
UPDATE deployments
   SET job_params = (job_params - 'schedule_id') || jsonb_build_object('run_id', job_params->'schedule_id')
 WHERE job_params ? 'schedule_id';
