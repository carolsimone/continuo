-- A deployment is keyed by the run it belongs to.
ALTER TABLE deployments RENAME COLUMN schedule_id TO run_id;

-- Serves the stop of a cancelled run: the rows of one run that still hold work.
DROP INDEX idx_deployments_open_by_run;
CREATE INDEX idx_deployments_open_by_run ON deployments (run_id)
    WHERE mode = 'production' AND status IN ('pending','reserved','starting','running','terminating');
