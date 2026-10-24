-- The guard table is keyed by run: state cancels a run, not a schedule.
ALTER TABLE cancelled_schedules RENAME TO cancelled_runs;
ALTER TABLE cancelled_runs RENAME COLUMN schedule_id TO run_id;
COMMENT ON TABLE cancelled_runs IS
    'a run cancelled by state: no attempt of it starts, and every Job of it is deleted; kept for the replay horizon';

-- stopped: the attempt was asked to stop because its run was cancelled and did
-- not succeed first.
ALTER TABLE deployments DROP CONSTRAINT deployments_outcome_check;
ALTER TABLE deployments ADD CONSTRAINT deployments_outcome_check
    CHECK (outcome IN ('ok','failed','skipped','stopped') OR outcome IS NULL);

-- Serves the stop of a cancelled run: the rows of one run that still hold work.
CREATE INDEX idx_deployments_open_by_run ON deployments (schedule_id)
    WHERE mode = 'production' AND status IN ('pending','reserved','starting','running','terminating');
