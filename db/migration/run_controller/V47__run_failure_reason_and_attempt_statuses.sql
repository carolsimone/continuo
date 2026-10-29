-- The values below repeat run.RunFailureReasons() and run.TaskStatuses() in
-- state/domain/aggregate/run; the schema tests fail if they ever differ.
--
-- Why a run failed when no task of it failed: stalled (no attempt in flight
-- and no progress for five minutes), empty_selection or invalid_node_type (a
-- cron fire's selection could not run). NULL for every other run.
ALTER TABLE scheduler_tracker ADD COLUMN failure_reason varchar(32) NULL;
ALTER TABLE scheduler_tracker ADD CONSTRAINT scheduler_tracker_failure_reason_check
    CHECK (failure_reason IN ('stalled', 'empty_selection', 'invalid_node_type'));

COMMENT ON COLUMN scheduler_tracker.failure_reason IS 'Why the run failed when no task of it failed; NULL otherwise';

-- A task is dispatched once state has dispatched an attempt of it, and
-- queued while execution-controller waits for a slot for that attempt.
ALTER TABLE task_tracker DROP CONSTRAINT task_tracker_status_check;
ALTER TABLE task_tracker ADD CONSTRAINT task_tracker_status_check
    CHECK (status IN ('pending', 'dispatched', 'queued', 'running', 'succeeded', 'failed', 'cancelled', 'skipped'));
