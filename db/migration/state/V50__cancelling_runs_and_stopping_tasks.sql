-- The status lists below repeat run.SchedulerStatuses(), run.TaskStatuses() and
-- the contract's attempt_status vocabulary; the schema tests fail if they ever
-- differ.
--
-- A run whose cancel was requested is cancelling until every attempt of it has
-- ended, then cancelled. It still holds its schedule's single active slot.
ALTER TABLE scheduler_tracker DROP CONSTRAINT scheduler_tracker_status_check;
ALTER TABLE scheduler_tracker ADD CONSTRAINT scheduler_tracker_status_check
    CHECK (status IN ('pending', 'running', 'cancelling', 'succeeded', 'failed', 'cancelled', 'skipped'));

DROP INDEX uq_scheduler_tracker_active_per_schedule;
CREATE UNIQUE INDEX uq_scheduler_tracker_active_per_schedule
    ON scheduler_tracker (schedule_name)
    WHERE status IN ('pending', 'running', 'cancelling');
COMMENT ON INDEX uq_scheduler_tracker_active_per_schedule IS
    'Enforces at most one active (pending|running|cancelling) run per schedule; the DB-level backstop for the activation TOCTOU race.';

-- A task is stopping while a stop is requested of its open attempt, and
-- cancelled once the attempt has stopped.
ALTER TABLE task_tracker DROP CONSTRAINT task_tracker_status_check;
ALTER TABLE task_tracker ADD CONSTRAINT task_tracker_status_check
    CHECK (status IN ('pending', 'dispatched', 'queued', 'running', 'stopping', 'succeeded', 'failed', 'cancelled', 'skipped'));

-- An attempt of a cancelled run ends stopped, once no pod of it can run again.
-- stop_requested_at is when state asked execution-controller to stop it; NULL
-- until then.
ALTER TABLE task_attempts ADD COLUMN stop_requested_at timestamptz NULL;
COMMENT ON COLUMN task_attempts.stop_requested_at IS 'When state requested a stop of the attempt because its run was cancelled; NULL until then';

ALTER TABLE task_attempts DROP CONSTRAINT task_attempts_status_check;
UPDATE task_attempts SET status = 'stopped' WHERE status = 'cancelled';
ALTER TABLE task_attempts ADD CONSTRAINT task_attempts_status_check
    CHECK (status IN ('dispatched', 'queued', 'running', 'succeeded', 'failed', 'stopped'));
