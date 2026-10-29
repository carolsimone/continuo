-- Whole-DAG / single-node Test runs with no tests to run finalize as a new
-- terminal status `skipped` (nothing to test) rather than `failed`. Widen the
-- scheduler_tracker status CHECK to include it. Mirrors V12, which added the
-- task-level `skipped` status.
ALTER TABLE scheduler_tracker DROP CONSTRAINT scheduler_tracker_status_check;

ALTER TABLE scheduler_tracker ADD CONSTRAINT scheduler_tracker_status_check
    CHECK (status = ANY (ARRAY['pending', 'running', 'succeeded', 'failed', 'cancelled', 'skipped']));
