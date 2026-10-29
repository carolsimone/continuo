-- Fix invalid timestamps constraint on task_execution
-- The original V3 constraint required started_at >= created_at, which is
-- wrong: the job starts before its execution record is persisted.
-- Replace with the correct invariant: completed_at >= started_at (when both present).

ALTER TABLE task_execution DROP CONSTRAINT valid_execution_timestamps;

ALTER TABLE task_execution ADD CONSTRAINT valid_execution_timestamps
    CHECK (
        (completed_at IS NULL)
        OR (started_at IS NULL)
        OR (completed_at >= started_at)
    );
