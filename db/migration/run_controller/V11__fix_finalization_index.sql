DROP INDEX IF EXISTS idx_scheduler_tracker_finalization;

CREATE INDEX idx_scheduler_tracker_finalization
    ON scheduler_tracker (schedule_id)
    WHERE status = 'running' AND initialization_status = 'completed';
