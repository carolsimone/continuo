ALTER TABLE scheduler_tracker
    ADD COLUMN total_task_count    INTEGER,
    ADD COLUMN terminal_task_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_scheduler_tracker_finalization
    ON scheduler_tracker (schedule_id)
    WHERE status = 'running' AND initialization_status = 'completed';
