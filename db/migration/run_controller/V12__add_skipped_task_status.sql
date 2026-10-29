-- Add 'skipped' as a valid task_tracker status for cascade-blocked downstream nodes
ALTER TABLE task_tracker DROP CONSTRAINT task_tracker_status_check;
ALTER TABLE task_tracker ADD CONSTRAINT task_tracker_status_check
    CHECK (status = ANY (ARRAY['pending', 'running', 'succeeded', 'failed', 'cancelled', 'skipped']));
