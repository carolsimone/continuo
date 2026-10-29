ALTER TABLE task_tracker
    ADD COLUMN IF NOT EXISTS manifest_version VARCHAR(50) NOT NULL DEFAULT '';

COMMENT ON COLUMN task_tracker.manifest_version IS
    'The manifest_version snapshotted on the EXECUTES edge at SnapshotGraph time. Pinned for the lifetime of the task.';
