ALTER TABLE deployment_outbox
    ADD COLUMN IF NOT EXISTS image_tag VARCHAR(255) NOT NULL DEFAULT '';

COMMENT ON COLUMN deployment_outbox.image_tag IS 'Content-addressed image tag snapshotted at SnapshotGraph time. Pinned for the lifetime of the task.';
