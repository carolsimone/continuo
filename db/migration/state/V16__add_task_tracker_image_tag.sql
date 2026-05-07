-- V16__add_task_tracker_image_tag.sql
-- Adds image_tag as a peer column to task_tracker.manifest_version (V13)
-- so per-task audit captures both halves of the metadata pair. Required
-- for Feature 2 (rebase) where one run carries split metadata.
--
-- Backfill copies image_tag from scheduler_tracker.service_metadata
-- (JSONB keyed by service_name) for each task's parent run. Tasks whose
-- service_name doesn't appear in the parent's metadata get '' (matches
-- V13's existing behaviour for manifest_version).

ALTER TABLE task_tracker
    ADD COLUMN image_tag character varying(255) NOT NULL DEFAULT '';

UPDATE task_tracker t
SET image_tag = COALESCE(
    (SELECT s.service_metadata->t.service_name->>'image_tag'
     FROM scheduler_tracker s
     WHERE s.schedule_id = t.schedule_id),
    ''
)
WHERE image_tag = '';

COMMENT ON COLUMN task_tracker.image_tag IS
    'The image_tag snapshotted on the EXECUTES edge at SnapshotGraph time. Pinned at task creation and never mutated. Peer to manifest_version (V13).';
