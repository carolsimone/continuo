-- V35__retire_task_tracker_manifest_version.sql
-- task_tracker.manifest_version was never populated for release-sourced
-- topology (the promoted topology carries no manifest identity), so every row
-- holds ''. image_tag is the per-task provenance and no current code reads or
-- writes the column.
--
-- The column is kept (NOT NULL DEFAULT '') rather than dropped: migrations run
-- as a pre-upgrade hook while the previous release's state pods are still
-- serving, and those pods still name the column in their task inserts and
-- node-history queries. Its default lets them keep working through the upgrade.

COMMENT ON COLUMN task_tracker.manifest_version IS
    'Unused: always empty. Retained only so replicas from the previous release keep working during an upgrade. image_tag is the per-task provenance.';

-- service_metadata (JSONB keyed by service_name) carries image_tag only; strip
-- the manifest_version key from rows written earlier.
UPDATE schedule_catalog
SET service_metadata = (
    SELECT jsonb_object_agg(e.key, CASE WHEN jsonb_typeof(e.value) = 'object' THEN e.value - 'manifest_version' ELSE e.value END)
    FROM jsonb_each(service_metadata) AS e
)
WHERE jsonb_typeof(service_metadata) = 'object'
  AND service_metadata <> '{}'::jsonb;

UPDATE scheduler_tracker
SET service_metadata = (
    SELECT jsonb_object_agg(e.key, CASE WHEN jsonb_typeof(e.value) = 'object' THEN e.value - 'manifest_version' ELSE e.value END)
    FROM jsonb_each(service_metadata) AS e
)
WHERE jsonb_typeof(service_metadata) = 'object'
  AND service_metadata <> '{}'::jsonb;
