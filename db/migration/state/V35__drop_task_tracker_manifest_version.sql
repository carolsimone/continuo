-- V35__drop_task_tracker_manifest_version.sql
-- task_tracker.manifest_version was never populated for release-sourced
-- topology (the promoted topology carries no manifest identity), so every row
-- holds ''. image_tag is the per-task provenance; the column is dropped.

ALTER TABLE task_tracker
    DROP COLUMN IF EXISTS manifest_version;

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
