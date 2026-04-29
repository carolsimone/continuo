ALTER TABLE schedule_catalog
    RENAME COLUMN manifest_versions TO service_metadata;

ALTER TABLE scheduler_tracker
    RENAME COLUMN manifest_versions TO service_metadata;

UPDATE schedule_catalog
SET service_metadata = (
    SELECT jsonb_object_agg(key, jsonb_build_object('manifest_version', value, 'image_tag', ''))
    FROM jsonb_each_text(service_metadata)
)
WHERE jsonb_typeof(service_metadata) = 'object'
  AND service_metadata != '{}'::jsonb
  AND EXISTS (
      SELECT 1 FROM jsonb_each(service_metadata) WHERE jsonb_typeof(value) = 'string'
  );

UPDATE scheduler_tracker
SET service_metadata = (
    SELECT jsonb_object_agg(key, jsonb_build_object('manifest_version', value, 'image_tag', ''))
    FROM jsonb_each_text(service_metadata)
)
WHERE jsonb_typeof(service_metadata) = 'object'
  AND service_metadata != '{}'::jsonb
  AND EXISTS (
      SELECT 1 FROM jsonb_each(service_metadata) WHERE jsonb_typeof(value) = 'string'
  );
