-- db/migration/state/V8__add_manifest_versions.sql
-- Stores the manifest file versions that were active when a schedule was loaded
-- or activated. Keyed by service name, e.g. {"service_a": "v3", "service_b": "v5"}.

ALTER TABLE schedule_catalog
    ADD COLUMN IF NOT EXISTS manifest_versions JSONB NOT NULL DEFAULT '{}';

ALTER TABLE scheduler_tracker
    ADD COLUMN IF NOT EXISTS manifest_versions JSONB NOT NULL DEFAULT '{}';
