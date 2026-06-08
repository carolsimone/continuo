-- Add changed_service: which dbt service this release's delta belongs to.
-- Needed at promote time to upsert the service_prod pointer.
-- Drop manifests_uri: the assembled manifest-key list is emitted in
-- release.requested:v1 and never needs to be re-read from the releases table.
ALTER TABLE releases ADD COLUMN changed_service TEXT NOT NULL DEFAULT '';
ALTER TABLE releases DROP COLUMN manifests_uri;
