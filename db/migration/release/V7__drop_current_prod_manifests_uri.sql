-- current_prod.manifests_uri is no longer needed: the per-service manifest
-- location is now reconstructed on demand from service_prod at advance-time.
ALTER TABLE current_prod DROP COLUMN manifests_uri;
