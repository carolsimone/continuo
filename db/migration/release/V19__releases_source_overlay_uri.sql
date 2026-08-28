-- A shadow release may carry the S3 URI of a source-overlay tarball its
-- compile leg lays over the service project, so the release verifies a
-- proposed fix instead of the committed source. Empty for every other release.
ALTER TABLE releases ADD COLUMN source_overlay_uri TEXT NOT NULL DEFAULT '';
