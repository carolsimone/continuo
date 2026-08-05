-- S3 URI of the release's code-bundle contract document
-- (code-bundles/<release_id>/bundle.json), recorded from the parse result and
-- carried into release.promoted:v1.
ALTER TABLE releases ADD COLUMN code_bundle_uri TEXT NOT NULL DEFAULT '';
