-- Guard: changed_service backfills to '' for every existing row, which produces
-- a broken s3://bucket//<id>/manifest.json key and a malformed service_prod
-- pointer for any release still in flight. Terminal releases never reassemble
-- their manifest set, so only nonterminal rows are dangerous. Abort loudly if
-- any exist; the operator must drain or reject them before migrating.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM releases WHERE status IN ('received','parsing','validating')) THEN
    RAISE EXCEPTION 'cannot add releases.changed_service: % nonterminal release(s) exist (received/parsing/validating); drain or reject them before migrating',
      (SELECT count(*) FROM releases WHERE status IN ('received','parsing','validating'));
  END IF;
END $$;

-- Add changed_service: which dbt service this release's delta belongs to.
-- Needed at promote time to upsert the service_prod pointer.
-- Drop manifests_uri: the assembled manifest-key list is emitted in
-- release.requested:v1 and never needs to be re-read from the releases table.
ALTER TABLE releases ADD COLUMN changed_service TEXT NOT NULL DEFAULT '';
ALTER TABLE releases DROP COLUMN manifests_uri;
