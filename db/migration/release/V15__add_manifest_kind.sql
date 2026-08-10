-- kind: how the service's release artifact is authored and parsed —
-- 'dbt' (manifest.json, compile leg) or 'python' (contract.yaml, CI-compiled,
-- skips the compile leg). The DEFAULT backfills existing rows (all dbt by
-- construction: the python path was dark before this migration) and is then
-- dropped so every subsequent INSERT states the kind explicitly.
ALTER TABLE releases ADD COLUMN kind TEXT NOT NULL DEFAULT 'dbt'
    CHECK (kind IN ('dbt', 'python'));
ALTER TABLE releases ALTER COLUMN kind DROP DEFAULT;

ALTER TABLE service_prod ADD COLUMN manifest_kind TEXT NOT NULL DEFAULT 'dbt'
    CHECK (manifest_kind IN ('dbt', 'python'));
ALTER TABLE service_prod ALTER COLUMN manifest_kind DROP DEFAULT;
