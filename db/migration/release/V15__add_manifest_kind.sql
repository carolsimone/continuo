-- kind: how the service's release artifact is authored and parsed —
-- 'dbt' (manifest.json, compile leg) or 'python' (contract.yaml, CI-compiled,
-- skips the compile leg). The DEFAULT serves two purposes: it backfills
-- existing rows (all dbt by construction, since the python path did not
-- exist before this migration), and it keeps the column writable through
-- the rolling-upgrade window. The db-init-migrate Helm hook runs
-- pre-install/pre-upgrade, so this migration applies while old
-- release-controller pods are still serving traffic; the old binary's
-- Save/Upsert calls omit these columns, and without a DEFAULT those
-- INSERTs would violate NOT NULL until every pod is replaced. The same
-- DEFAULT also keeps a `helm rollback` to the old binary working, since a
-- rolled-back binary hits this same already-migrated schema. The new
-- binary always writes both columns explicitly (enforced by its
-- constructors), so the DEFAULT is inert for it. Dropping the DEFAULT is
-- deferred to a future migration, once no binary that omits these columns
-- can run against this schema.
ALTER TABLE releases ADD COLUMN kind TEXT NOT NULL DEFAULT 'dbt'
    CHECK (kind IN ('dbt', 'python'));

ALTER TABLE service_prod ADD COLUMN manifest_kind TEXT NOT NULL DEFAULT 'dbt'
    CHECK (manifest_kind IN ('dbt', 'python'));
