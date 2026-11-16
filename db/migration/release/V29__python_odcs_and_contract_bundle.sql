-- python-odcs: a Python service released as one ODCS contract per node, packed
-- into odcs-bundle.json. Each run also keeps its contract bundle reference and
-- the continuo-contracts version its parse used.
DO $$
DECLARE c record;
BEGIN
  FOR c IN
    SELECT con.conname, rel.relname
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    WHERE con.contype = 'c'
      AND rel.relname IN ('release_pipeline_runs', 'service_prod')
      AND pg_get_constraintdef(con.oid) LIKE '%manifest_kind%'
  LOOP
    EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', c.relname, c.conname);
  END LOOP;
END $$;
ALTER TABLE release_pipeline_runs ADD CONSTRAINT release_pipeline_runs_manifest_kind_check
  CHECK (manifest_kind IN ('dbt', 'python', 'python-odcs'));
ALTER TABLE service_prod ADD CONSTRAINT service_prod_manifest_kind_check
  CHECK (manifest_kind IN ('dbt', 'python', 'python-odcs'));
ALTER TABLE release_pipeline_runs
  ADD COLUMN candidate_contract_bundle_uri text NOT NULL DEFAULT '',
  ADD COLUMN candidate_contract_bundle_sha256 text NOT NULL DEFAULT '',
  ADD COLUMN continuo_contracts text NOT NULL DEFAULT '';
