-- ontology: the kind of the install's ontology service, whose artifact is the
-- company ontology file (ontology.yaml). A run and a service_prod row may hold
-- it beside the kinds they already hold.
ALTER TABLE release_pipeline_runs
  DROP CONSTRAINT release_pipeline_runs_manifest_kind_check,
  ADD CONSTRAINT release_pipeline_runs_manifest_kind_check
    CHECK (manifest_kind IN ('dbt', 'python', 'python-odcs', 'ontology'));
ALTER TABLE service_prod
  DROP CONSTRAINT service_prod_manifest_kind_check,
  ADD CONSTRAINT service_prod_manifest_kind_check
    CHECK (manifest_kind IN ('dbt', 'python', 'python-odcs', 'ontology'));
