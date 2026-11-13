-- Each release's seed reload notices: what promoting it will do to every
-- changed seed whose load drops its table, the views the run will rebuild,
-- and, once the seed-build leg ran, what the warehouse holds.
ALTER TABLE release_pipeline_runs ADD COLUMN notices jsonb NOT NULL DEFAULT '[]'::jsonb;
