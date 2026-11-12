-- A rebaseline release is a bootstrap that re-baselines fingerprints: its
-- promotion tells run-controller to record every live seed as loaded at its new
-- hash instead of reloading it. It is only valid together with bootstrap.
ALTER TABLE release_pipeline_runs ADD COLUMN rebaseline boolean NOT NULL DEFAULT false;
ALTER TABLE release_pipeline_runs
  ADD CONSTRAINT release_pipeline_runs_rebaseline_needs_bootstrap CHECK (NOT rebaseline OR bootstrap);
