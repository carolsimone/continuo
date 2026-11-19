-- How long the parse of a release took, as topology-controller reports it on
-- manifest.loaded.candidate:v2: the milliseconds of the parse itself, the
-- milliseconds spent on its contracts, and how many contracts the contract bundle
-- holds by family ({"dbt": 3, "python": 2}). A run the parse has not reached, and
-- one parsed by a topology-controller that does not report them, holds NULL.
ALTER TABLE release_pipeline_runs
  ADD COLUMN parse_ms BIGINT,
  ADD COLUMN contracts_ms BIGINT,
  ADD COLUMN contracts_by_family JSONB;
