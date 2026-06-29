-- V19 extends the candidate-release dispatch path with the compile leg, a
-- sequential phase that runs BEFORE parsing/validation for a release_id: the
-- changed service's image is run as `dbt compile` to produce manifest.json,
-- which a sidecar uploads to S3 at the canonical key. Like validation and
-- seed_build, a compile deployment carries full (release_id, node_id) identity
-- and reuses the ValidationDeployTask shape; mode='compile'.
--
-- Plan 5 added ModeCompile in Go (deployment.go) + repo handling but, unlike the
-- seed-build leg (V18), shipped no migration — so every `INSERT ... mode='compile'`
-- was rejected by the mode CHECK, NACKing the compile.requested handler in a retry
-- loop and stranding the release in status `compiling` (no compile Job, no
-- validation.requested). This migration closes that gap.

-- Allow mode='compile' on executor_deployments.
ALTER TABLE executor_deployments
    DROP CONSTRAINT IF EXISTS executor_deployments_mode_check,
    ADD  CONSTRAINT executor_deployments_mode_check
        CHECK (mode IN ('production','validation','seed_build','compile'));

-- Widen the candidate partial indexes to cover compile rows too, so the
-- per-release pending-count + (release,node,mode) uniqueness apply to the compile
-- leg exactly as they do for validation/seed_build. (The candidate identity
-- check from V18 — mode='production' OR (release_id AND node_id NOT NULL) —
-- already holds for compile rows, which carry full identity.)
DROP INDEX IF EXISTS idx_executor_deployments_candidate_release;
CREATE INDEX idx_executor_deployments_candidate_release
    ON executor_deployments (release_id, mode)
    WHERE mode IN ('validation','seed_build','compile');

DROP INDEX IF EXISTS uq_executor_deployments_candidate_release_node_mode;
CREATE UNIQUE INDEX uq_executor_deployments_candidate_release_node_mode
    ON executor_deployments (release_id, node_id, mode)
    WHERE mode IN ('validation','seed_build','compile');

-- The aggregate-emit sentinel is keyed on (release_id, mode); allow the compile
-- leg to claim its own emission independently of validation/seed_build.
ALTER TABLE validation_aggregates
    DROP CONSTRAINT IF EXISTS validation_aggregates_mode_check,
    ADD  CONSTRAINT validation_aggregates_mode_check
        CHECK (mode IN ('validation','seed_build','compile'));
