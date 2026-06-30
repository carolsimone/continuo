-- V18 extends the candidate-release dispatch path with the seed-build leg, a
-- sequential phase that runs BEFORE validation for the same release_id: new or
-- changed dbt-seed nodes are built (team image, `dbt seed`) into the candidate
-- schema so it is populated when the validation leg runs --empty checks against
-- it. seed-build rows reuse the validation columns (release_id, node_id, outcome,
-- dbt_log_uri, run_results_uri, outcome_at) and the ValidationDeployTask shape.

-- Allow mode='seed_build' on executor_deployments.
ALTER TABLE executor_deployments
    DROP CONSTRAINT IF EXISTS executor_deployments_mode_check,
    ADD  CONSTRAINT executor_deployments_mode_check
        CHECK (mode IN ('production','validation','seed_build'));

-- The validation-identity check (release_id + node_id NOT NULL) must hold for
-- seed-build rows too — both legs carry full (release, node) identity.
ALTER TABLE executor_deployments
    DROP CONSTRAINT IF EXISTS executor_deployments_validation_identity_check,
    ADD  CONSTRAINT executor_deployments_candidate_identity_check
        CHECK (mode = 'production' OR (release_id IS NOT NULL AND node_id IS NOT NULL));

-- The per-release pending-count + (release,node) uniqueness queries are now run
-- for both candidate legs (validation and seed_build), so widen the partial
-- predicates from mode='validation' to "any non-production candidate row".
DROP INDEX IF EXISTS idx_executor_deployments_validation_release;
CREATE INDEX idx_executor_deployments_candidate_release
    ON executor_deployments (release_id, mode)
    WHERE mode IN ('validation','seed_build');

DROP INDEX IF EXISTS uq_executor_deployments_validation_release_node;
CREATE UNIQUE INDEX uq_executor_deployments_candidate_release_node_mode
    ON executor_deployments (release_id, node_id, mode)
    WHERE mode IN ('validation','seed_build');

-- The aggregate-emit sentinel is now keyed on (release_id, mode): the seed-build
-- and validation legs share one release_id but emit independently, so a single
-- release_id PRIMARY KEY would let only the first leg ever claim its emission.
-- Existing rows are validation-leg claims; backfill mode then re-key the PK.
ALTER TABLE validation_aggregates
    ADD COLUMN mode TEXT NOT NULL DEFAULT 'validation'
        CHECK (mode IN ('validation','seed_build'));

ALTER TABLE validation_aggregates DROP CONSTRAINT validation_aggregates_pkey;
ALTER TABLE validation_aggregates ADD PRIMARY KEY (release_id, mode);

COMMENT ON TABLE validation_aggregates IS
    'Per-(release, leg) sentinel preventing duplicate emission of a leg completion event (validation.completed:v1 / seed.build.completed:v1) when the last per-node results arrive concurrently. Keyed on (release_id, mode) so the sequential seed-build and validation legs of one release claim independently.';
