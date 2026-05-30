-- V15 extends executor_deployments to carry the candidate-release dispatch path.
-- mode='production' rows are the existing query.model:v1 path (default; no
-- behavior change). mode='validation' rows carry the dbt --empty validation
-- run for one node of one candidate release. release_id and node_id are
-- nullable so production rows leave them NULL. outcome is the per-node
-- terminal status reported back by k8s-controller via
-- validation.node.completed:v1.

ALTER TABLE executor_deployments
    ADD COLUMN mode       TEXT NOT NULL DEFAULT 'production'
        CHECK (mode IN ('production','validation')),
    ADD COLUMN release_id TEXT NULL,
    ADD COLUMN node_id    TEXT NULL,
    ADD COLUMN outcome    TEXT NULL
        CHECK (outcome IN ('ok','failed') OR outcome IS NULL),
    ADD COLUMN dbt_log_uri    TEXT NULL,
    ADD COLUMN outcome_at     TIMESTAMPTZ NULL;

-- Drives the per-release pending-count query that gates the
-- aggregate-emit decision (executor-controller).
CREATE INDEX idx_executor_deployments_validation_release
    ON executor_deployments (release_id)
    WHERE mode = 'validation';

-- Sanity: validation rows must carry both identity columns.
ALTER TABLE executor_deployments
    ADD CONSTRAINT executor_deployments_validation_identity_check
        CHECK (mode <> 'validation' OR (release_id IS NOT NULL AND node_id IS NOT NULL));

CREATE TABLE validation_aggregates (
    release_id              TEXT PRIMARY KEY,
    aggregate_emitted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE validation_aggregates IS
    'Per-release sentinel preventing duplicate emission of validation.completed:v1 when the last two per-node results arrive concurrently.';

-- Belt-and-braces: a candidate release validates each node at most once, so
-- (release_id, node_id) is unique among validation rows. Production rows leave
-- both columns NULL and are excluded by the partial predicate.
CREATE UNIQUE INDEX uq_executor_deployments_validation_release_node
    ON executor_deployments (release_id, node_id)
    WHERE mode = 'validation';
