-- Deploy command queue drained by the dispatcher. A production row (mode =
-- 'production') is one task dispatch; a candidate row (validation, seed_build,
-- compile) is one node of a release leg and records that node's terminal
-- outcome. Candidate rows start pending or blocked (blocked until every in-set
-- upstream is ok) and may be skipped when an upstream fails.
CREATE TABLE deployments (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_processing_id UUID NULL REFERENCES message_processing(id),
    task_id               UUID NOT NULL,
    schedule_id           UUID NOT NULL,
    job_params            JSONB NOT NULL,
    status                TEXT NOT NULL DEFAULT 'pending',
    retry_count           INT  NOT NULL DEFAULT 0,
    max_retries           INT  NOT NULL DEFAULT 3,
    next_attempt_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deployed_at           TIMESTAMPTZ NULL,
    error_message         TEXT NULL,
    mode                  TEXT NOT NULL DEFAULT 'production',
    release_id            TEXT NULL,
    node_id               TEXT NULL,
    outcome               TEXT NULL,
    dbt_log_uri           TEXT NULL,
    outcome_at            TIMESTAMPTZ NULL,
    run_results_uri       TEXT NULL,
    failed_container      TEXT NULL,
    CONSTRAINT deployments_status_check
        CHECK (status IN ('pending','blocked','deployed','failed','skipped')),
    CONSTRAINT deployments_mode_check
        CHECK (mode IN ('production','validation','seed_build','compile')),
    CONSTRAINT deployments_outcome_check
        CHECK (outcome IN ('ok','failed','skipped') OR outcome IS NULL),
    CONSTRAINT deployments_candidate_identity_check
        CHECK (mode = 'production' OR (release_id IS NOT NULL AND node_id IS NOT NULL))
);
CREATE INDEX idx_deployments_due ON deployments (next_attempt_at) WHERE status = 'pending';
CREATE INDEX idx_deployments_candidate_release
    ON deployments (release_id, mode)
    WHERE mode IN ('validation','seed_build','compile');
CREATE UNIQUE INDEX uq_deployments_candidate_release_node_mode
    ON deployments (release_id, node_id, mode)
    WHERE mode IN ('validation','seed_build','compile');
