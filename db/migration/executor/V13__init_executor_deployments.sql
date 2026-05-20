-- executor_deployments is the K8s-deploy command queue. The query.model:v1 and
-- retry.task:v1 handlers write a 'pending' row here inside their Unit-of-Work
-- transaction (a pure Postgres write, no Kubernetes I/O). The deployer.Dispatcher
-- drains due rows, creates the K8s Job, and on success writes the canonical
-- announcement rows to executor_outbox. This keeps the K8s side effect off the
-- transactional outbox so every outbox Publisher is a uniform marshal-and-XADD.
CREATE TABLE executor_deployments (
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
    CONSTRAINT executor_deployments_status_check
        CHECK (status IN ('pending','deployed','failed'))
);

-- Index drives the dispatcher's due-row query:
-- WHERE status='pending' AND next_attempt_at <= NOW() ORDER BY next_attempt_at.
CREATE INDEX idx_executor_deployments_due
    ON executor_deployments (next_attempt_at) WHERE status = 'pending';

COMMENT ON TABLE executor_deployments IS
    'K8s-deploy command queue drained by deployer.Dispatcher (executor-controller).';
