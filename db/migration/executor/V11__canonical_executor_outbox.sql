-- Replaces the typed deployment_outbox table with the canonical executor_outbox shape.
-- The old table stored deployment-specific typed columns (task_id, schedule_id,
-- service_name, etc.). The canonical shape stores a generic JSONB payload so that
-- the executor outbox is structurally identical to the other service outboxes and
-- can be consumed by the shared outbox relay.
--
-- Data loss is intentional: this is a pre-production migration; in-flight rows
-- in deployment_outbox are discarded by design.

DROP TABLE IF EXISTS deployment_outbox CASCADE;

CREATE TABLE executor_outbox (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_processing_id  UUID NULL REFERENCES message_processing(id),
    aggregate_type         TEXT NOT NULL,
    aggregate_id           UUID NOT NULL,
    event_type             TEXT NOT NULL,
    payload                JSONB NOT NULL,
    stream_name            TEXT NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'pending',
    retry_count            INT  NOT NULL DEFAULT 0,
    max_retries            INT  NOT NULL DEFAULT 3,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at           TIMESTAMPTZ NULL,
    error_message          TEXT NULL,
    CONSTRAINT executor_outbox_status_check
        CHECK (status IN ('pending', 'processed', 'failed'))
);

CREATE INDEX idx_executor_outbox_pending
    ON executor_outbox (created_at)
    WHERE status = 'pending';

CREATE INDEX idx_executor_outbox_aggregate
    ON executor_outbox (aggregate_type, aggregate_id);
