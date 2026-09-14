-- Canonical transactional outbox (pkg/outbox). One row per event to publish;
-- the processor XADDs it and marks it processed, schedules a retry with
-- next_attempt_at, or marks it failed and writes a dead-letter row.
CREATE TABLE execution_outbox (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_processing_id UUID NULL REFERENCES message_processing(id),
    aggregate_type        TEXT NOT NULL,
    aggregate_id          UUID NOT NULL,
    event_type            TEXT NOT NULL,
    payload               JSONB NOT NULL,
    stream_name           TEXT NOT NULL,
    status                TEXT NOT NULL DEFAULT 'pending',
    retry_count           INT  NOT NULL DEFAULT 0,
    max_retries           INT  NOT NULL DEFAULT 13,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at          TIMESTAMPTZ NULL,
    error_message         TEXT NULL,
    next_attempt_at       TIMESTAMPTZ NULL,
    CONSTRAINT execution_outbox_status_check
        CHECK (status IN ('pending', 'scheduled', 'processed', 'failed'))
);
CREATE INDEX idx_execution_outbox_pending   ON execution_outbox (created_at) WHERE status = 'pending';
CREATE INDEX idx_execution_outbox_aggregate ON execution_outbox (aggregate_type, aggregate_id);
CREATE INDEX idx_execution_outbox_due       ON execution_outbox (next_attempt_at) WHERE status = 'scheduled';
