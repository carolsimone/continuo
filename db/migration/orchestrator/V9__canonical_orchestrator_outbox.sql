-- published_messages: dropped. Publisher-side dedup is no longer needed;
-- consumer-side dedup in pkg/messageprocessing covers the same window.
DROP TABLE IF EXISTS published_messages CASCADE;

-- Rewrite outbox → orchestrator_outbox. The existing outbox table is already
-- canonical-shape (columns accumulated across V1, V2, V4); this migration
-- drops it and reasserts the canonical shape under the new name.
-- Data loss is tolerated: this is a pre-prod migration.
DROP TABLE IF EXISTS outbox CASCADE;

CREATE TABLE orchestrator_outbox (
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
    CONSTRAINT orchestrator_outbox_status_check
        CHECK (status IN ('pending', 'processed', 'failed'))
);

CREATE INDEX idx_orchestrator_outbox_pending
    ON orchestrator_outbox (created_at)
    WHERE status = 'pending';

CREATE INDEX idx_orchestrator_outbox_aggregate
    ON orchestrator_outbox (aggregate_type, aggregate_id);
