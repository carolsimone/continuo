-- Legacy outbox-entry-id dedup is superseded by per-stream message_processing
-- (consumer-side dedup keyed on (message_id, stream_name)).
DROP TABLE IF EXISTS processed_events CASCADE;

-- Drop the legacy typed outbox; replaced by the canonical k8s_outbox below.
-- Data loss is tolerated: this is a pre-prod migration.
DROP TABLE IF EXISTS k8s_status_outbox CASCADE;

-- Per-service message_processing table for k8s-controller. Mirrors the contract
-- used by state, orchestrator, and executor so pkg/messageprocessing's Postgres
-- implementation works against this database too.
-- The composite UNIQUE (message_id, stream_name) is required: Redis Streams assign
-- message IDs per-stream, and a publisher can emit two messages to two streams in
-- the same millisecond with identical IDs.
CREATE TABLE IF NOT EXISTS message_processing (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id  VARCHAR(255) NOT NULL,
    stream_name VARCHAR(100) NOT NULL,
    state       VARCHAR(50)  NOT NULL CHECK (state IN ('processing', 'completed', 'acked')),
    payload     JSONB        NOT NULL,
    error       TEXT,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT message_processing_message_id_stream_name_key UNIQUE (message_id, stream_name)
);

CREATE INDEX IF NOT EXISTS idx_message_processing_message_id ON message_processing(message_id);
CREATE INDEX IF NOT EXISTS idx_message_processing_state      ON message_processing(state);
CREATE INDEX IF NOT EXISTS idx_message_processing_created_at ON message_processing(created_at);

-- Canonical outbox table for k8s-controller. Domain-agnostic shape: all
-- k8s-specific fields (task_retry_count, node_type, image_tag, etc.) are
-- encoded in the JSONB payload column.
CREATE TABLE k8s_outbox (
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
    CONSTRAINT k8s_outbox_status_check
        CHECK (status IN ('pending', 'processed', 'failed'))
);

CREATE INDEX idx_k8s_outbox_pending
    ON k8s_outbox (created_at)
    WHERE status = 'pending';

CREATE INDEX idx_k8s_outbox_aggregate
    ON k8s_outbox (aggregate_type, aggregate_id);
