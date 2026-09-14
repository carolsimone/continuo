-- Inbound dedup: one row per consumed Redis message, keyed on (message_id, stream_name).
-- outbox_entry_id is the producer's outbox row id, carried on every message so a
-- row republished with a fresh Redis id still deduplicates.
CREATE TABLE message_processing (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id      VARCHAR(255) NOT NULL,
    stream_name     VARCHAR(100) NOT NULL,
    state           VARCHAR(50)  NOT NULL,
    payload         JSONB        NOT NULL,
    error           TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    outbox_entry_id UUID,
    CONSTRAINT message_processing_state_check
        CHECK (state IN ('processing', 'completed', 'acked')),
    CONSTRAINT message_processing_message_id_stream_name_key UNIQUE (message_id, stream_name)
);
CREATE INDEX idx_message_processing_message_id ON message_processing (message_id);
CREATE INDEX idx_message_processing_state      ON message_processing (state);
CREATE INDEX idx_message_processing_created_at ON message_processing (created_at);
CREATE UNIQUE INDEX idx_message_processing_outbox_entry_id_stream
    ON message_processing (outbox_entry_id, stream_name)
    WHERE outbox_entry_id IS NOT NULL;
COMMENT ON TABLE message_processing IS 'Tracks consumed Redis messages for exactly-once processing.';
