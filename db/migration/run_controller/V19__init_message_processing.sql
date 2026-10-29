-- Per-service message_processing table for state. Mirrors orchestrator's
-- V2 column contract so pkg/messageprocessing's Postgres impl works
-- against this database too. Uses TIMESTAMPTZ to match state's convention;
-- the Go layer reads both TIMESTAMP and TIMESTAMPTZ as time.Time.

CREATE TABLE IF NOT EXISTS message_processing (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id  VARCHAR(255) NOT NULL UNIQUE,
    stream_name VARCHAR(100) NOT NULL,
    state       VARCHAR(50)  NOT NULL CHECK (state IN ('processing', 'completed', 'acked')),
    payload     JSONB        NOT NULL,
    error       TEXT,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_message_processing_message_id ON message_processing(message_id);
CREATE INDEX IF NOT EXISTS idx_message_processing_state      ON message_processing(state);
CREATE INDEX IF NOT EXISTS idx_message_processing_created_at ON message_processing(created_at);

ALTER TABLE state_outbox ADD COLUMN IF NOT EXISTS message_processing_id UUID REFERENCES message_processing(id);
CREATE INDEX IF NOT EXISTS idx_state_outbox_message_processing_id ON state_outbox(message_processing_id);

COMMENT ON TABLE message_processing IS 'Tracks consumed Redis messages for exactly-once processing (state service).';
