-- Per-service message_processing table for executor-controller. Mirrors
-- state and orchestrator's column contract so pkg/messageprocessing's
-- Postgres impl works against this database too. TIMESTAMPTZ matches the
-- convention used elsewhere; the Go layer reads both TIMESTAMP and
-- TIMESTAMPTZ as time.Time.
--
-- The composite UNIQUE (message_id, stream_name) is required: Redis
-- Streams assign message IDs per-stream, and a single outbox publisher
-- can emit two messages to two streams in the same millisecond and
-- produce identical message IDs.

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

COMMENT ON TABLE message_processing IS 'Tracks consumed Redis messages for exactly-once processing (executor-controller).';
