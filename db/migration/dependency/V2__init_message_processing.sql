-- Message Processing Table
-- Deduplication of consumed Redis messages for exactly-once processing

CREATE TABLE IF NOT EXISTS message_processing (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id VARCHAR(255) NOT NULL UNIQUE,
    stream_name VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL CHECK (state IN ('processing', 'completed', 'acked')),
    payload JSONB NOT NULL,
    error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_message_processing_message_id ON message_processing(message_id);
CREATE INDEX IF NOT EXISTS idx_message_processing_state ON message_processing(state);
CREATE INDEX IF NOT EXISTS idx_message_processing_created_at ON message_processing(created_at);

-- Add message_processing_id to outbox table for provenance tracking
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS message_processing_id UUID REFERENCES message_processing(id);
CREATE INDEX IF NOT EXISTS idx_outbox_message_processing_id ON outbox(message_processing_id);

COMMENT ON TABLE message_processing IS 'Tracks consumed Redis messages for exactly-once processing';
COMMENT ON COLUMN message_processing.message_id IS 'Redis Stream message ID (e.g., 1738756432123-0)';
COMMENT ON COLUMN message_processing.state IS 'Processing state: processing, completed, acked';
