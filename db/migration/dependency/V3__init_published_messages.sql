-- Published Messages Table
-- Deduplication table for outbox publishing to prevent duplicate Redis publishes

CREATE TABLE IF NOT EXISTS published_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    outbox_entry_id UUID NOT NULL UNIQUE REFERENCES outbox(id),
    redis_message_id VARCHAR(255),
    published_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_published_messages_outbox_entry_id
    ON published_messages(outbox_entry_id);

COMMENT ON TABLE published_messages IS 'Tracks successfully published outbox entries to prevent duplicates';
COMMENT ON COLUMN published_messages.outbox_entry_id IS 'References outbox.id - ensures each entry published at most once';
COMMENT ON COLUMN published_messages.redis_message_id IS 'Message ID returned by Redis XADD';
