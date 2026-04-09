-- Processed Events Table
-- Deduplication table to prevent duplicate K8s job creation

CREATE TABLE IF NOT EXISTS processed_events (
    outbox_entry_id UUID PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_events(processed_at);

COMMENT ON TABLE processed_events IS 'Tracks processed outbox entries to prevent duplicate K8s job creation';
COMMENT ON COLUMN processed_events.outbox_entry_id IS 'References outbox entry ID from upstream service';
