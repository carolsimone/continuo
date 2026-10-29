-- db/migration/state/V5__init_processed_events.sql
-- Deduplication table for the state service's Redis consumers.
-- Uses TIMESTAMPTZ (consistent with all other state service tables).
-- Primary key is named event_id — the state service receives event IDs directly
-- from Redis message payloads; there is no outbox write in this flow.

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_events (processed_at);
