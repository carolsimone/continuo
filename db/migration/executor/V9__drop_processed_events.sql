-- The processed_events.outbox_entry_id dedup table is superseded by
-- message_processing (V8). The new binding deduplicates by
-- (message_id, stream_name) inside the same transaction that writes
-- to deployment_outbox, so processed_events is no longer read or
-- written by any code.

DROP INDEX IF EXISTS idx_processed_events_processed_at;
DROP TABLE IF EXISTS processed_events;
