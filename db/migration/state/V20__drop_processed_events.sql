-- The processed_events dedup table is superseded by message_processing
-- (V19). State's handlers now use pkg/messageprocessing.Dedup which
-- operates against message_processing.

DROP INDEX IF EXISTS idx_processed_events_processed_at;
DROP TABLE IF EXISTS processed_events;
