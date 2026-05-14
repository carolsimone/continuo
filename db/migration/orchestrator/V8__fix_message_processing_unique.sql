-- Defensive parity with state's V21: include stream_name in the uniqueness
-- key for message_processing so the same message_id from different streams
-- cannot collide. Orchestrator's consumed streams don't currently overlap
-- across publishers, but the invariant should hold service-by-service.

ALTER TABLE message_processing
    DROP CONSTRAINT IF EXISTS message_processing_message_id_key;

ALTER TABLE message_processing
    ADD CONSTRAINT message_processing_message_id_stream_name_key
        UNIQUE (message_id, stream_name);

-- The outbox FK is a provenance link only; clearing it on existing rows
-- preserves them as-is so any pending publish work continues without disruption.
UPDATE outbox SET message_processing_id = NULL WHERE message_processing_id IS NOT NULL;
DELETE FROM message_processing;
