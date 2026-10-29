-- The dedup invariant is per-stream: the same Redis message_id can appear
-- on multiple streams (Redis Streams assign IDs per-stream; co-published
-- messages from a single outbox processor commonly share the millisecond
-- prefix and a seq of 0). Disambiguate by including stream_name in the
-- uniqueness key. The old per-message constraint caused state's
-- task.status.updated:v1 and task.execution.recorded:v1 consumers to
-- false-dedup each other when k8s-controller's outbox-processor
-- published both in the same millisecond.

ALTER TABLE message_processing
    DROP CONSTRAINT IF EXISTS message_processing_message_id_key;

ALTER TABLE message_processing
    ADD CONSTRAINT message_processing_message_id_stream_name_key
        UNIQUE (message_id, stream_name);

-- Stale data from the bug window. Each row represents either a successful
-- dedup-and-process (no need to keep) or a false-dedup-skip (the original
-- message has already been ACKed, replaying won't help). Wiping ensures
-- subsequent runs start from a clean slate. The outbox FK is a provenance
-- link only; clearing it on existing rows preserves them as-is so any
-- pending publish work continues without disruption.
UPDATE state_outbox SET message_processing_id = NULL WHERE message_processing_id IS NOT NULL;
DELETE FROM message_processing;
