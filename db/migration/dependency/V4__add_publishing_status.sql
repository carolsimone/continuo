-- Add 'publishing' as a valid outbox status
-- The outbox processor uses 'publishing' as an optimistic lock: it transitions
-- an entry from 'pending' → 'publishing' before publishing to Redis, so that
-- competing processor instances skip it.  The original constraint omitted this
-- intermediate state, causing every UpdateStatus call to fail.

ALTER TABLE outbox
    DROP CONSTRAINT outbox_status_check;

ALTER TABLE outbox
    ADD CONSTRAINT outbox_status_check
    CHECK (status IN ('pending', 'publishing', 'processed', 'failed'));
