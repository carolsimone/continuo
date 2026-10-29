-- Bring state_outbox to the canonical 12-column outbox shape shared across
-- all services. V7 created the table; V19 added message_processing_id.
-- This migration adds the two remaining columns and the standard indexes.

ALTER TABLE state_outbox
    ADD COLUMN IF NOT EXISTS processed_at  TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS error_message TEXT        NULL;

-- Normalize legacy 'published' status to 'processed' so the canonical
-- CHECK constraint can be applied. Rows with status='published' were
-- successfully dispatched to Redis; 'processed' is the equivalent canonical value.
UPDATE state_outbox SET status = 'processed' WHERE status = 'published';

ALTER TABLE state_outbox
    DROP CONSTRAINT IF EXISTS state_outbox_status_check;
ALTER TABLE state_outbox
    ADD CONSTRAINT state_outbox_status_check
        CHECK (status IN ('pending', 'processed', 'failed'));

CREATE INDEX IF NOT EXISTS idx_state_outbox_pending
    ON state_outbox (created_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_state_outbox_aggregate
    ON state_outbox (aggregate_type, aggregate_id);
