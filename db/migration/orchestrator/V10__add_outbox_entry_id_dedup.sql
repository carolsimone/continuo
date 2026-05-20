-- Add outbox_entry_id-based dedup to message_processing. See
-- db/migration/state/V23 for the full rationale; this migration adds the
-- equivalent column + partial unique index to orchestrator's per-service
-- message_processing table.

ALTER TABLE message_processing
    ADD COLUMN IF NOT EXISTS outbox_entry_id UUID NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_message_processing_outbox_entry_id
    ON message_processing (outbox_entry_id)
    WHERE outbox_entry_id IS NOT NULL;
