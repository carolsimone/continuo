-- Add outbox_entry_id-based dedup to message_processing.
--
-- pkg/outbox.Processor cannot make XADD + MarkProcessed atomic across Redis
-- and Postgres. If the processor crashes after XADD succeeds but before the
-- DB commit, the row stays pending and is re-published on the next tick with
-- a fresh Redis message_id. Consumer-side dedup keyed only on (message_id,
-- stream_name) treats the redelivery as a new message and runs the handler
-- twice.
--
-- The fix: producers include outbox_entry_id in every message they publish,
-- and consumers dedup on it alongside (message_id, stream_name). The same
-- outbox row published twice produces the same outbox_entry_id, so the
-- second delivery is rejected.

ALTER TABLE message_processing
    ADD COLUMN IF NOT EXISTS outbox_entry_id UUID NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_message_processing_outbox_entry_id
    ON message_processing (outbox_entry_id)
    WHERE outbox_entry_id IS NOT NULL;
