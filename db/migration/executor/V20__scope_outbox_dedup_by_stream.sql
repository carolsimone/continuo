-- Scope the outbox_entry_id dedup index by stream_name (consumer group).
--
-- The message_processing table dedups inbound messages on its (message_id,
-- stream_name) PRIMARY KEY, where stream_name identifies the consuming group.
-- A secondary unique index on outbox_entry_id catches the case where a
-- producer's outbox republishes the same row under a fresh Redis message id.
--
-- A GLOBAL unique index on outbox_entry_id alone assumes each message is read
-- by at most one consumer group. If a service ever runs two consumer groups
-- over a stream it already consumes, both groups read the SAME source message
-- (one outbox_entry_id): the group that inserts its message_processing row
-- first "claims" the outbox_entry_id, and the other group's InsertIfNotExists
-- (ON CONFLICT DO NOTHING) silently treats the message as already-processed and
-- ACKs WITHOUT doing its work.
--
-- Scope uniqueness by (outbox_entry_id, stream_name) — matching the (message_id,
-- stream_name) PRIMARY KEY's per-consumer-group scoping. This preserves dedup of
-- outbox-entry redeliveries WITHIN a group (same stream_name) while letting
-- distinct groups each process the shared message exactly once. This is a
-- uniform, defensive alignment across services; the ON CONFLICT DO NOTHING path
-- honors the re-scoped index transparently.

DROP INDEX IF EXISTS idx_message_processing_outbox_entry_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_message_processing_outbox_entry_id_stream
    ON message_processing (outbox_entry_id, stream_name)
    WHERE outbox_entry_id IS NOT NULL;
