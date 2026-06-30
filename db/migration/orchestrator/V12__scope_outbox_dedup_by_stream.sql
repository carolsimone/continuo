-- Scope the outbox_entry_id dedup index by stream_name (consumer group).
--
-- The orchestrator runs TWO consumer groups on release.promoted:v1: the
-- topology-swap consumer (group `orchestrator-release-promoted`, dedup
-- stream_name `release.promoted:v1`) and the prod seed-build consumer (group
-- `orchestrator-release-promoted-seed-build`). Both read the SAME source
-- message, which carries ONE outbox_entry_id.
--
-- The original GLOBAL unique index on outbox_entry_id let whichever group
-- inserted its message_processing row first "claim" that outbox_entry_id; the
-- other group's InsertIfNotExists then hit the conflict (ON CONFLICT DO NOTHING)
-- and the handler treated it as already-processed — ACKing WITHOUT doing its
-- work. Racily: when the seed-build group won, the topology-swap handler skipped
-- and the Neo4j :Meta singleton was never updated, so the release never
-- "swapped" (e2e TestE2E_ReleasePromote_* topology-swap timeouts).
--
-- Fix: scope uniqueness by (outbox_entry_id, stream_name) — matching the
-- (message_id, stream_name) PRIMARY KEY's per-consumer-group scoping. This still
-- dedups redeliveries of the same outbox entry WITHIN a group (same stream_name)
-- while letting distinct groups each process the shared message exactly once.
-- InsertIfNotExists uses ON CONFLICT DO NOTHING (no target), so it transparently
-- honors the re-scoped index.

DROP INDEX IF EXISTS idx_message_processing_outbox_entry_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_message_processing_outbox_entry_id_stream
    ON message_processing (outbox_entry_id, stream_name)
    WHERE outbox_entry_id IS NOT NULL;
