-- The run-controller outbox is named after the service: run_controller_outbox,
-- with constraints, indexes and a notify trigger that carry the same prefix.
-- The rows stay as they are; only the names change.
--
-- continuo_outbox_notify() notifies the channel TG_TABLE_NAME, so the relay's
-- channel is now run_controller_outbox, the name run-controller's waker listens
-- on (postgres.OutboxTable).

ALTER TABLE state_outbox RENAME TO run_controller_outbox;

-- Constraints first: renaming the primary key also renames the index behind it,
-- so state_outbox_pkey needs no ALTER INDEX of its own.
ALTER TABLE run_controller_outbox
    RENAME CONSTRAINT state_outbox_pkey TO run_controller_outbox_pkey;
ALTER TABLE run_controller_outbox
    RENAME CONSTRAINT state_outbox_status_check TO run_controller_outbox_status_check;
ALTER TABLE run_controller_outbox
    RENAME CONSTRAINT state_outbox_message_processing_id_fkey TO run_controller_outbox_message_processing_id_fkey;

ALTER INDEX idx_state_outbox_aggregate RENAME TO idx_run_controller_outbox_aggregate;
ALTER INDEX idx_state_outbox_claimable RENAME TO idx_run_controller_outbox_claimable;
ALTER INDEX idx_state_outbox_failed RENAME TO idx_run_controller_outbox_failed;
ALTER INDEX idx_state_outbox_message_processing_id RENAME TO idx_run_controller_outbox_message_processing_id;
ALTER INDEX idx_state_outbox_open_by_aggregate RENAME TO idx_run_controller_outbox_open_by_aggregate;

ALTER TRIGGER state_outbox_notify ON run_controller_outbox RENAME TO run_controller_outbox_notify;
