-- The agent-remediation outbox is named after the service: agent_remediation_outbox,
-- with constraints, indexes and a notify trigger that carry the same prefix, and
-- its rows carry the outbox aggregate type agent_remediation. The rows stay in
-- place; only the names and the aggregate type change.
--
-- continuo_outbox_notify() notifies the channel TG_TABLE_NAME, so the relay's
-- channel is now agent_remediation_outbox, the name agent-remediation's waker
-- listens on (postgres.OutboxTable).

ALTER TABLE remediation_agent_outbox RENAME TO agent_remediation_outbox;

-- Constraints first: renaming the primary key also renames the index behind it,
-- so remediation_agent_outbox_pkey needs no ALTER INDEX of its own.
ALTER TABLE agent_remediation_outbox
    RENAME CONSTRAINT remediation_agent_outbox_pkey TO agent_remediation_outbox_pkey;
ALTER TABLE agent_remediation_outbox
    RENAME CONSTRAINT remediation_agent_outbox_status_check TO agent_remediation_outbox_status_check;
ALTER TABLE agent_remediation_outbox
    RENAME CONSTRAINT remediation_agent_outbox_message_processing_id_fkey TO agent_remediation_outbox_message_processing_id_fkey;

-- Postgres 18 stores each NOT NULL column constraint in pg_constraint under
-- <table>_<column>_not_null and keeps that name when the table is renamed. Every
-- constraint of the table that still carries the previous prefix is renamed
-- here; Postgres 16 keeps no such constraint, so the loop finds nothing there.
DO $$
DECLARE
    old_prefix CONSTANT text := 'remediation_agent_outbox_';
    new_prefix CONSTANT text := 'agent_remediation_outbox_';
    c record;
BEGIN
    FOR c IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'agent_remediation_outbox'::regclass
          AND starts_with(conname, old_prefix)
    LOOP
        EXECUTE format('ALTER TABLE agent_remediation_outbox RENAME CONSTRAINT %I TO %I',
                       c.conname, new_prefix || substr(c.conname, length(old_prefix) + 1));
    END LOOP;
END
$$;

ALTER INDEX idx_remediation_agent_outbox_claimable RENAME TO idx_agent_remediation_outbox_claimable;
ALTER INDEX idx_remediation_agent_outbox_failed RENAME TO idx_agent_remediation_outbox_failed;
ALTER INDEX idx_remediation_agent_outbox_open_by_aggregate RENAME TO idx_agent_remediation_outbox_open_by_aggregate;

ALTER TRIGGER remediation_agent_outbox_notify ON agent_remediation_outbox RENAME TO agent_remediation_outbox_notify;

-- The relay withholds a row while an older open row shares its aggregate type
-- and id, so a row left under the previous aggregate type would form a lane of
-- its own beside the rows written under the new one.
UPDATE agent_remediation_outbox
SET aggregate_type = 'agent_remediation'
WHERE aggregate_type = 'remediation_agent';
