-- The outbox relay claims due rows with
--   WHERE status IN ('pending', 'scheduled') ... ORDER BY created_at, id
-- and withholds a row while an older row of its aggregate is still open, so
-- both predicates get a partial index of their own. Dead-lettered rows
-- ('failed') are counted for metrics through a third. The superseded indexes
-- on 'pending' and 'scheduled' alone are dropped.
DROP INDEX IF EXISTS idx_execution_outbox_pending;
DROP INDEX IF EXISTS idx_execution_outbox_due;

CREATE INDEX IF NOT EXISTS idx_execution_outbox_claimable
    ON execution_outbox (created_at, id)
    WHERE status IN ('pending', 'scheduled');

CREATE INDEX IF NOT EXISTS idx_execution_outbox_open_by_aggregate
    ON execution_outbox (aggregate_type, aggregate_id, created_at)
    WHERE status IN ('pending', 'scheduled');

CREATE INDEX IF NOT EXISTS idx_execution_outbox_failed
    ON execution_outbox (created_at)
    WHERE status = 'failed';

-- Wakes the relay listening on the channel named after the table. Postgres
-- delivers the notification when the inserting transaction commits, once per
-- transaction however many rows it inserted.
CREATE OR REPLACE FUNCTION continuo_outbox_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(TG_TABLE_NAME, '');
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS execution_outbox_notify ON execution_outbox;
CREATE TRIGGER execution_outbox_notify
    AFTER INSERT ON execution_outbox
    FOR EACH STATEMENT
    EXECUTE FUNCTION continuo_outbox_notify();
