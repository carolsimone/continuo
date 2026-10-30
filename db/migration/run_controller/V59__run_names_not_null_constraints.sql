-- The NOT NULL constraints of runs and tasks take the names of the tables and
-- columns they belong to.
--
-- Postgres 18 stores each NOT NULL column constraint in pg_constraint under
-- <table>_<column>_not_null and keeps that name when the table or the column is
-- renamed, so the constraints of scheduler_tracker and task_tracker are still
-- named scheduler_tracker_<column>_not_null and task_tracker_<column>_not_null
-- (for example scheduler_tracker_schedule_id_not_null), and the one of the
-- column that was retry_count is named for it. Every constraint of runs and
-- tasks whose name still carries one of the retired table or column names is
-- renamed here to the name it would have had if the table had been created with
-- the current names. Postgres 16 keeps no such constraint, so the loop finds
-- nothing there.
DO $$
DECLARE
    c record;
BEGIN
    FOR c IN
        SELECT r.relname AS tbl,
               k.conname,
               replace(replace(replace(replace(replace(k.conname,
                   'scheduler_tracker', 'runs'),
                   'task_tracker', 'tasks'),
                   'schedule_id', 'run_id'),
                   'last_heartbeat_at', 'last_progress_at'),
                   'retry_count', 'attempt') AS new_name
        FROM pg_constraint k
        JOIN pg_class r ON r.oid = k.conrelid
        WHERE k.conrelid IN ('runs'::regclass, 'tasks'::regclass)
          AND k.conname ~ 'scheduler_tracker|task_tracker|schedule_id|last_heartbeat_at|retry_count'
    LOOP
        EXECUTE format('ALTER TABLE %I RENAME CONSTRAINT %I TO %I', c.tbl, c.conname, c.new_name);
    END LOOP;
END
$$;
