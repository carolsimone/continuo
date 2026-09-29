-- Allow 'full_refresh' as a run and task operation.
--
-- A full refresh rebuilds one dbt model or seed from scratch (dbt
-- --full-refresh). 'full_refresh' is 12 characters, so the column holds up to
-- 16. Operation stays a closed, non-empty domain; the empty-string domain
-- default (model.OperationRun) is written as 'run'.

ALTER TABLE scheduler_tracker ALTER COLUMN operation TYPE varchar(16);
ALTER TABLE scheduler_tracker DROP CONSTRAINT IF EXISTS scheduler_tracker_operation_check;
ALTER TABLE scheduler_tracker
    ADD CONSTRAINT scheduler_tracker_operation_check
    CHECK (operation IN ('run','test','build','full_refresh'));

ALTER TABLE task_tracker ALTER COLUMN operation TYPE varchar(16);
ALTER TABLE task_tracker DROP CONSTRAINT IF EXISTS task_tracker_operation_check;
ALTER TABLE task_tracker
    ADD CONSTRAINT task_tracker_operation_check
    CHECK (operation IN ('run','test','build','full_refresh'));

COMMENT ON COLUMN scheduler_tracker.operation IS
    'dbt verb the run applies to its nodes: run (model) | test | build | full_refresh. Stamped at activation; immutable.';
COMMENT ON COLUMN task_tracker.operation IS
    'dbt verb this task ran, denormalized from its run at dispatch: run (model) | test | build | full_refresh.';
