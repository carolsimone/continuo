-- V28__add_operation_to_run_and_task.sql
-- Records the dbt verb each run/task applies to its node: run (model), test, or
-- build. The Nodes catalog groups a node's history by this operation so model
-- health and test health read separately instead of blended.
--
-- Encoded as a non-empty 3-value domain; the empty-string domain default
-- (model.OperationRun) is written as 'run'. Pre-V28 rows predate operation
-- capture and default to 'run' (model); genuine test/build rows written before
-- this migration are unrecoverable and read as 'run'.

ALTER TABLE scheduler_tracker
    ADD COLUMN operation varchar(10) NOT NULL DEFAULT 'run';
ALTER TABLE scheduler_tracker
    ADD CONSTRAINT scheduler_tracker_operation_check
    CHECK (operation IN ('run','test','build'));

ALTER TABLE task_tracker
    ADD COLUMN operation varchar(10) NOT NULL DEFAULT 'run';
ALTER TABLE task_tracker
    ADD CONSTRAINT task_tracker_operation_check
    CHECK (operation IN ('run','test','build'));

COMMENT ON COLUMN scheduler_tracker.operation IS
    'dbt verb the run applies to its nodes: run (model) | test | build. Stamped at activation; immutable.';
COMMENT ON COLUMN task_tracker.operation IS
    'dbt verb this task ran, denormalized from its run at dispatch: run (model) | test | build.';
