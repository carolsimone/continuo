-- The run tables carry the names of what they hold: a run row is a run and a
-- task row is a task. runs replaces scheduler_tracker and tasks replaces
-- task_tracker; their key column is run_id, and the run's progress clock is
-- last_progress_at. Constraints and indexes keep their meaning and take the
-- new table name in place of the old one. Foreign keys, defaults, triggers and
-- the sequences of other tables follow a table rename by themselves.

ALTER TABLE scheduler_tracker RENAME TO runs;
ALTER TABLE task_tracker RENAME TO tasks;

ALTER TABLE runs  RENAME COLUMN schedule_id TO run_id;
ALTER TABLE runs  RENAME COLUMN last_heartbeat_at TO last_progress_at;
ALTER TABLE tasks RENAME COLUMN schedule_id TO run_id;

ALTER TABLE runs RENAME CONSTRAINT scheduler_tracker_pkey TO runs_pkey;
ALTER TABLE runs RENAME CONSTRAINT scheduler_tracker_failure_reason_check TO runs_failure_reason_check;
ALTER TABLE runs RENAME CONSTRAINT scheduler_tracker_initialization_status_check TO runs_initialization_status_check;
ALTER TABLE runs RENAME CONSTRAINT scheduler_tracker_kind_check TO runs_kind_check;
ALTER TABLE runs RENAME CONSTRAINT scheduler_tracker_operation_check TO runs_operation_check;
ALTER TABLE runs RENAME CONSTRAINT scheduler_tracker_status_check TO runs_status_check;

ALTER TABLE tasks RENAME CONSTRAINT task_tracker_pkey TO tasks_pkey;
ALTER TABLE tasks RENAME CONSTRAINT task_tracker_schedule_id_fkey TO tasks_run_id_fkey;
ALTER TABLE tasks RENAME CONSTRAINT task_tracker_operation_check TO tasks_operation_check;
ALTER TABLE tasks RENAME CONSTRAINT task_tracker_status_check TO tasks_status_check;
ALTER TABLE tasks RENAME CONSTRAINT task_tracker_wait_reason_check TO tasks_wait_reason_check;

ALTER INDEX idx_scheduler_tracker_finalization           RENAME TO idx_runs_finalization;
ALTER INDEX idx_scheduler_tracker_init_status            RENAME TO idx_runs_init_status;
ALTER INDEX idx_scheduler_tracker_kind                   RENAME TO idx_runs_kind;
ALTER INDEX idx_scheduler_tracker_release_id             RENAME TO idx_runs_release_id;
ALTER INDEX idx_scheduler_tracker_schedule_name_created  RENAME TO idx_runs_schedule_name_created;
ALTER INDEX idx_scheduler_tracker_source_run_id          RENAME TO idx_runs_source_run_id;
ALTER INDEX uq_scheduler_tracker_active_per_schedule     RENAME TO uq_runs_active_per_schedule;

ALTER INDEX idx_task_tracker_inherited_from RENAME TO idx_tasks_inherited_from;
ALTER INDEX idx_task_tracker_job_name       RENAME TO idx_tasks_job_name;
ALTER INDEX idx_task_tracker_lease_waiters  RENAME TO idx_tasks_lease_waiters;
ALTER INDEX idx_task_tracker_node_identity  RENAME TO idx_tasks_node_identity;
ALTER INDEX idx_task_tracker_schedule_id    RENAME TO idx_tasks_run_id;
ALTER INDEX uq_task_tracker_run_node        RENAME TO uq_tasks_run_node;

COMMENT ON TABLE runs IS 'One run of a schedule: its lifecycle from pending through running to a final status';
COMMENT ON COLUMN runs.run_id IS 'Unique identifier of the run';
COMMENT ON TABLE tasks IS 'One task of a run: the work for a single node, with its status and current attempt';
COMMENT ON COLUMN tasks.run_id IS 'The run the task belongs to; deleting the run deletes its tasks';
COMMENT ON COLUMN task_attempts.attempt IS '0-based attempt number; tasks.attempt names the current one';
