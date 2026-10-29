-- db/migration/state/V32__add_task_execution_run_results_uri.sql
ALTER TABLE task_execution ADD COLUMN run_results_uri VARCHAR(500);
