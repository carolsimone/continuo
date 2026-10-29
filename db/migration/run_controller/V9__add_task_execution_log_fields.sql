-- db/migration/state/V9__add_task_execution_log_fields.sql
ALTER TABLE task_execution ADD COLUMN log_s3_key VARCHAR(500);
