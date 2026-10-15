-- task_tracker has no index on status alone.
--
-- Every query that filters task_tracker by status also names the run
-- (schedule_id) or the task (task_id), which uq_task_tracker_run_node,
-- idx_task_tracker_schedule_id and the primary key serve. Pending tasks are a
-- small share of the table, so its statistics make an index on status look
-- selective for status = 'pending', and the planner would read every pending
-- task of every run where one task of one run is wanted.

DROP INDEX IF EXISTS idx_task_tracker_status;
