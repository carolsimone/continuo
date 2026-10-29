-- A run whose tasks state chose records when it chose them, and each of its
-- tasks records when its dispatch was written.
--
-- scheduler_tracker.planned_at is set in the transaction that chose the run's
-- tasks from the run's release and inserted them. It is NULL on a run whose
-- tasks state did not choose.
--
-- task_tracker.dispatched_at is set in the transaction that wrote the task's
-- query.model:v1 dispatch to state_outbox, so a task is dispatched at most
-- once. It is NULL on a task not yet dispatched and on a task inherited from a
-- source run, which is never dispatched.
--
-- idx_topology_nodes_schedule serves the read of one schedule's nodes in a
-- release.

ALTER TABLE scheduler_tracker ADD COLUMN planned_at timestamptz NULL;

ALTER TABLE task_tracker ADD COLUMN dispatched_at timestamptz NULL;

CREATE INDEX idx_topology_nodes_schedule
    ON topology_nodes (tenant_id, release_id, schedule_name);
