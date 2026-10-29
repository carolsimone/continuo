-- Every run names the release it runs on, and every task names its node in
-- that release.
--
-- scheduler_tracker.promotion_seq is the promotion seq the run's release was
-- live under when the run was created, or the source run's for a run that
-- replays another run's release. It is NULL on runs that name no release.
--
-- task_tracker.node_key is the lowercased "<schema>.<table>" of the task's
-- node, the key of topology_nodes. It is NULL on tasks of runs that name no
-- release. A run holds at most one task per node.

ALTER TABLE scheduler_tracker ADD COLUMN promotion_seq bigint NULL;

ALTER TABLE task_tracker ADD COLUMN node_key text NULL;

CREATE UNIQUE INDEX uq_task_tracker_run_node
    ON task_tracker (schedule_id, node_key)
    WHERE node_key IS NOT NULL;
