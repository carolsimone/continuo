-- A node has at most one writer at a time. A task takes the write lease on its
-- node before state dispatches an attempt of it and holds the lease until the
-- task ends; a ready task whose node is leased waits for it.
--
-- task_tracker.wait_reason is what a pending task waits for beyond its upstream
-- tasks: node_lease while another task holds the lease on its node, NULL
-- otherwise. lease_wait_since is when it began to wait, which orders the
-- waiters of a node oldest first.
CREATE TABLE node_leases (
    tenant_id   text        NOT NULL,
    node_key    text        NOT NULL,
    task_id     uuid        NOT NULL UNIQUE REFERENCES task_tracker (task_id) ON DELETE CASCADE,
    run_id      uuid        NOT NULL,
    acquired_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, node_key)
);
COMMENT ON TABLE node_leases IS
    'The write lease on a node: at most one row per node, and one per task. Deleting the row (or the task) releases the node.';
COMMENT ON COLUMN node_leases.tenant_id IS 'Tenant of the node';
COMMENT ON COLUMN node_leases.node_key IS 'Lowercased "<schema>.<table>" of the leased node, the key of topology_nodes';
COMMENT ON COLUMN node_leases.task_id IS 'The task that holds the lease; a task holds at most one, and the lease ends with the task';
COMMENT ON COLUMN node_leases.run_id IS 'The run of the task that holds the lease, shown to the tasks that wait for it';
COMMENT ON COLUMN node_leases.acquired_at IS 'When the task took the lease';

-- Releasing leases wakes the tasks that wait for them. The trigger is
-- statement-level, so a statement that releases many leases sends one
-- notification, delivered when its transaction commits.
CREATE FUNCTION notify_node_leases_released() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('node_leases', '');
    RETURN NULL;
END;
$$;
COMMENT ON FUNCTION notify_node_leases_released() IS
    'Notifies the node_leases channel after a statement has deleted leases';

CREATE TRIGGER node_leases_released
    AFTER DELETE ON node_leases
    FOR EACH STATEMENT EXECUTE FUNCTION notify_node_leases_released();

ALTER TABLE task_tracker
    ADD COLUMN wait_reason varchar(16) NULL CHECK (wait_reason IN ('node_lease')),
    ADD COLUMN lease_wait_since timestamptz NULL;
COMMENT ON COLUMN task_tracker.wait_reason IS 'What a pending task waits for besides its upstream tasks: node_lease while another task holds the lease on its node; NULL when it waits for nothing';
COMMENT ON COLUMN task_tracker.lease_wait_since IS 'When the task began to wait for its node lease; NULL when it is not waiting';

CREATE INDEX idx_task_tracker_lease_waiters
    ON task_tracker (node_key, lease_wait_since)
    WHERE wait_reason = 'node_lease';
COMMENT ON INDEX idx_task_tracker_lease_waiters IS
    'The tasks waiting for a node lease, oldest first per node; the waker grants from it.';
