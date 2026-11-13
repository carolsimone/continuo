-- idx_tasks_node_key_created serves the read of the newest task of a node
-- within a tenant: the schedule graph reads the last run of each dormant node
-- it shows through it, one probe per node, whatever the size of the task
-- history. A task with no node key is never read by node, so it is left out.
CREATE INDEX idx_tasks_node_key_created
    ON tasks (tenant_id, node_key, created_at DESC)
    WHERE node_key IS NOT NULL;
COMMENT ON INDEX idx_tasks_node_key_created IS
    'The tasks of one node within a tenant, newest first; the schedule graph reads the last run of a dormant node from it.';
