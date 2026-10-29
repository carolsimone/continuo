-- The task timeout a node's domain team declared, stored with the node of the
-- release that declared it. A stored release never changes, so a run keeps the
-- timeouts of the release it runs on.

ALTER TABLE topology_nodes ADD COLUMN timeout_seconds integer NULL;

COMMENT ON COLUMN topology_nodes.timeout_seconds IS 'Task timeout in seconds the node declared in this release; NULL when it declared none';
