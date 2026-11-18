-- Who must approve what an attempt did with each node a remediation settled at
-- its end, the fixed nodes and the handed-over nodes: a JSON array of
-- {node_id, owner, source} objects, one per node, sorted by node id. A row the
-- legacy engine wrote, or one written before approvers were kept, holds the
-- empty array.
ALTER TABLE proposal ADD COLUMN approvers JSONB NOT NULL DEFAULT '[]'::jsonb;
