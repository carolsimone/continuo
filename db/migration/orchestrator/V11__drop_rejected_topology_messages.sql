-- Topology enters production exclusively via release.promoted:v1. This table
-- was a forensics sink for permanently-rejected topology-ingest messages and is
-- unused; drop it.
DROP TABLE IF EXISTS rejected_topology_messages;
