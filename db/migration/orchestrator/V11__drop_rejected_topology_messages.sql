-- The legacy topology-ingest path (update.graph:v1 -> manifest.loaded:v1 ->
-- IngestTopology) has been retired; topology now enters exclusively via
-- release.promoted:v1. The forensics table for that path's permanently-rejected
-- messages is no longer written or read.
DROP TABLE IF EXISTS rejected_topology_messages;
