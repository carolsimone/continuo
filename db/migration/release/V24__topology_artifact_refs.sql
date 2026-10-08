-- A run's candidate topology lives in an immutable S3 artifact (written by
-- topology-controller, one per run); the run stores where it is, the SHA-256
-- of its stored bytes, and how many nodes it holds. The three columns are NULL
-- until the run's parse result names an artifact.
ALTER TABLE release_pipeline_runs
    ADD COLUMN candidate_topology_uri    text,
    ADD COLUMN candidate_topology_sha256 text,
    ADD COLUMN candidate_node_count      integer;

-- parsing_at_upgrade marks the runs that were waiting on a parse result when
-- this migration ran. That result was published on a stream release-controller
-- no longer reads, so the one-time startup step fails those runs with reason
-- upgrade_interrupted instead of leaving them parsing forever.
ALTER TABLE release_pipeline_runs
    ADD COLUMN parsing_at_upgrade boolean NOT NULL DEFAULT false;
UPDATE release_pipeline_runs SET parsing_at_upgrade = true WHERE status = 'parsing';
