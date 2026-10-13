-- release-controller keeps every stored topology in an object-storage artifact
-- and only the artifact reference in Postgres. The columns that held a
-- topology inline are dropped:
-- release_pipeline_runs.candidate_topology,
-- release_pipeline_runs.parsing_at_upgrade and
-- current_prod.topology_snapshot.
--
-- release-controller 0.10.0 and later moves every inline topology
-- into an artifact when it starts, leaves no run in the parsing state that
-- waited on a pre-artifact parse result, and points current_prod at an
-- artifact. The guard below refuses the migration while any row is not yet
-- settled, so no topology is lost; each count selects exactly the rows that
-- startup step settles.
DO $$
DECLARE
    inline_runs  bigint;
    waiting_runs bigint;
    legacy_prod  bigint;
BEGIN
    SELECT count(*) INTO inline_runs
      FROM release_pipeline_runs
     WHERE candidate_topology IS NOT NULL
       AND candidate_topology <> 'null'::jsonb
       AND candidate_topology_uri IS NULL;
    SELECT count(*) INTO waiting_runs
      FROM release_pipeline_runs
     WHERE parsing_at_upgrade AND status = 'parsing';
    SELECT count(*) INTO legacy_prod
      FROM current_prod
     WHERE id = 1 AND topology_uri IS NULL AND release_id <> '';
    IF inline_runs + waiting_runs + legacy_prod > 0 THEN
        RAISE EXCEPTION 'release-controller''s stored topologies are not all in artifacts: % run(s) hold an inline candidate topology, % run(s) wait on a parse result from before artifacts, % current_prod row(s) reference no artifact; upgrade to continuo 0.10.0 and let release-controller start once before upgrading to this release',
            inline_runs, waiting_runs, legacy_prod;
    END IF;
END $$;

ALTER TABLE release_pipeline_runs
    DROP COLUMN candidate_topology,
    DROP COLUMN parsing_at_upgrade;
ALTER TABLE current_prod
    DROP COLUMN topology_snapshot;
