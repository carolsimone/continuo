-- state reads the schedule names a tenant has from its own copy of the live
-- release: each model's topology_nodes.schedule_name, the first dbt tag of the
-- model, names the schedule that runs it. A schedule's cron, when it has one,
-- is its row in schedules. A trigger starts a name that a model of the live
-- release carries or that has a row in schedules; any other name is not found.
-- Nothing reads or writes schedule_catalog or schedule_catalog_state.
DROP TABLE schedule_catalog;
DROP TABLE schedule_catalog_state;

-- A run keeps its schedule_name, the schedule it runs or the name a single-node
-- or promote-seed run is given, and the release it runs on (release_id,
-- promotion_seq). Each task carries the image tag of its node in that release
-- (tasks.image_tag), so the run stores no per-service metadata.
ALTER TABLE runs DROP COLUMN service_metadata;
