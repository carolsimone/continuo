-- History-list indexes and removal of the vestigial dbt_logs_uri column.
-- Per-node dbt log URIs now live in releases.per_node_results; the aggregate
-- dbt_logs_uri was always written empty.
CREATE INDEX idx_releases_created_at        ON releases (created_at DESC, release_id DESC);
CREATE INDEX idx_releases_status_created_at ON releases (status, created_at DESC, release_id DESC);
ALTER TABLE releases DROP COLUMN dbt_logs_uri;
