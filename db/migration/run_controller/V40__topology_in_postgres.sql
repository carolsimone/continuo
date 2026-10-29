-- state's copy of every promoted release's topology, and its own pointer to
-- the release that is live.
--
-- topology_releases has one row per (tenant, release) state has loaded from the
-- release's topology artifact: the highest promotion_seq the release was
-- announced under, the artifact's URI and SHA-256, and when it was loaded. A
-- release's nodes and edges are written once, with that row, and never change.
--
-- topology_nodes holds the release's nodes other than dbt tests, keyed by
-- node_key, the lowercased "<schema>.<table>". test_count is NULL when the
-- number of data tests on the node is unknown.
--
-- topology_edges holds one row per (upstream, downstream) pair of the release's
-- nodes; both ends are nodes of the same release.
--
-- live_release names, per tenant, the release state treats as live and the
-- promotion_seq it was applied under; it moves only to a higher promotion_seq.
-- Its foreign key has no cascade, so the live release cannot be deleted.
--
-- node_materializations records, per tenant and node, the version last loaded
-- into the warehouse (content_hash, image_tag, release_id, run_id, task_id,
-- materialized_at, source) and the latest build the seed reconcile requested
-- for it (attempt_content_hash, attempt_run_id, attempted_at).
--
-- scheduler_tracker.release_id names the release a run builds, for the runs
-- that carry one.

CREATE TABLE topology_releases (
    tenant_id       text        NOT NULL,
    release_id      text        NOT NULL,
    promotion_seq   bigint      NOT NULL,
    topology_uri    text        NOT NULL,
    topology_sha256 text        NOT NULL,
    loaded_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT topology_releases_pkey PRIMARY KEY (tenant_id, release_id)
);

CREATE TABLE topology_nodes (
    tenant_id     text    NOT NULL,
    release_id    text    NOT NULL,
    node_key      text    NOT NULL,
    unique_id     text    NOT NULL,
    service_name  text    NOT NULL,
    schema_name   text    NOT NULL,
    table_name    text    NOT NULL,
    node_type     text    NOT NULL,
    test_count    integer NULL,
    content_hash  text    NOT NULL DEFAULT '',
    image_tag     text    NOT NULL DEFAULT '',
    secret_ref    text    NOT NULL DEFAULT '',
    schedule_name text    NOT NULL DEFAULT '',
    CONSTRAINT topology_nodes_pkey PRIMARY KEY (tenant_id, release_id, node_key),
    CONSTRAINT topology_nodes_release_fkey FOREIGN KEY (tenant_id, release_id)
        REFERENCES topology_releases (tenant_id, release_id) ON DELETE CASCADE
);

CREATE TABLE topology_edges (
    tenant_id      text NOT NULL,
    release_id     text NOT NULL,
    upstream_key   text NOT NULL,
    downstream_key text NOT NULL,
    CONSTRAINT topology_edges_pkey PRIMARY KEY (tenant_id, release_id, upstream_key, downstream_key),
    CONSTRAINT topology_edges_upstream_fkey FOREIGN KEY (tenant_id, release_id, upstream_key)
        REFERENCES topology_nodes (tenant_id, release_id, node_key) ON DELETE CASCADE,
    CONSTRAINT topology_edges_downstream_fkey FOREIGN KEY (tenant_id, release_id, downstream_key)
        REFERENCES topology_nodes (tenant_id, release_id, node_key) ON DELETE CASCADE
);

CREATE INDEX idx_topology_edges_downstream ON topology_edges (tenant_id, release_id, downstream_key);

CREATE TABLE live_release (
    tenant_id     text        NOT NULL,
    release_id    text        NOT NULL,
    promotion_seq bigint      NOT NULL,
    updated_at    timestamptz NOT NULL,
    CONSTRAINT live_release_pkey PRIMARY KEY (tenant_id),
    CONSTRAINT live_release_release_fkey FOREIGN KEY (tenant_id, release_id)
        REFERENCES topology_releases (tenant_id, release_id)
);

CREATE TABLE node_materializations (
    tenant_id            text        NOT NULL,
    node_key             text        NOT NULL,
    content_hash         text        NOT NULL DEFAULT '',
    image_tag            text        NOT NULL DEFAULT '',
    release_id           text        NOT NULL DEFAULT '',
    run_id               uuid        NULL,
    task_id              uuid        NULL,
    materialized_at      timestamptz NULL,
    source               text        NULL,
    attempt_content_hash text        NOT NULL DEFAULT '',
    attempt_run_id       uuid        NULL,
    attempted_at         timestamptz NULL,
    CONSTRAINT node_materializations_pkey PRIMARY KEY (tenant_id, node_key),
    CONSTRAINT node_materializations_source_check CHECK (source IN ('build', 'adopted'))
);

ALTER TABLE scheduler_tracker ADD COLUMN release_id text NULL;

CREATE INDEX idx_scheduler_tracker_release_id ON scheduler_tracker (release_id)
    WHERE release_id IS NOT NULL;

COMMENT ON COLUMN topology_nodes.node_key IS
    'Lowercased "<schema>.<table>"; unique within a release.';
COMMENT ON COLUMN topology_releases.promotion_seq IS
    'The highest promotion_seq this release was announced under.';
COMMENT ON COLUMN node_materializations.source IS
    'build: a run loaded this version; adopted: recorded as already loaded when state applied its first promotion; NULL: no version recorded yet.';
COMMENT ON COLUMN scheduler_tracker.release_id IS
    'The release whose topology this run builds; NULL for runs that carry none.';
