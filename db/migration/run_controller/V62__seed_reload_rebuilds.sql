-- What a seed reload needs: each node's dbt materialization, a seed's
-- +full_refresh setting and the relation the node writes; whether a release
-- skipped validation; each task's plan reason, seed preflight and the seed whose
-- load it rebuilds after; each attempt's preflight findings; and the views a
-- seed load left missing or stale.
ALTER TABLE topology_nodes
    ADD COLUMN materialized         text    NOT NULL DEFAULT '',
    ADD COLUMN full_refresh         boolean NULL,
    ADD COLUMN resolved_relation_id text    NOT NULL DEFAULT '';
ALTER TABLE topology_releases ADD COLUMN bootstrap boolean NOT NULL DEFAULT false;
ALTER TABLE tasks
    ADD COLUMN plan_reason      text  NOT NULL DEFAULT '',
    ADD COLUMN seed_preflight   jsonb NULL,
    ADD COLUMN rebuild_seed_key text  NOT NULL DEFAULT '';
ALTER TABLE task_attempts ADD COLUMN seed_preflight_findings jsonb NULL;
CREATE TABLE view_outages (
    tenant_id text        NOT NULL,
    node_key  text        NOT NULL,
    state     text        NOT NULL,
    since     timestamptz NOT NULL,
    seed_key  text        NOT NULL,
    run_id    uuid        NOT NULL,
    CONSTRAINT view_outages_pkey PRIMARY KEY (tenant_id, node_key),
    CONSTRAINT view_outages_state_check CHECK (state IN ('missing', 'stale'))
);
COMMENT ON COLUMN topology_nodes.full_refresh IS 'A dbt seed''s +full_refresh setting; NULL when the seed leaves it unset, and on every other node.';
COMMENT ON COLUMN tasks.rebuild_seed_key IS 'The seed whose load drops the relation this task rebuilds; empty for every other task.';
COMMENT ON TABLE view_outages IS 'One row per view a seed load dropped (missing) or left stale (stale), from the commit of that load until a task of the view succeeds.';
