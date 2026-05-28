-- release-controller initial schema.
-- See docs/superpowers/specs/2026-05-28-dbt-blue-green-design.md §6.1.

CREATE TABLE current_prod (
    id                       smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    release_id               text NOT NULL,
    topology_snapshot        jsonb NOT NULL,
    updated_at               timestamptz NOT NULL
);

CREATE TABLE releases (
    release_id               text PRIMARY KEY,
    status                   text NOT NULL CHECK (status IN
                              ('received','parsing','validating',
                               'promoted','rejected','superseded')),
    changed_node_ids         text[] NOT NULL,
    image_tags               jsonb NOT NULL,
    manifests_uri            text NOT NULL,
    candidate_topology       jsonb,
    validation_node_ids      text[],
    per_node_results         jsonb,
    reject_reason            text,
    failing_nodes            text[],
    dbt_logs_uri             text,
    created_at               timestamptz NOT NULL,
    parsing_started_at       timestamptz,
    validating_started_at    timestamptz,
    resolved_at              timestamptz,
    transitions              jsonb NOT NULL DEFAULT '[]'::jsonb
);
CREATE INDEX releases_active_status_idx ON releases (created_at)
    WHERE status IN ('received','parsing','validating');

CREATE TABLE release_controller_outbox (
    id                       bigserial PRIMARY KEY,
    stream                   text NOT NULL,
    payload                  jsonb NOT NULL,
    created_at               timestamptz NOT NULL,
    published_at             timestamptz
);
CREATE INDEX release_controller_outbox_unpublished_idx
    ON release_controller_outbox (id) WHERE published_at IS NULL;

CREATE TABLE processed_messages (
    stream                   text NOT NULL,
    message_id               text NOT NULL,
    processed_at             timestamptz NOT NULL,
    PRIMARY KEY (stream, message_id)
);
