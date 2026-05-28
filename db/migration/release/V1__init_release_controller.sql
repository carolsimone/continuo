-- release-controller initial schema.
-- See docs/superpowers/specs/2026-05-28-dbt-blue-green-design.md §6.1.
-- Aligns with pkg/messageprocessing and pkg/outbox canonical shapes.

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
CREATE INDEX idx_releases_active_status ON releases (created_at)
    WHERE status IN ('received','parsing','validating');

-- message_processing: shared shape consumed by pkg/messageprocessing.
-- Must be created before release_controller_outbox because the outbox
-- table has a nullable FK to message_processing(id).
CREATE TABLE message_processing (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id      VARCHAR(255) NOT NULL,
    stream_name     VARCHAR(100) NOT NULL,
    state           VARCHAR(50)  NOT NULL CHECK (state IN ('processing','completed','acked')),
    payload         JSONB        NOT NULL,
    error           TEXT,
    outbox_entry_id UUID         NULL,
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT message_processing_message_id_stream_name_key UNIQUE (message_id, stream_name)
);
CREATE INDEX idx_message_processing_message_id  ON message_processing (message_id);
CREATE INDEX idx_message_processing_state       ON message_processing (state);
CREATE INDEX idx_message_processing_created_at  ON message_processing (created_at);
CREATE UNIQUE INDEX idx_message_processing_outbox_entry_id
    ON message_processing (outbox_entry_id)
    WHERE outbox_entry_id IS NOT NULL;

COMMENT ON TABLE message_processing IS 'Tracks consumed Redis messages for exactly-once processing';
COMMENT ON COLUMN message_processing.message_id IS 'Redis Stream message ID (e.g., 1738756432123-0)';
COMMENT ON COLUMN message_processing.state IS 'Processing state: processing, completed, acked';

-- release_controller_outbox: canonical pkg/outbox shape.
CREATE TABLE release_controller_outbox (
    id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    message_processing_id  UUID        NULL REFERENCES message_processing(id),
    aggregate_type         TEXT        NOT NULL,
    aggregate_id           UUID        NOT NULL,
    event_type             TEXT        NOT NULL,
    payload                JSONB       NOT NULL,
    stream_name            TEXT        NOT NULL,
    status                 TEXT        NOT NULL DEFAULT 'pending',
    retry_count            INT         NOT NULL DEFAULT 0,
    max_retries            INT         NOT NULL DEFAULT 3,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at           TIMESTAMPTZ NULL,
    error_message          TEXT        NULL,
    CONSTRAINT release_controller_outbox_status_check
        CHECK (status IN ('pending', 'processed', 'failed'))
);
CREATE INDEX idx_release_controller_outbox_pending
    ON release_controller_outbox (created_at)
    WHERE status = 'pending';
CREATE INDEX idx_release_controller_outbox_aggregate
    ON release_controller_outbox (aggregate_type, aggregate_id);
