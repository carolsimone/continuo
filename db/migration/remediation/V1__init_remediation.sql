-- remediation-service initial schema.
-- message_processing is the shared shape consumed by pkg/messageprocessing.
-- remediation_outbox and classification_decision are remediation-specific.

-- message_processing: shared shape consumed by pkg/messageprocessing.
-- Must be created before remediation_outbox because the outbox
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

-- remediation_outbox: canonical pkg/outbox shape.
CREATE TABLE remediation_outbox (
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
    CONSTRAINT remediation_outbox_status_check
        CHECK (status IN ('pending', 'processed', 'failed'))
);
CREATE INDEX idx_remediation_outbox_pending
    ON remediation_outbox (created_at) WHERE status = 'pending';

-- classification_decision: append-only audit of every classified node,
-- recorded for emit AND drop. Natural key gives idempotency on redelivery.
CREATE TABLE classification_decision (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source          TEXT        NOT NULL,
    release_id      TEXT        NOT NULL,
    node_id         TEXT        NOT NULL,
    category        TEXT        NOT NULL,
    error_signature TEXT        NOT NULL,
    decision        TEXT        NOT NULL,
    reason          TEXT        NOT NULL,
    dbt_log_uri     TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT classification_decision_decision_check CHECK (decision IN ('emit','drop')),
    CONSTRAINT classification_decision_category_check
        CHECK (category IN ('logic','test','unknown','infra_transient')),
    CONSTRAINT classification_decision_uniq UNIQUE (source, release_id, node_id)
);
CREATE INDEX idx_classification_decision_signature
    ON classification_decision (node_id, error_signature);
