-- remediation-agent initial schema.
-- message_processing is the shared shape consumed by pkg/messageprocessing.
-- remediation_agent_outbox is the canonical pkg/outbox shape.
-- proposal records each fix-proposal attempt for a failed dbt node.

-- message_processing: shared shape consumed by pkg/messageprocessing.
-- Must be created before remediation_agent_outbox because the outbox
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

-- remediation_agent_outbox: canonical pkg/outbox shape.
CREATE TABLE remediation_agent_outbox (
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
    CONSTRAINT remediation_agent_outbox_status_check
        CHECK (status IN ('pending', 'processed', 'failed'))
);
CREATE INDEX idx_remediation_agent_outbox_pending
    ON remediation_agent_outbox (created_at) WHERE status = 'pending';

-- proposal: append-only record of each fix-proposal attempt for a failed
-- dbt node. One row per attempt; unique on (release_id, node_id, attempt).
-- idx_proposal_node_signature supports CountAttempts lookups.
CREATE TABLE proposal (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source          TEXT        NOT NULL,
    release_id      TEXT        NOT NULL,
    node_id         TEXT        NOT NULL,
    error_signature TEXT        NOT NULL,
    attempt         INT         NOT NULL,
    status          TEXT        NOT NULL,
    confidence      TEXT        NOT NULL DEFAULT '',
    rationale       TEXT        NOT NULL DEFAULT '',
    proposed_sql_uri TEXT       NOT NULL DEFAULT '',
    diff_uri        TEXT        NOT NULL DEFAULT '',
    model           TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT proposal_status_check CHECK (status IN ('proposed','skipped','failed','escalated')),
    CONSTRAINT proposal_uniq UNIQUE (release_id, node_id, attempt)
);
CREATE INDEX idx_proposal_node_signature ON proposal (source, node_id, error_signature);
