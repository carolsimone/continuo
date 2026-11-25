-- continuo_remediation: remediation's database.
--
-- The classifier's decision on each failing node of a rejected release, the
-- messages it consumed and its outbox.
--
-- Each table is one block: its columns and constraints, then its indexes,
-- triggers and comments.

-- Functions

CREATE FUNCTION continuo_outbox_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_notify(TG_TABLE_NAME, '');
    RETURN NULL;
END;
$$;

-- classification_decision

CREATE TABLE classification_decision (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source text NOT NULL,
    release_id text NOT NULL,
    node_id text NOT NULL,
    category text NOT NULL,
    error_signature text NOT NULL,
    decision text NOT NULL,
    reason text NOT NULL,
    dbt_log_uri text DEFAULT '' NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    remediation_round integer DEFAULT 1 NOT NULL,
    CONSTRAINT classification_decision_pkey PRIMARY KEY (id),
    CONSTRAINT classification_decision_uniq UNIQUE (source, release_id, remediation_round, node_id),
    CONSTRAINT classification_decision_category_check CHECK ((category IN ('logic', 'test', 'unknown', 'infra_transient'))),
    CONSTRAINT classification_decision_decision_check CHECK ((decision IN ('emit', 'drop')))
);

CREATE INDEX idx_classification_decision_signature ON classification_decision (node_id, error_signature);

-- message_processing

CREATE TABLE message_processing (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    message_id varchar(255) NOT NULL,
    stream_name varchar(100) NOT NULL,
    state varchar(50) NOT NULL,
    payload jsonb NOT NULL,
    error text,
    outbox_entry_id uuid,
    created_at timestamp DEFAULT now() NOT NULL,
    updated_at timestamp DEFAULT now() NOT NULL,
    CONSTRAINT message_processing_pkey PRIMARY KEY (id),
    CONSTRAINT message_processing_message_id_stream_name_key UNIQUE (message_id, stream_name),
    CONSTRAINT message_processing_state_check CHECK ((state IN ('processing', 'completed', 'acked')))
);

CREATE INDEX idx_message_processing_created_at ON message_processing (created_at);

CREATE INDEX idx_message_processing_message_id ON message_processing (message_id);

CREATE UNIQUE INDEX idx_message_processing_outbox_entry_id_stream ON message_processing (outbox_entry_id, stream_name) WHERE outbox_entry_id IS NOT NULL;

CREATE INDEX idx_message_processing_state ON message_processing (state);

COMMENT ON TABLE message_processing IS 'Tracks consumed Redis messages for exactly-once processing';

COMMENT ON COLUMN message_processing.message_id IS 'Redis Stream message ID (e.g., 1738756432123-0)';

COMMENT ON COLUMN message_processing.state IS 'Processing state: processing, completed, acked';

-- remediation_outbox

CREATE TABLE remediation_outbox (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    message_processing_id uuid,
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    stream_name text NOT NULL,
    status text DEFAULT 'pending' NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    max_retries integer DEFAULT 13 NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    processed_at timestamptz,
    error_message text,
    next_attempt_at timestamptz,
    CONSTRAINT remediation_outbox_pkey PRIMARY KEY (id),
    CONSTRAINT remediation_outbox_status_check CHECK ((status IN ('pending', 'scheduled', 'processed', 'failed'))),
    CONSTRAINT remediation_outbox_message_processing_id_fkey FOREIGN KEY (message_processing_id) REFERENCES message_processing(id)
);

CREATE INDEX idx_remediation_outbox_claimable ON remediation_outbox (created_at, id) WHERE status IN ('pending', 'scheduled');

CREATE INDEX idx_remediation_outbox_failed ON remediation_outbox (created_at) WHERE status = 'failed';

CREATE INDEX idx_remediation_outbox_open_by_aggregate ON remediation_outbox (aggregate_type, aggregate_id, created_at) WHERE status IN ('pending', 'scheduled');

CREATE TRIGGER remediation_outbox_notify AFTER INSERT ON remediation_outbox FOR EACH STATEMENT EXECUTE FUNCTION continuo_outbox_notify();
