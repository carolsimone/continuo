-- continuo_release: release-controller's database.
--
-- Each release check, a candidate release's or a fix's, with its legs and results, the
-- production pointer of each project and of the install, the promotion
-- sequence, the messages it consumed and its outbox.
--
-- Each table is one block: its columns and constraints, then its indexes,
-- triggers and comments. The promotion_sequence row is inserted at the end.

-- Functions

CREATE FUNCTION continuo_outbox_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_notify(TG_TABLE_NAME, '');
    RETURN NULL;
END;
$$;

-- live_release

CREATE TABLE live_release (
    id smallint DEFAULT 1 NOT NULL,
    release_id text NOT NULL,
    updated_at timestamptz NOT NULL,
    topology_uri text,
    topology_sha256 text,
    node_count integer,
    promotion_seq bigint DEFAULT 0 NOT NULL,
    CONSTRAINT live_release_pkey PRIMARY KEY (id),
    CONSTRAINT live_release_id_check CHECK ((id = 1))
);

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

-- promotion_sequence

CREATE TABLE promotion_sequence (
    id smallint DEFAULT 1 NOT NULL,
    last_seq bigint NOT NULL,
    CONSTRAINT promotion_sequence_pkey PRIMARY KEY (id),
    CONSTRAINT promotion_sequence_id_check CHECK ((id = 1))
);

-- release_checks

CREATE TABLE release_checks (
    check_id text NOT NULL,
    status text NOT NULL,
    image_tags jsonb NOT NULL,
    validation_node_ids text[],
    per_node_results jsonb,
    fail_reason text,
    failing_nodes text[],
    created_at timestamptz NOT NULL,
    parsing_started_at timestamptz,
    validating_started_at timestamptz,
    resolved_at timestamptz,
    transitions jsonb DEFAULT '[]'::jsonb NOT NULL,
    bootstrap boolean DEFAULT false NOT NULL,
    changed_project text DEFAULT '' NOT NULL,
    repo text NOT NULL,
    commit_sha text NOT NULL,
    code_bundle_uri text DEFAULT '' NOT NULL,
    manifest_kind text DEFAULT 'dbt' NOT NULL,
    fail_detail text DEFAULT '' NOT NULL,
    remediation_round integer DEFAULT 1 NOT NULL,
    rejection_payload jsonb,
    source_overlay_uri text DEFAULT '' NOT NULL,
    verifies_release_id text DEFAULT '' NOT NULL,
    check_kind text NOT NULL,
    try integer DEFAULT 0 NOT NULL,
    candidate_topology_uri text,
    candidate_topology_sha256 text,
    candidate_node_count integer,
    rebaseline boolean DEFAULT false NOT NULL,
    notices jsonb DEFAULT '[]'::jsonb NOT NULL,
    candidate_contract_bundle_uri text DEFAULT '' NOT NULL,
    candidate_contract_bundle_sha256 text DEFAULT '' NOT NULL,
    continuo_contracts text DEFAULT '' NOT NULL,
    parse_ms bigint,
    contracts_ms bigint,
    contracts_by_family jsonb,
    CONSTRAINT release_checks_pkey PRIMARY KEY (check_id),
    CONSTRAINT release_checks_kind_status_check CHECK ((((check_kind = 'candidate') AND (status <> ALL (ARRAY['passed', 'failed']))) OR ((check_kind = 'fix') AND (status <> ALL (ARRAY['promoted', 'rejected', 'superseded']))))),
    CONSTRAINT release_checks_manifest_kind_check CHECK ((manifest_kind IN ('dbt', 'python', 'ontology'))),
    CONSTRAINT release_checks_rebaseline_needs_bootstrap CHECK (((NOT rebaseline) OR bootstrap)),
    CONSTRAINT release_checks_check_kind_check CHECK ((check_kind IN ('candidate', 'fix'))),
    CONSTRAINT release_checks_status_check CHECK ((status IN ('received', 'compiling', 'parsing', 'seed_building', 'validating', 'promoted', 'rejected', 'superseded', 'passed', 'failed')))
);

CREATE INDEX idx_release_checks_active ON release_checks (created_at) WHERE status IN ('received', 'compiling', 'parsing', 'seed_building', 'validating');

CREATE INDEX idx_release_checks_kind_created ON release_checks (check_kind, created_at DESC, check_id DESC);

CREATE INDEX idx_release_checks_kind_status_created ON release_checks (check_kind, status, created_at DESC, check_id DESC);

CREATE INDEX idx_release_checks_verifies ON release_checks (verifies_release_id) WHERE check_kind = 'fix';

-- production_pointers

CREATE TABLE production_pointers (
    project_name text NOT NULL,
    release_id text NOT NULL,
    manifest_s3_key text NOT NULL,
    image_tag text NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    manifest_kind text DEFAULT 'dbt' NOT NULL,
    CONSTRAINT production_pointers_pkey PRIMARY KEY (project_name),
    CONSTRAINT production_pointers_manifest_kind_check CHECK ((manifest_kind IN ('dbt', 'python', 'ontology')))
);

-- release_controller_outbox

CREATE TABLE release_controller_outbox (
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
    CONSTRAINT release_controller_outbox_pkey PRIMARY KEY (id),
    CONSTRAINT release_controller_outbox_status_check CHECK ((status IN ('pending', 'scheduled', 'processed', 'failed'))),
    CONSTRAINT release_controller_outbox_message_processing_id_fkey FOREIGN KEY (message_processing_id) REFERENCES message_processing(id)
);

CREATE INDEX idx_release_controller_outbox_aggregate ON release_controller_outbox (aggregate_type, aggregate_id);

CREATE INDEX idx_release_controller_outbox_claimable ON release_controller_outbox (created_at, id) WHERE status IN ('pending', 'scheduled');

CREATE INDEX idx_release_controller_outbox_failed ON release_controller_outbox (created_at) WHERE status = 'failed';

CREATE INDEX idx_release_controller_outbox_open_by_aggregate ON release_controller_outbox (aggregate_type, aggregate_id, created_at) WHERE status IN ('pending', 'scheduled');

CREATE TRIGGER release_controller_outbox_notify AFTER INSERT ON release_controller_outbox FOR EACH STATEMENT EXECUTE FUNCTION continuo_outbox_notify();

INSERT INTO promotion_sequence (id, last_seq) VALUES (1, 0);
