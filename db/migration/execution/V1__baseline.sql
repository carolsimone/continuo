-- continuo_execution: execution-controller's database.
--
-- The attempt each Kubernetes Job runs (deployments), the Job capacity it
-- admits them under, the runs a cancel fenced, the validation results of a
-- release leg, the messages it consumed and its outbox.
--
-- Each table is one block: its columns and constraints, then its indexes,
-- triggers and comments. The admission_capacity row is inserted at the end.

-- Functions

CREATE FUNCTION continuo_deployments_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_notify('deployments', '');
    RETURN NULL;
END;
$$;

CREATE FUNCTION continuo_outbox_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_notify(TG_TABLE_NAME, '');
    RETURN NULL;
END;
$$;

-- admission_capacity

CREATE TABLE admission_capacity (
    scope text NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT admission_capacity_pkey PRIMARY KEY (scope)
);

-- cancelled_runs

CREATE TABLE cancelled_runs (
    run_id uuid NOT NULL,
    cancelled_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT cancelled_schedules_pkey PRIMARY KEY (run_id)
);

COMMENT ON TABLE cancelled_runs IS 'a run cancelled by run-controller: no attempt of it starts, and every Job of it is deleted; kept for the replay horizon';

-- message_processing

CREATE TABLE message_processing (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    message_id varchar(255) NOT NULL,
    stream_name varchar(100) NOT NULL,
    state varchar(50) NOT NULL,
    payload jsonb NOT NULL,
    error text,
    created_at timestamptz DEFAULT now() NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    outbox_entry_id uuid,
    CONSTRAINT message_processing_pkey PRIMARY KEY (id),
    CONSTRAINT message_processing_message_id_stream_name_key UNIQUE (message_id, stream_name),
    CONSTRAINT message_processing_state_check CHECK ((state IN ('processing', 'completed', 'acked')))
);

CREATE INDEX idx_message_processing_created_at ON message_processing (created_at);

CREATE INDEX idx_message_processing_message_id ON message_processing (message_id);

CREATE UNIQUE INDEX idx_message_processing_outbox_entry_id_stream ON message_processing (outbox_entry_id, stream_name) WHERE outbox_entry_id IS NOT NULL;

CREATE INDEX idx_message_processing_state ON message_processing (state);

COMMENT ON TABLE message_processing IS 'Tracks consumed Redis messages for exactly-once processing.';

-- validation_aggregates

CREATE TABLE validation_aggregates (
    release_id text NOT NULL,
    mode text DEFAULT 'validation' NOT NULL,
    aggregate_emitted_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT validation_aggregates_pkey PRIMARY KEY (release_id, mode),
    CONSTRAINT validation_aggregates_mode_check CHECK ((mode IN ('validation', 'seed_build', 'compile')))
);

-- deployments

CREATE TABLE deployments (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    message_processing_id uuid,
    task_id uuid NOT NULL,
    run_id uuid NOT NULL,
    job_params jsonb NOT NULL,
    status text DEFAULT 'pending' NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    max_retries integer DEFAULT 3 NOT NULL,
    next_attempt_at timestamptz DEFAULT now() NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    deployed_at timestamptz,
    error_message text,
    mode text DEFAULT 'production' NOT NULL,
    release_id text,
    node_id text,
    outcome text,
    dbt_log_uri text,
    outcome_at timestamptz,
    run_results_uri text,
    failed_container text,
    state_changed_at timestamptz DEFAULT now() NOT NULL,
    job_name text GENERATED ALWAYS AS ((job_params ->> 'job_name')) STORED,
    facts jsonb,
    attempt integer GENERATED ALWAYS AS (((job_params ->> 'attempt'))::integer) STORED,
    last_report jsonb,
    tenant_id text NOT NULL,
    CONSTRAINT deployments_pkey PRIMARY KEY (id),
    CONSTRAINT deployments_candidate_identity_check CHECK (((mode = 'production') OR ((release_id IS NOT NULL) AND (node_id IS NOT NULL)))),
    CONSTRAINT deployments_mode_check CHECK ((mode IN ('production', 'validation', 'seed_build', 'compile'))),
    CONSTRAINT deployments_outcome_check CHECK (((outcome IN ('ok', 'failed', 'skipped', 'stopped')) OR (outcome IS NULL))),
    CONSTRAINT deployments_status_check CHECK ((status IN ('pending', 'blocked', 'reserved', 'starting', 'running', 'terminating', 'done', 'failed', 'skipped'))),
    CONSTRAINT deployments_message_processing_id_fkey FOREIGN KEY (message_processing_id) REFERENCES message_processing(id) ON DELETE SET NULL
);

CREATE INDEX idx_deployments_candidate_release ON deployments (release_id, mode) WHERE mode IN ('validation', 'seed_build', 'compile');

CREATE INDEX idx_deployments_due ON deployments (next_attempt_at) WHERE status = 'pending';

CREATE INDEX idx_deployments_in_flight ON deployments (status, state_changed_at) WHERE status IN ('reserved', 'starting', 'running', 'terminating');

CREATE INDEX idx_deployments_job_name ON deployments (job_name);

CREATE INDEX idx_deployments_message_processing_id ON deployments (message_processing_id) WHERE message_processing_id IS NOT NULL;

CREATE INDEX idx_deployments_open_by_run ON deployments (run_id) WHERE (mode = 'production') AND (status IN ('pending', 'reserved', 'starting', 'running', 'terminating'));

CREATE UNIQUE INDEX uq_deployments_candidate_release_node_mode ON deployments (release_id, node_id, mode) WHERE mode IN ('validation', 'seed_build', 'compile');

CREATE UNIQUE INDEX uq_deployments_task_attempt ON deployments (task_id, attempt) WHERE (mode = 'production') AND (attempt IS NOT NULL);

CREATE TRIGGER deployments_accepted_notify AFTER INSERT ON deployments FOR EACH STATEMENT EXECUTE FUNCTION continuo_deployments_notify();

CREATE TRIGGER deployments_admission_notify AFTER UPDATE OF status ON deployments FOR EACH ROW WHEN ((((new.status = 'pending') AND (old.status <> 'pending')) OR ((old.status = ANY (ARRAY['reserved', 'starting', 'running', 'terminating'])) AND (new.status <> ALL (ARRAY['reserved', 'starting', 'running', 'terminating']))))) EXECUTE FUNCTION continuo_deployments_notify();

COMMENT ON COLUMN deployments.tenant_id IS 'Tenant of the task.dispatch:v1 the deployment runs; its reports are published under it';

-- execution_outbox

CREATE TABLE execution_outbox (
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
    CONSTRAINT execution_outbox_pkey PRIMARY KEY (id),
    CONSTRAINT execution_outbox_status_check CHECK ((status IN ('pending', 'scheduled', 'processed', 'failed'))),
    CONSTRAINT execution_outbox_message_processing_id_fkey FOREIGN KEY (message_processing_id) REFERENCES message_processing(id)
);

CREATE INDEX idx_execution_outbox_aggregate ON execution_outbox (aggregate_type, aggregate_id);

CREATE INDEX idx_execution_outbox_claimable ON execution_outbox (created_at, id) WHERE status IN ('pending', 'scheduled');

CREATE INDEX idx_execution_outbox_failed ON execution_outbox (created_at) WHERE status = 'failed';

CREATE INDEX idx_execution_outbox_message_processing_id ON execution_outbox (message_processing_id) WHERE message_processing_id IS NOT NULL;

CREATE INDEX idx_execution_outbox_open_by_aggregate ON execution_outbox (aggregate_type, aggregate_id, created_at) WHERE status IN ('pending', 'scheduled');

CREATE TRIGGER execution_outbox_notify AFTER INSERT ON execution_outbox FOR EACH STATEMENT EXECUTE FUNCTION continuo_outbox_notify();

INSERT INTO admission_capacity (scope) VALUES ('global');
