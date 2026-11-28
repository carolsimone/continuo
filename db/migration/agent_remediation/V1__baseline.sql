-- continuo_agent_remediation: agent-remediation's database.
--
-- Each remediation with its tries, clusters and journal of steps, the
-- fixes and their pull requests, the messages it consumed and its outbox.
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

CREATE FUNCTION stamp_pr_claimed_at_on_opening() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.pr_state = 'opening' AND NEW.pr_state <> 'opening' THEN
        NEW.pr_claimed_at := NULL;
    ELSIF NEW.pr_state = 'opening' AND NEW.pr_claimed_at IS NULL THEN
        NEW.pr_claimed_at := clock_timestamp();
    END IF;
    RETURN NEW;
END;
$$;

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

-- remediation

CREATE TABLE remediation (
    id uuid NOT NULL,
    tenant_id text NOT NULL,
    release_id text NOT NULL,
    round integer NOT NULL,
    flow text NOT NULL,
    status text NOT NULL,
    trigger_payload jsonb NOT NULL,
    trigger_message_id text NOT NULL,
    trigger_fields jsonb NOT NULL,
    current_try integer DEFAULT 0 NOT NULL,
    lease_owner text,
    lease_until timestamptz,
    failure text DEFAULT '' NOT NULL,
    failures integer DEFAULT 0 NOT NULL,
    last_error text DEFAULT '' NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT remediation_pkey PRIMARY KEY (id),
    CONSTRAINT remediation_tenant_id_release_id_round_key UNIQUE (tenant_id, release_id, round),
    CONSTRAINT remediation_round_check CHECK ((round >= 1)),
    CONSTRAINT remediation_status_check CHECK ((status IN ('running', 'done', 'system_failure')))
);

CREATE INDEX remediation_claimable ON remediation (created_at) WHERE status = 'running';

-- agent_remediation_outbox

CREATE TABLE agent_remediation_outbox (
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
    CONSTRAINT agent_remediation_outbox_pkey PRIMARY KEY (id),
    CONSTRAINT agent_remediation_outbox_status_check CHECK ((status IN ('pending', 'scheduled', 'processed', 'failed'))),
    CONSTRAINT agent_remediation_outbox_message_processing_id_fkey FOREIGN KEY (message_processing_id) REFERENCES message_processing(id)
);

CREATE INDEX idx_agent_remediation_outbox_claimable ON agent_remediation_outbox (created_at, id) WHERE status IN ('pending', 'scheduled');

CREATE INDEX idx_agent_remediation_outbox_failed ON agent_remediation_outbox (created_at) WHERE status = 'failed';

CREATE INDEX idx_agent_remediation_outbox_open_by_aggregate ON agent_remediation_outbox (aggregate_type, aggregate_id, created_at) WHERE status IN ('pending', 'scheduled');

CREATE TRIGGER agent_remediation_outbox_notify AFTER INSERT ON agent_remediation_outbox FOR EACH STATEMENT EXECUTE FUNCTION continuo_outbox_notify();

-- fix

CREATE TABLE fix (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source text NOT NULL,
    release_id text NOT NULL,
    node_id text NOT NULL,
    error_signature text NOT NULL,
    try integer NOT NULL,
    status text NOT NULL,
    confidence text DEFAULT '' NOT NULL,
    rationale text DEFAULT '' NOT NULL,
    proposed_sql_uri text DEFAULT '' NOT NULL,
    diff_uri text DEFAULT '' NOT NULL,
    model text DEFAULT '' NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    candidate_fix_sql_uri text DEFAULT '' NOT NULL,
    candidate_fix_diff_uri text DEFAULT '' NOT NULL,
    source_resolved boolean DEFAULT false NOT NULL,
    repo text DEFAULT '' NOT NULL,
    commit_sha text DEFAULT '' NOT NULL,
    file_path text DEFAULT '' NOT NULL,
    pr_url text DEFAULT '' NOT NULL,
    pr_number integer DEFAULT 0 NOT NULL,
    pr_state text DEFAULT '' NOT NULL,
    pr_opened_at timestamptz,
    pr_opened_by text DEFAULT '' NOT NULL,
    pr_closed_at timestamptz,
    pr_claimed_at timestamptz,
    file_edits jsonb DEFAULT '[]'::jsonb NOT NULL,
    release_check_id text DEFAULT '' NOT NULL,
    verify_error text DEFAULT '' NOT NULL,
    trigger_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    round integer DEFAULT 1 NOT NULL,
    resolved_node_ids jsonb DEFAULT '[]'::jsonb NOT NULL,
    node_outcomes jsonb DEFAULT '{}'::jsonb NOT NULL,
    release_checks jsonb DEFAULT '[]'::jsonb NOT NULL,
    projects text[] DEFAULT '{}'::text[] NOT NULL,
    remediation_id uuid,
    approvers jsonb DEFAULT '[]'::jsonb NOT NULL,
    CONSTRAINT fix_pkey PRIMARY KEY (id),
    CONSTRAINT fix_uniq UNIQUE (release_id, round, try),
    CONSTRAINT fix_try_check CHECK ((try >= 0)),
    CONSTRAINT fix_status_check CHECK ((status IN ('generating', 'verifying', 'proposed', 'skipped', 'failed', 'handed_over'))),
    CONSTRAINT fix_remediation_id_fkey FOREIGN KEY (remediation_id) REFERENCES remediation(id) ON DELETE SET NULL
);

CREATE INDEX idx_fix_release_round ON fix (release_id, round);

CREATE INDEX idx_fix_resolved_node_ids ON fix USING gin (resolved_node_ids);

CREATE INDEX idx_fix_projects ON fix USING gin (projects);

CREATE INDEX idx_fix_remediation_id ON fix (remediation_id) WHERE remediation_id IS NOT NULL;

CREATE TRIGGER fix_stamp_pr_claimed_at BEFORE UPDATE ON fix FOR EACH ROW EXECUTE FUNCTION stamp_pr_claimed_at_on_opening();

-- remediation_try

CREATE TABLE remediation_try (
    remediation_id uuid NOT NULL,
    try integer NOT NULL,
    status text NOT NULL,
    file_revisions jsonb NOT NULL,
    deadline_at timestamptz NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT remediation_try_pkey PRIMARY KEY (remediation_id, try),
    CONSTRAINT remediation_try_status_check CHECK ((status IN ('drafting', 'checking', 'verifying', 'passed', 'failed'))),
    CONSTRAINT remediation_try_try_check CHECK ((try >= 1)),
    CONSTRAINT remediation_try_remediation_id_fkey FOREIGN KEY (remediation_id) REFERENCES remediation(id) ON DELETE CASCADE
);

-- fix_pull_request

CREATE TABLE fix_pull_request (
    fix_id uuid NOT NULL,
    project text DEFAULT '' NOT NULL,
    repo text DEFAULT '' NOT NULL,
    branch text DEFAULT '' NOT NULL,
    pr_state text DEFAULT '' NOT NULL,
    pr_url text DEFAULT '' NOT NULL,
    pr_number integer DEFAULT 0 NOT NULL,
    pr_claimed_at timestamptz,
    pr_opened_at timestamptz,
    pr_opened_by text DEFAULT '' NOT NULL,
    pr_closed_at timestamptz,
    CONSTRAINT fix_pull_request_pkey PRIMARY KEY (fix_id, project),
    CONSTRAINT fix_pull_request_fix_id_fkey FOREIGN KEY (fix_id) REFERENCES fix(id) ON DELETE CASCADE
);

CREATE INDEX idx_fix_pull_request_live ON fix_pull_request (pr_state) WHERE pr_state IN ('opening', 'open');

-- remediation_cluster

CREATE TABLE remediation_cluster (
    remediation_id uuid NOT NULL,
    try integer NOT NULL,
    cluster integer NOT NULL,
    kind text NOT NULL,
    target_node_id text NOT NULL,
    members text[] NOT NULL,
    type_name text DEFAULT '' NOT NULL,
    status text NOT NULL,
    draft integer DEFAULT 1 NOT NULL,
    outcome jsonb,
    updated_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT remediation_cluster_pkey PRIMARY KEY (remediation_id, try, cluster),
    CONSTRAINT remediation_cluster_cluster_check CHECK ((cluster >= 1)),
    CONSTRAINT remediation_cluster_kind_check CHECK ((kind IN ('single', 'shared_upstream', 'cross_project'))),
    CONSTRAINT remediation_cluster_status_check CHECK ((status IN ('gathering', 'proposing', 'preflight', 'checked', 'handed_over', 'fell_back'))),
    CONSTRAINT remediation_cluster_remediation_id_try_fkey FOREIGN KEY (remediation_id, try) REFERENCES remediation_try(remediation_id, try) ON DELETE CASCADE
);

-- remediation_step

CREATE TABLE remediation_step (
    id bigserial,
    remediation_id uuid NOT NULL,
    try integer NOT NULL,
    cluster integer DEFAULT 0 NOT NULL,
    step text NOT NULL,
    kind text NOT NULL,
    draft integer DEFAULT 1 NOT NULL,
    status text NOT NULL,
    input_digest text DEFAULT '' NOT NULL,
    output jsonb,
    findings jsonb DEFAULT '[]'::jsonb NOT NULL,
    waiting_on jsonb,
    model text DEFAULT '' NOT NULL,
    prompt_version text DEFAULT '' NOT NULL,
    tokens_in integer DEFAULT 0 NOT NULL,
    tokens_out integer DEFAULT 0 NOT NULL,
    cache_hit boolean DEFAULT false NOT NULL,
    file_revision text DEFAULT '' NOT NULL,
    failures integer DEFAULT 0 NOT NULL,
    last_error text DEFAULT '' NOT NULL,
    started_at timestamptz DEFAULT now() NOT NULL,
    finished_at timestamptz,
    CONSTRAINT remediation_step_pkey PRIMARY KEY (id),
    CONSTRAINT remediation_step_remediation_id_try_cluster_step_draft_key UNIQUE (remediation_id, try, cluster, step, draft),
    CONSTRAINT remediation_step_kind_check CHECK ((kind IN ('context', 'prompt', 'check', 'approver', 'output', 'runner'))),
    CONSTRAINT remediation_step_status_check CHECK ((status IN ('running', 'waiting', 'done', 'failed'))),
    CONSTRAINT remediation_step_remediation_id_try_fkey FOREIGN KEY (remediation_id, try) REFERENCES remediation_try(remediation_id, try) ON DELETE CASCADE
);
