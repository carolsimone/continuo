-- continuo_run_controller: run-controller's database.
--
-- Runs and their tasks and attempts, the node write leases, the schedules and
-- their crons, run-controller's copy of every promoted topology, the version
-- each node last loaded, the command request ids, the open view outages, the
-- messages it consumed and its outbox.
--
-- Each table is one block: its columns and constraints, then its indexes,
-- triggers and comments.

-- Functions

CREATE FUNCTION analyze_topology() RETURNS void
    LANGUAGE sql SECURITY DEFINER
    SET search_path TO 'public', 'pg_temp'
    AS $$
    ANALYZE topology_nodes, topology_edges
$$;

CREATE FUNCTION continuo_outbox_notify() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_notify(TG_TABLE_NAME, '');
    RETURN NULL;
END;
$$;

CREATE FUNCTION notify_node_leases_released() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM released_leases) THEN
        PERFORM pg_notify('node_leases', '');
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION notify_schedules_changed() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_notify('schedules', '');
    RETURN NULL;
END;
$$;

COMMENT ON FUNCTION analyze_topology() IS 'Analyzes topology_nodes and topology_edges as the tables'' owner, for a service login that does not own them';

COMMENT ON FUNCTION notify_node_leases_released() IS 'Notifies the node_leases channel after a statement has deleted at least one lease';

COMMENT ON FUNCTION notify_schedules_changed() IS 'Notifies the schedules channel after a statement has inserted, updated or deleted schedules';

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

-- node_materializations

CREATE TABLE node_materializations (
    tenant_id text NOT NULL,
    node_key text NOT NULL,
    content_hash text DEFAULT '' NOT NULL,
    image_tag text DEFAULT '' NOT NULL,
    release_id text DEFAULT '' NOT NULL,
    run_id uuid,
    task_id uuid,
    materialized_at timestamptz,
    source text,
    attempt_content_hash text DEFAULT '' NOT NULL,
    attempt_run_id uuid,
    attempted_at timestamptz,
    seed_columns_hash text DEFAULT '' NOT NULL,
    CONSTRAINT node_materializations_pkey PRIMARY KEY (tenant_id, node_key),
    CONSTRAINT node_materializations_source_check CHECK ((source IN ('build', 'adopted')))
);

COMMENT ON COLUMN node_materializations.source IS 'build: a run loaded this version; adopted: recorded as already loaded when run-controller applied its first promotion; NULL: no version recorded yet.';

-- runs

CREATE TABLE runs (
    run_id uuid DEFAULT gen_random_uuid() NOT NULL,
    schedule_name varchar(50) NOT NULL,
    status varchar(20) NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    started_at timestamptz,
    completed_at timestamptz,
    last_progress_at timestamptz,
    cancelled_at timestamptz,
    cancelled_by text,
    cancellation_reason text,
    total_task_count integer,
    terminal_task_count integer DEFAULT 0 NOT NULL,
    kind varchar(20) DEFAULT 'cron'::character varying NOT NULL,
    source_run_id uuid,
    initiated_by text DEFAULT 'system' NOT NULL,
    operation varchar(16) DEFAULT 'run'::character varying NOT NULL,
    release_id text,
    promotion_seq bigint,
    planned_at timestamptz,
    failure_reason varchar(32),
    tenant_id text NOT NULL,
    scheduled_for timestamptz,
    CONSTRAINT runs_pkey PRIMARY KEY (tenant_id, run_id),
    CONSTRAINT runs_failure_reason_check CHECK ((failure_reason IN ('stalled', 'empty_selection', 'invalid_node_type'))),
    CONSTRAINT runs_kind_check CHECK ((kind IN ('cron', 'manual', 'rerun', 'rebase', 'single_node_run', 'promote_seed'))),
    CONSTRAINT runs_operation_check CHECK ((operation IN ('run', 'test', 'build', 'full_refresh'))),
    CONSTRAINT runs_status_check CHECK ((status IN ('pending', 'running', 'cancelling', 'succeeded', 'failed', 'cancelled', 'skipped'))),
    CONSTRAINT valid_timestamps CHECK ((((started_at IS NULL) OR (started_at >= created_at)) AND ((completed_at IS NULL) OR (completed_at >= started_at))))
);

CREATE INDEX idx_runs_kind ON runs (kind);

CREATE INDEX idx_runs_release_id ON runs (release_id) WHERE release_id IS NOT NULL;

CREATE INDEX idx_runs_schedule_name_created ON runs (tenant_id, schedule_name, created_at DESC);

CREATE INDEX idx_runs_source_run_id ON runs (source_run_id) WHERE source_run_id IS NOT NULL;

CREATE UNIQUE INDEX uq_runs_active_per_schedule ON runs (tenant_id, schedule_name) WHERE status IN ('pending', 'running', 'cancelling');

CREATE UNIQUE INDEX uq_runs_scheduled_for ON runs (tenant_id, schedule_name, scheduled_for) WHERE scheduled_for IS NOT NULL;

COMMENT ON TABLE runs IS 'One run of a schedule: its lifecycle from pending through running to a final status';

COMMENT ON COLUMN runs.run_id IS 'Unique identifier of the run';

COMMENT ON COLUMN runs.last_progress_at IS 'The run''s progress clock: stamped when a task of the run is dispatched or moves; run-controller fails an active run with nothing in flight whose clock is more than five minutes old as stalled';

COMMENT ON COLUMN runs.kind IS 'Run discriminator. Allowed: cron, manual, rerun, rebase, single_node_run, promote_seed. Written once at activation time and immutable thereafter; a rerun/rebase mints a new row with its own kind rather than mutating the source.';

COMMENT ON COLUMN runs.source_run_id IS 'Lineage pointer to the run this one derives from (rerun, rebase, stale-mode single_node_run). NULL for cron and manual runs. Not a foreign key — orphans are fine.';

COMMENT ON COLUMN runs.initiated_by IS 'User who initiated the run, or ''system'' for cron / platform-initiated runs. Stamped at creation from the gRPC initiating-user metadata header; immutable thereafter.';

COMMENT ON COLUMN runs.operation IS 'dbt verb the run applies to its nodes: run (model) | test | build | full_refresh. Stamped at activation; immutable.';

COMMENT ON COLUMN runs.release_id IS 'The release whose topology this run builds; NULL for runs that carry none.';

COMMENT ON COLUMN runs.failure_reason IS 'Why the run failed when no task of it failed; NULL otherwise';

COMMENT ON COLUMN runs.tenant_id IS 'Tenant of the run; leads the run''s key';

COMMENT ON COLUMN runs.scheduled_for IS 'Slot of the schedule this run was fired for, the instant its cron expression named; NULL for a run no schedule fired';

COMMENT ON INDEX idx_runs_schedule_name_created IS 'Serves the schedule_name-keyed active-run lookups and the DISTINCT ON last-run-per-schedule scan within a tenant.';

COMMENT ON INDEX uq_runs_active_per_schedule IS 'Enforces at most one active (pending|running|cancelling) run per schedule within a tenant; the DB-level backstop for the activation TOCTOU race.';

COMMENT ON INDEX uq_runs_scheduled_for IS 'At most one run per schedule slot within a tenant, so a fire made twice creates one run.';

-- schedules

CREATE TABLE schedules (
    tenant_id text NOT NULL,
    name text NOT NULL,
    cron text NOT NULL,
    timezone text NOT NULL,
    description text DEFAULT '' NOT NULL,
    paused boolean DEFAULT false NOT NULL,
    next_fire_at timestamptz,
    version bigint DEFAULT 1 NOT NULL,
    updated_by text NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT schedules_pkey PRIMARY KEY (tenant_id, name),
    CONSTRAINT schedules_version_positive CHECK ((version >= 1))
);

CREATE TRIGGER schedules_changed AFTER INSERT OR DELETE OR UPDATE ON schedules FOR EACH STATEMENT EXECUTE FUNCTION notify_schedules_changed();

COMMENT ON TABLE schedules IS 'The cron schedules run-controller fires, one row per tenant and schedule name';

COMMENT ON COLUMN schedules.tenant_id IS 'Tenant that owns the schedule; leads the key, so another tenant may use the same name';

COMMENT ON COLUMN schedules.name IS 'Name of the schedule, the schedule_name of the runs it fires';

COMMENT ON COLUMN schedules.cron IS 'Cron expression that says when the schedule fires, read in timezone';

COMMENT ON COLUMN schedules.timezone IS 'IANA time zone name the cron expression is read in';

COMMENT ON COLUMN schedules.description IS 'Free text that says what the schedule is for; empty when none was given';

COMMENT ON COLUMN schedules.paused IS 'Whether the schedule is paused; a paused schedule fires nothing';

COMMENT ON COLUMN schedules.next_fire_at IS 'Instant of the next slot the schedule fires; NULL while the schedule is paused';

COMMENT ON COLUMN schedules.version IS 'Counts the operator''s edits of the schedule: 1 when it is created, raised by each edit; a fire leaves it';

COMMENT ON COLUMN schedules.updated_by IS 'Who made the last edit of the schedule; a fire leaves it';

COMMENT ON COLUMN schedules.updated_at IS 'When the last edit of the schedule was made; a fire leaves it';

COMMENT ON CONSTRAINT schedules_version_positive ON schedules IS 'A schedule''s version counts the operator''s edits from 1';

-- topology_releases

CREATE TABLE topology_releases (
    tenant_id text NOT NULL,
    release_id text NOT NULL,
    promotion_seq bigint NOT NULL,
    topology_uri text NOT NULL,
    topology_sha256 text NOT NULL,
    loaded_at timestamptz DEFAULT now() NOT NULL,
    bootstrap boolean DEFAULT false NOT NULL,
    CONSTRAINT topology_releases_pkey PRIMARY KEY (tenant_id, release_id)
);

COMMENT ON COLUMN topology_releases.promotion_seq IS 'The highest promotion_seq this release was announced under.';

-- view_outages

CREATE TABLE view_outages (
    tenant_id text NOT NULL,
    node_key text NOT NULL,
    state text NOT NULL,
    since timestamptz NOT NULL,
    seed_key text NOT NULL,
    run_id uuid NOT NULL,
    CONSTRAINT view_outages_pkey PRIMARY KEY (tenant_id, node_key),
    CONSTRAINT view_outages_state_check CHECK ((state IN ('missing', 'stale')))
);

COMMENT ON TABLE view_outages IS 'One row per view a seed load dropped (missing) or left stale (stale), from the commit of that load until a task of the view succeeds.';

-- command_requests

CREATE TABLE command_requests (
    tenant_id text NOT NULL,
    request_id uuid NOT NULL,
    command text NOT NULL,
    fingerprint text NOT NULL,
    run_id uuid NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT command_requests_pkey PRIMARY KEY (tenant_id, request_id),
    CONSTRAINT command_requests_run_id_fkey FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, run_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX idx_command_requests_created_at ON command_requests (created_at);

CREATE INDEX idx_command_requests_run ON command_requests (tenant_id, run_id);

COMMENT ON TABLE command_requests IS 'One row per command request id: the command, the fingerprint of its parameters and the run it created or cancelled';

COMMENT ON COLUMN command_requests.tenant_id IS 'Tenant that made the request; leads the key, so another tenant may use the same request id';

COMMENT ON COLUMN command_requests.request_id IS 'Id the caller gave the request; a repeat of it returns the first call''s run';

COMMENT ON COLUMN command_requests.command IS 'Name of the command: start_run, start_single_node_run, cancel_active_run and the like';

COMMENT ON COLUMN command_requests.fingerprint IS 'Hex SHA-256 of the command and its parsed parameters; another fingerprint under the same id is a reuse';

COMMENT ON COLUMN command_requests.run_id IS 'Run the command created or cancelled';

COMMENT ON COLUMN command_requests.created_at IS 'When the request was first made; the sweeper deletes rows older than the replay horizon';

-- live_release

CREATE TABLE live_release (
    tenant_id text NOT NULL,
    release_id text NOT NULL,
    promotion_seq bigint NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT live_release_pkey PRIMARY KEY (tenant_id),
    CONSTRAINT live_release_release_fkey FOREIGN KEY (tenant_id, release_id) REFERENCES topology_releases(tenant_id, release_id)
);

-- run_controller_outbox

CREATE TABLE run_controller_outbox (
    id uuid NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    stream_name text NOT NULL,
    status text DEFAULT 'pending' NOT NULL,
    max_retries integer DEFAULT 13 NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    message_processing_id uuid,
    processed_at timestamptz,
    error_message text,
    next_attempt_at timestamptz,
    CONSTRAINT run_controller_outbox_pkey PRIMARY KEY (id),
    CONSTRAINT run_controller_outbox_status_check CHECK ((status IN ('pending', 'scheduled', 'processed', 'failed'))),
    CONSTRAINT run_controller_outbox_message_processing_id_fkey FOREIGN KEY (message_processing_id) REFERENCES message_processing(id)
);

CREATE INDEX idx_run_controller_outbox_aggregate ON run_controller_outbox (aggregate_type, aggregate_id);

CREATE INDEX idx_run_controller_outbox_claimable ON run_controller_outbox (created_at, id) WHERE status IN ('pending', 'scheduled');

CREATE INDEX idx_run_controller_outbox_failed ON run_controller_outbox (created_at) WHERE status = 'failed';

CREATE INDEX idx_run_controller_outbox_message_processing_id ON run_controller_outbox (message_processing_id);

CREATE INDEX idx_run_controller_outbox_open_by_aggregate ON run_controller_outbox (aggregate_type, aggregate_id, created_at) WHERE status IN ('pending', 'scheduled');

CREATE TRIGGER run_controller_outbox_notify AFTER INSERT ON run_controller_outbox FOR EACH STATEMENT EXECUTE FUNCTION continuo_outbox_notify();

-- tasks

CREATE TABLE tasks (
    task_id uuid NOT NULL,
    run_id uuid NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    project_name varchar(100) NOT NULL,
    schema_name varchar(100) NOT NULL,
    table_name varchar(100) NOT NULL,
    job_name varchar(63) NOT NULL,
    status varchar(20) NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    max_retries integer NOT NULL,
    cancelled_at timestamptz,
    cancelled_by text,
    image_tag varchar(255) DEFAULT ''::character varying NOT NULL,
    inherited_from_task_id uuid,
    operation varchar(16) DEFAULT 'run'::character varying NOT NULL,
    node_key text,
    dispatched_at timestamptz,
    wait_reason varchar(16),
    lease_wait_since timestamptz,
    tenant_id text NOT NULL,
    plan_reason text DEFAULT '' NOT NULL,
    seed_preflight jsonb,
    rebuild_seed_key text DEFAULT '' NOT NULL,
    CONSTRAINT tasks_pkey PRIMARY KEY (tenant_id, task_id),
    CONSTRAINT tasks_operation_check CHECK ((operation IN ('run', 'test', 'build', 'full_refresh'))),
    CONSTRAINT tasks_status_check CHECK ((status IN ('pending', 'dispatched', 'queued', 'running', 'stopping', 'succeeded', 'failed', 'cancelled', 'skipped'))),
    CONSTRAINT tasks_wait_reason_check CHECK (((wait_reason)::text = 'node_lease')),
    CONSTRAINT tasks_run_id_fkey FOREIGN KEY (tenant_id, run_id) REFERENCES runs(tenant_id, run_id) ON DELETE CASCADE
);

CREATE INDEX idx_tasks_inherited_from ON tasks (inherited_from_task_id) WHERE inherited_from_task_id IS NOT NULL;

CREATE INDEX idx_tasks_job_name ON tasks (job_name);

CREATE INDEX idx_tasks_lease_waiters ON tasks (tenant_id, node_key, lease_wait_since) WHERE (wait_reason)::text = 'node_lease';

CREATE INDEX idx_tasks_node_identity ON tasks (project_name, schema_name, table_name);

CREATE INDEX idx_tasks_node_key_created ON tasks (tenant_id, node_key, created_at DESC) WHERE node_key IS NOT NULL;

CREATE INDEX idx_tasks_run_id ON tasks (run_id);

CREATE UNIQUE INDEX uq_tasks_run_node ON tasks (tenant_id, run_id, node_key) WHERE node_key IS NOT NULL;

COMMENT ON TABLE tasks IS 'One task of a run: the work for a single node, with its status and current attempt';

COMMENT ON COLUMN tasks.task_id IS 'Unique identifier for the task';

COMMENT ON COLUMN tasks.run_id IS 'The run the task belongs to; deleting the run deletes its tasks';

COMMENT ON COLUMN tasks.job_name IS 'Kubernetes job name (max 63 chars)';

COMMENT ON COLUMN tasks.attempt IS 'Number of the task''s current attempt, 0-based';

COMMENT ON COLUMN tasks.image_tag IS 'The image tag of the task''s node in the release its run builds, copied when the task is planned and never changed.';

COMMENT ON COLUMN tasks.inherited_from_task_id IS 'Lineage marker for inherited tasks in rebase runs. NULL = task was actually executed in this run. Non-NULL = task projected as SUCCEEDED from a rebase parent; points to the ROOT executed task_id (resolve-to-root semantics — chain depth ≤ 1 even across rebase-of-rebase). Not a foreign key — orphaned pointers are fine.';

COMMENT ON COLUMN tasks.operation IS 'dbt verb this task ran, denormalized from its run at dispatch: run (model) | test | build | full_refresh.';

COMMENT ON COLUMN tasks.wait_reason IS 'What a pending task waits for besides its upstream tasks: node_lease while another task holds the lease on its node; NULL when it waits for nothing';

COMMENT ON COLUMN tasks.lease_wait_since IS 'When the task began to wait for its node lease; NULL when it is not waiting';

COMMENT ON COLUMN tasks.tenant_id IS 'Tenant of the task, the tenant of its run; leads the task''s key';

COMMENT ON COLUMN tasks.rebuild_seed_key IS 'The seed whose load drops the relation this task rebuilds; empty for every other task.';

COMMENT ON INDEX idx_tasks_lease_waiters IS 'The tasks waiting for a node lease, oldest first per tenant and node; the waker grants from it.';

COMMENT ON INDEX idx_tasks_node_key_created IS 'The tasks of one node within a tenant, newest first; the schedule graph reads the last run of a dormant node from it.';

-- topology_nodes

CREATE TABLE topology_nodes (
    tenant_id text NOT NULL,
    release_id text NOT NULL,
    node_key text NOT NULL,
    unique_id text NOT NULL,
    project_name text NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    node_type text NOT NULL,
    test_count integer,
    content_hash text DEFAULT '' NOT NULL,
    image_tag text DEFAULT '' NOT NULL,
    secret_ref text DEFAULT '' NOT NULL,
    schedule_name text DEFAULT '' NOT NULL,
    timeout_seconds integer,
    seed_columns_hash text DEFAULT '' NOT NULL,
    materialized text DEFAULT '' NOT NULL,
    full_refresh boolean,
    resolved_relation_id text DEFAULT '' NOT NULL,
    CONSTRAINT topology_nodes_pkey PRIMARY KEY (tenant_id, release_id, node_key),
    CONSTRAINT topology_nodes_release_fkey FOREIGN KEY (tenant_id, release_id) REFERENCES topology_releases(tenant_id, release_id) ON DELETE CASCADE
);

CREATE INDEX idx_topology_nodes_schedule ON topology_nodes (tenant_id, release_id, schedule_name);

COMMENT ON COLUMN topology_nodes.node_key IS 'Lowercased "<schema>.<table>"; unique within a release.';

COMMENT ON COLUMN topology_nodes.timeout_seconds IS 'Task timeout in seconds the node declared in this release; NULL when it declared none';

COMMENT ON COLUMN topology_nodes.full_refresh IS 'A dbt seed''s +full_refresh setting; NULL when the seed leaves it unset, and on every other node.';

-- node_leases

CREATE TABLE node_leases (
    tenant_id text NOT NULL,
    node_key text NOT NULL,
    task_id uuid NOT NULL,
    run_id uuid NOT NULL,
    acquired_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT node_leases_pkey PRIMARY KEY (tenant_id, node_key),
    CONSTRAINT node_leases_task_id_key UNIQUE (tenant_id, task_id),
    CONSTRAINT node_leases_task_id_fkey FOREIGN KEY (tenant_id, task_id) REFERENCES tasks(tenant_id, task_id) ON DELETE CASCADE
);

CREATE TRIGGER node_leases_released AFTER DELETE ON node_leases REFERENCING OLD TABLE AS released_leases FOR EACH STATEMENT EXECUTE FUNCTION notify_node_leases_released();

COMMENT ON TABLE node_leases IS 'The write lease on a node: at most one row per tenant and node, and one per task. Deleting the row (or the task) releases the node.';

COMMENT ON COLUMN node_leases.tenant_id IS 'Tenant of the node';

COMMENT ON COLUMN node_leases.node_key IS 'Lowercased "<schema>.<table>" of the leased node, the key of topology_nodes';

COMMENT ON COLUMN node_leases.task_id IS 'The task that holds the lease; a task holds at most one, and the lease ends with the task';

COMMENT ON COLUMN node_leases.run_id IS 'The run of the task that holds the lease, shown to the tasks that wait for it';

COMMENT ON COLUMN node_leases.acquired_at IS 'When the task took the lease';

-- task_attempts

CREATE TABLE task_attempts (
    task_id uuid NOT NULL,
    attempt integer NOT NULL,
    job_name varchar(63) NOT NULL,
    status varchar(16) NOT NULL,
    failure_class varchar(32),
    failure_reason varchar(32),
    dispatched_at timestamptz DEFAULT now() NOT NULL,
    queued_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    execution_seconds numeric(10,3),
    exit_code integer,
    error_message text,
    log_s3_key varchar(500),
    run_results_uri varchar(500),
    parse_cache varchar(16),
    parse_cache_reason text,
    last_report_at timestamptz,
    next_check_at timestamptz,
    checks_sent integer DEFAULT 0 NOT NULL,
    stop_requested_at timestamptz,
    tenant_id text NOT NULL,
    seed_preflight_findings jsonb,
    CONSTRAINT task_attempts_pkey PRIMARY KEY (tenant_id, task_id, attempt),
    CONSTRAINT task_attempts_attempt_check CHECK ((attempt >= 0)),
    CONSTRAINT task_attempts_status_check CHECK ((status IN ('dispatched', 'queued', 'running', 'succeeded', 'failed', 'stopped'))),
    CONSTRAINT task_attempts_task_id_fkey FOREIGN KEY (tenant_id, task_id) REFERENCES tasks(tenant_id, task_id) ON DELETE CASCADE
);

CREATE INDEX idx_task_attempts_due ON task_attempts (next_check_at) WHERE status IN ('dispatched', 'queued', 'running');

COMMENT ON TABLE task_attempts IS 'One row per attempt of a task: its job, status, times, failure and logs';

COMMENT ON COLUMN task_attempts.attempt IS '0-based attempt number; tasks.attempt names the current one';

COMMENT ON COLUMN task_attempts.next_check_at IS 'When run-controller next asks execution-controller about the attempt: set when the attempt is dispatched, moved by each non-final report run-controller records for the attempt and each time run-controller asks; NULL once the attempt has ended or was cancelled';

COMMENT ON COLUMN task_attempts.stop_requested_at IS 'When run-controller requested a stop of the attempt because its run was cancelled; NULL until then';

COMMENT ON COLUMN task_attempts.tenant_id IS 'Tenant of the attempt, the tenant of its task; leads the attempt''s key';

-- topology_edges

CREATE TABLE topology_edges (
    tenant_id text NOT NULL,
    release_id text NOT NULL,
    upstream_key text NOT NULL,
    downstream_key text NOT NULL,
    CONSTRAINT topology_edges_pkey PRIMARY KEY (tenant_id, release_id, upstream_key, downstream_key),
    CONSTRAINT topology_edges_downstream_fkey FOREIGN KEY (tenant_id, release_id, downstream_key) REFERENCES topology_nodes(tenant_id, release_id, node_key) ON DELETE CASCADE,
    CONSTRAINT topology_edges_upstream_fkey FOREIGN KEY (tenant_id, release_id, upstream_key) REFERENCES topology_nodes(tenant_id, release_id, node_key) ON DELETE CASCADE
);

CREATE INDEX idx_topology_edges_downstream ON topology_edges (tenant_id, release_id, downstream_key);
