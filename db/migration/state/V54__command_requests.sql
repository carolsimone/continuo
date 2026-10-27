-- command_requests records the commands that create or cancel a run, so a
-- command repeated under the same request id returns the run of the first call
-- instead of making another.
--
-- A request is scoped to its tenant: the key is (tenant_id, request_id). The
-- fingerprint is the hash of the parsed command with its defaults applied; a
-- request id used again with another fingerprint is refused. run_id is the run
-- the command created or cancelled, and the record goes with that run. The
-- foreign key is checked when the transaction commits, because a command
-- writes its record before the run it creates.
--
-- A record older than the replay horizon is deleted by the state retention
-- sweeper, which ends the window in which a retry can be answered.
CREATE TABLE command_requests (
    tenant_id   text        NOT NULL,
    request_id  uuid        NOT NULL,
    command     text        NOT NULL,
    fingerprint text        NOT NULL,
    run_id      uuid        NOT NULL,
    created_at  timestamptz NOT NULL,
    CONSTRAINT command_requests_pkey PRIMARY KEY (tenant_id, request_id),
    CONSTRAINT command_requests_run_id_fkey FOREIGN KEY (tenant_id, run_id)
        REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);

-- The sweeper deletes by age.
CREATE INDEX idx_command_requests_created_at ON command_requests (created_at);
-- A run's records are found, and deleted with the run, by its key.
CREATE INDEX idx_command_requests_run ON command_requests (tenant_id, run_id);

COMMENT ON TABLE command_requests IS 'One row per command request id: the command, the fingerprint of its parameters and the run it created or cancelled';
COMMENT ON COLUMN command_requests.tenant_id IS 'Tenant that made the request; leads the key, so another tenant may use the same request id';
COMMENT ON COLUMN command_requests.request_id IS 'Id the caller gave the request; a repeat of it returns the first call''s run';
COMMENT ON COLUMN command_requests.command IS 'Name of the command: start_run, trigger_single_node_run, cancel_active_run and the like';
COMMENT ON COLUMN command_requests.fingerprint IS 'Hex SHA-256 of the command and its parsed parameters; another fingerprint under the same id is a reuse';
COMMENT ON COLUMN command_requests.run_id IS 'Run the command created or cancelled';
COMMENT ON COLUMN command_requests.created_at IS 'When the request was first made; the sweeper deletes rows older than the replay horizon';

-- A run no longer carries an initialization status: its tasks are planned in
-- the transaction that creates it, so a run is planned once planned_at is set.
-- The column's check constraint goes with it, and so do the two indexes whose
-- keys or predicates name it (idx_runs_init_status and idx_runs_finalization).
ALTER TABLE runs DROP COLUMN initialization_status;
