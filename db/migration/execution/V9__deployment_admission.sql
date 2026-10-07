-- The status values below repeat model.AllStatuses() and
-- model.InFlightStatuses() in execution-controller/domain/model/status.go;
-- TestExecutionSchemaMatchesContract fails if they ever differ.
--
-- Admission of deployments to execution slots. A deployment holds a slot while
-- reserved (claimed, Job not created), starting (Job created) or running (a
-- status check found the Job unfinished); it releases it on done. A dispatcher
-- reserves slots in one transaction that locks its scope's admission_capacity
-- row, so concurrent dispatchers serialise on that lock and together never hold
-- more slots than the configured cap. One row per scope; 'global' covers every
-- deployment.
CREATE TABLE admission_capacity (
    scope      TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO admission_capacity (scope) VALUES ('global');

-- A 'deployed' row's Job was created without slot tracking; it becomes 'done'
-- and holds no slot. Its outcome stays unset, so its Job's terminal status
-- check still records and reports it.
ALTER TABLE deployments DROP CONSTRAINT deployments_status_check;
UPDATE deployments SET status = 'done' WHERE status = 'deployed';
ALTER TABLE deployments ADD CONSTRAINT deployments_status_check
    CHECK (status IN ('pending','blocked','reserved','starting','running','done','failed','skipped'));

-- When the row last changed status; reconciliation measures staleness from it.
ALTER TABLE deployments ADD COLUMN state_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- The Job the row creates. Status checks find the row by it.
ALTER TABLE deployments
    ADD COLUMN job_name TEXT GENERATED ALWAYS AS (job_params ->> 'job_name') STORED;
CREATE INDEX idx_deployments_job_name ON deployments (job_name);

-- The rows holding a slot: counted on every claim, scanned by reconciliation.
CREATE INDEX idx_deployments_in_flight ON deployments (status, state_changed_at)
    WHERE status IN ('reserved','starting','running');

-- Wakes the dispatcher listening on channel 'deployments' when work may be
-- admitted: a deployment is accepted (inserted, or returned to pending) or a
-- slot is released. Postgres delivers the notification when the transaction
-- commits, once per transaction however many rows changed.
CREATE OR REPLACE FUNCTION continuo_deployments_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('deployments', '');
    RETURN NULL;
END;
$$;

CREATE TRIGGER deployments_accepted_notify
    AFTER INSERT ON deployments
    FOR EACH STATEMENT
    EXECUTE FUNCTION continuo_deployments_notify();

CREATE TRIGGER deployments_admission_notify
    AFTER UPDATE OF status ON deployments
    FOR EACH ROW
    WHEN ((NEW.status = 'pending' AND OLD.status <> 'pending')
       OR (OLD.status IN ('reserved','starting','running')
           AND NEW.status NOT IN ('reserved','starting','running')))
    EXECUTE FUNCTION continuo_deployments_notify();
