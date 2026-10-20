-- The status values below repeat model.AllStatuses() and
-- model.InFlightStatuses() in execution-controller/domain/model/status.go;
-- TestExecutionSchemaMatchesContract fails if they ever differ.
--
-- Every fact execution-controller observes of a deployment's attempt, kept as
-- it is seen: the main container's start and end, its exit code and
-- termination reason, the pod's and the Job's failure reasons, the uploaded
-- log and result keys, and the failure class and reason once decided. NULL
-- until the attempt is first observed. A fact seen before a pod disappears is
-- kept after it.
ALTER TABLE deployments ADD COLUMN facts JSONB NULL;

-- terminating: execution-controller deleted the Job to end the attempt itself
-- (a start error or the start timeout) and waits for its pods to be gone. It
-- holds its slot until then.
ALTER TABLE deployments DROP CONSTRAINT deployments_status_check;
ALTER TABLE deployments ADD CONSTRAINT deployments_status_check
    CHECK (status IN ('pending','blocked','reserved','starting','running','terminating','done','failed','skipped'));

DROP INDEX idx_deployments_in_flight;
CREATE INDEX idx_deployments_in_flight ON deployments (status, state_changed_at)
    WHERE status IN ('reserved','starting','running','terminating');

DROP TRIGGER deployments_admission_notify ON deployments;
CREATE TRIGGER deployments_admission_notify
    AFTER UPDATE OF status ON deployments
    FOR EACH ROW
    WHEN ((NEW.status = 'pending' AND OLD.status <> 'pending')
       OR (OLD.status IN ('reserved','starting','running','terminating')
           AND NEW.status NOT IN ('reserved','starting','running','terminating')))
    EXECUTE FUNCTION continuo_deployments_notify();
