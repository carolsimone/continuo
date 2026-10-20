-- A deployment the previous version moved to done without recording its
-- outcome (its slot released before its Job's result was recorded) still
-- awaits its result: the observer's resync lists such rows by Job name.
CREATE INDEX idx_deployments_awaiting_result ON deployments (job_name)
    WHERE status = 'done' AND outcome IS NULL;
