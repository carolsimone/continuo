-- One row per attempt of a task, keyed (task_id, attempt).
--
-- A task is dispatched once per attempt; the attempt row records that attempt's
-- Kubernetes job, status, times, failure class and reason, log and parse cache.
-- task_tracker keeps the task's own status and the number of its current
-- attempt (0-based; the last attempt allowed is max_retries).
--
-- dispatched_at of attempt 0 is the task's task_tracker.dispatched_at, which
-- state's own clock stamped when it dispatched the task; a later attempt, or
-- attempt 0 of a task with no dispatch time, takes the database clock when it
-- is first recorded. last_report_at is on the database clock. next_check_at
-- and checks_sent are set by nothing: state only clears next_check_at, when an
-- attempt's final status is saved and when its run cancels it. queued_at,
-- started_at and finished_at are stored as reported by execution-controller
-- and only displayed.

CREATE TABLE task_attempts (
    task_id            uuid          NOT NULL,
    attempt            integer       NOT NULL,
    job_name           varchar(63)   NOT NULL,
    status             varchar(16)   NOT NULL,
    failure_class      varchar(32),
    failure_reason     varchar(32),
    dispatched_at      timestamptz   NOT NULL DEFAULT now(),
    queued_at          timestamptz,
    started_at         timestamptz,
    finished_at        timestamptz,
    execution_seconds  numeric(10,3),
    exit_code          integer,
    error_message      text,
    log_s3_key         varchar(500),
    run_results_uri    varchar(500),
    parse_cache        varchar(16),
    parse_cache_reason text,
    last_report_at     timestamptz,
    next_check_at      timestamptz,
    checks_sent        integer       NOT NULL DEFAULT 0,
    CONSTRAINT task_attempts_pkey PRIMARY KEY (task_id, attempt),
    CONSTRAINT task_attempts_task_id_fkey FOREIGN KEY (task_id) REFERENCES task_tracker(task_id) ON DELETE CASCADE,
    CONSTRAINT task_attempts_attempt_check CHECK (attempt >= 0),
    CONSTRAINT task_attempts_status_check CHECK (status IN ('dispatched', 'queued', 'running', 'succeeded', 'failed', 'cancelled'))
);

CREATE INDEX idx_task_attempts_due ON task_attempts (next_check_at)
    WHERE status IN ('dispatched', 'queued', 'running');

COMMENT ON TABLE task_attempts IS 'One row per attempt of a task: its job, status, times, failure and logs';
COMMENT ON COLUMN task_attempts.attempt IS '0-based attempt number; task_tracker.attempt names the current one';
COMMENT ON COLUMN task_attempts.next_check_at IS 'Set by nothing; state clears it when the attempt''s final status is saved and when the attempt is cancelled';

ALTER TABLE task_tracker RENAME COLUMN retry_count TO attempt;

COMMENT ON COLUMN task_tracker.attempt IS 'Number of the task''s current attempt, 0-based';
