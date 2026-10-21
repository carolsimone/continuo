-- One row per attempt of a task, keyed (task_id, attempt).
--
-- A task is dispatched once per attempt; the attempt row records that attempt's
-- Kubernetes job, status, times, failure class and reason, log and parse cache.
-- task_tracker keeps the task's own status and the number of its current
-- attempt (0-based; the last attempt allowed is max_retries).
--
-- Every timestamp written by the database (dispatched_at, last_report_at,
-- next_check_at) is on the database clock, and every comparison state makes
-- uses that clock. queued_at, started_at and finished_at are stored as
-- reported by execution-controller and only displayed.

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
COMMENT ON COLUMN task_attempts.next_check_at IS 'Database clock; when state next asks about the attempt; NULL once final';

ALTER TABLE task_tracker RENAME COLUMN retry_count TO attempt;

COMMENT ON COLUMN task_tracker.attempt IS 'Number of the task''s current attempt, 0-based';
