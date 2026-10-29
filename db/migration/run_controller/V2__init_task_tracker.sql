-- Task Tracker Table
-- Tracks individual tasks within a schedule

CREATE TABLE task_tracker (
    task_id uuid NOT NULL,
    schedule_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    service_name character varying(100) NOT NULL,
    schema_name character varying(100) NOT NULL,
    table_name character varying(100) NOT NULL,
    job_name character varying(63) NOT NULL,
    status character varying(20) NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    max_retries integer NOT NULL,
    cancelled_at timestamp with time zone,
    cancelled_by character varying(255),
    CONSTRAINT task_tracker_pkey PRIMARY KEY (task_id),
    CONSTRAINT task_tracker_status_check CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'running'::character varying, 'succeeded'::character varying, 'failed'::character varying, 'cancelled'::character varying])::text[]))),
    CONSTRAINT task_tracker_schedule_id_fkey FOREIGN KEY (schedule_id) REFERENCES scheduler_tracker(schedule_id) ON DELETE CASCADE
);

CREATE INDEX idx_task_tracker_schedule_id ON task_tracker USING btree (schedule_id);
CREATE INDEX idx_task_tracker_status ON task_tracker USING btree (status);
CREATE INDEX idx_task_tracker_job_name ON task_tracker USING btree (job_name);

COMMENT ON TABLE task_tracker IS 'Tracks individual tasks within a schedule';
COMMENT ON COLUMN task_tracker.task_id IS 'Unique identifier for the task';
COMMENT ON COLUMN task_tracker.job_name IS 'Kubernetes job name (max 63 chars)';
