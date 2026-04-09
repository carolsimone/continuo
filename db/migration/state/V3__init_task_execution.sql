-- Task Execution Table
-- Tracks execution attempts for tasks

CREATE TABLE task_execution (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    task_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    execution_time_seconds decimal(10, 3),
    executor_id character varying(100),
    k8s_job_name character varying(253),
    error_message text,
    cancelled_at timestamp with time zone,
    cancelled_by character varying(255),
    cancellation_reason text,
    CONSTRAINT task_execution_pkey PRIMARY KEY (id),
    CONSTRAINT task_execution_task_id_fkey FOREIGN KEY (task_id) REFERENCES task_tracker(task_id) ON DELETE CASCADE,
    CONSTRAINT valid_execution_timestamps CHECK (((started_at IS NULL) OR (started_at >= created_at)) AND ((completed_at IS NULL) OR (completed_at >= started_at)))
);

CREATE INDEX idx_task_execution_task_id ON task_execution USING btree (task_id);
CREATE INDEX idx_task_execution_created_at ON task_execution USING btree (created_at DESC);

COMMENT ON TABLE task_execution IS 'Tracks execution attempts for tasks';
COMMENT ON COLUMN task_execution.execution_time_seconds IS 'Total execution time in seconds';
COMMENT ON COLUMN task_execution.k8s_job_name IS 'Kubernetes job name for this execution';
