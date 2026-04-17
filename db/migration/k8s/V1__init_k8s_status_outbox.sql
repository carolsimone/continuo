-- K8s Status Outbox Table
-- Implements Transactional Outbox Pattern to prevent dual-write problems

CREATE TABLE k8s_status_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Event identification
    event_type VARCHAR(50) NOT NULL,  -- 'task_succeeded', 'task_failed', 'task_retry', 'check_delayed'
    stream_name VARCHAR(100) NOT NULL, -- Target stream: retry.task:v1, task.failed:v1, check.k8s:v1

    -- Task context (copied from consumed message)
    task_id UUID NOT NULL,
    schedule_id UUID NOT NULL,
    schedule_name VARCHAR(255) NOT NULL,
    service_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    job_name VARCHAR(63) NOT NULL,

    -- Event-specific data
    error_message TEXT,
    task_retry_count INT DEFAULT 0,  -- Renamed to avoid confusion with outbox_retry_count
    check_after BIGINT,  -- Unix timestamp for delayed check.k8s:v1 events

    -- State update flags (what gRPC calls to make in OutboxProcessor)
    update_task_status BOOLEAN DEFAULT false,
    new_task_status VARCHAR(20),
    new_retry_count INT,
    create_execution BOOLEAN DEFAULT false,
    execution_started_at TIMESTAMP WITH TIME ZONE,
    execution_completed_at TIMESTAMP WITH TIME ZONE,
    execution_seconds DOUBLE PRECISION,

    -- Outbox processing status
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    outbox_retry_count INT DEFAULT 0,  -- How many times OutboxProcessor has tried this entry
    max_retries INT DEFAULT 3,
    outbox_error_message TEXT,  -- Error from OutboxProcessor attempts

    CONSTRAINT k8s_status_outbox_status_check CHECK (
        status IN ('pending', 'processed', 'failed')
    )
);

-- Index for efficient polling by OutboxProcessor
CREATE INDEX idx_k8s_status_outbox_pending
ON k8s_status_outbox(created_at)
WHERE status = 'pending' AND outbox_retry_count < max_retries;

-- Index for monitoring failed entries
CREATE INDEX idx_k8s_status_outbox_failed
ON k8s_status_outbox(created_at)
WHERE status = 'failed';

COMMENT ON TABLE k8s_status_outbox IS 'Transactional outbox for K8s job status events';
COMMENT ON COLUMN k8s_status_outbox.event_type IS 'Type of status event to publish';
COMMENT ON COLUMN k8s_status_outbox.task_retry_count IS 'Task retry count (not outbox retry count)';
